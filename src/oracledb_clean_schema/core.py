import heapq
import itertools
import logging
import os
import pathlib
import queue
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from functools import partial, total_ordering
from typing import Final, Iterator, Self, cast

import oracledb
from dotenv import load_dotenv
from oracledb.connection import Connection
from oracledb.pool import ConnectionPool, create_pool

from oracledb_clean_schema import constants

logger = logging.getLogger(__name__)

MAX_RETRY_ERRORS: Final[int] = int(os.getenv("MAX_RETRY_ERRORS", 10))
RETRY_ERROR_Q: queue.Queue[Exception] = queue.Queue(maxsize=MAX_RETRY_ERRORS)
EXECUTING_USER: str


class SchemaObjectType(Enum):
    DBMS_JOB = 0
    R_CONSTRAINT = 1
    TABLE = 2
    VIEW = 3
    PACKAGE = 4
    PROCEDURE = 5
    FUNCTION = 6
    SEQUENCE = 7
    SYNONYM = 8
    TYPE = 9
    JOB = 10
    CHAIN = 11
    PROGRAM_ARG = 12
    PROGRAM = 13
    CREDENTIALS = 14

    def __lt__(self, other):
        return self.value < other.value

    @property
    def priority(self):
        return self.value


@total_ordering
@dataclass(frozen=True)
class SchemaObject:
    type: SchemaObjectType
    schema: str
    name: str

    def __eq__(self, other: Self):
        return self.type.priority == other.type.priority

    def __lt__(self, other: Self):
        return self.type.priority < other.type.priority


@dataclass(frozen=True)
class RefConstraintObject(SchemaObject):
    table_name: str = field(compare=False)


def purge_recycle_bin(schema: str, conn: Connection) -> None:
    with conn.cursor() as cur:
        if schema.casefold() == EXECUTING_USER.casefold():
            cur.execute(constants.SQL_PURGE_USER_RECYCLE_BIN)
        else:
            cur.execute(
                constants.SQL_GET_SCHEMA_TABLESPACES, parameters=dict(owner=schema)
            )
            for row in cur.fetchall():
                # Cannot accept bind parameters on this statement.
                cur.execute(
                    constants.SQL_PURGE_RECYCLE_BIN.format(
                        tablespace=row[0], owner=schema
                    ),
                )


def get_dbms_jobs(schema: str, conn: Connection) -> Iterator[SchemaObject]:
    with conn.cursor() as cur:
        cur.execute(constants.SQL_GET_JOBS, parameters=dict(owner=schema))
        for row in cur.fetchall():
            yield SchemaObject(SchemaObjectType.DBMS_JOB, row[0], row[1])


def get_all_objects(schema: str, conn: Connection) -> Iterator[SchemaObject]:
    with conn.cursor() as cur:
        for kind in SchemaObjectType:
            cur.execute(
                constants.SQL_GET_OBJECT_TYPE,
                parameters=dict(owner=schema, kind=kind.name),
            )
            for row in cur.fetchall():
                yield SchemaObject(SchemaObjectType[row[0]], row[1], row[2])


def get_ref_constraints(schema: str, conn: Connection) -> Iterator[RefConstraintObject]:
    with conn.cursor() as cur:
        cur.execute(constants.SQL_GET_REF_CONSTRAINTS, parameters=dict(owner=schema))
        for row in cur.fetchall():
            yield RefConstraintObject(
                SchemaObjectType.R_CONSTRAINT, row[0], row[1], row[2]
            )


def get_db_credentials(schema: str, conn: Connection) -> Iterator[SchemaObject]:
    with conn.cursor() as cur:
        cur.execute(constants.SQL_GET_CREDS, parameters=dict(owner=schema))
        for row in cur.fetchall():
            yield SchemaObject(SchemaObjectType.CREDENTIALS, row[0], row[1])


def get_object_count(schema: str, conn: Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(constants.SQL_GET_USER_OBJ_COUNT, parameters=dict(owner=schema))
        return int(cast(tuple, cur.fetchone())[0])


def init_session(conn: Connection, requested_tag: str) -> None:
    with conn.cursor() as cur:
        cur.execute(constants.SQL_SET_DDL_LOCK_TIMEOUT)
        cur.callproc("DBMS_APPLICATION_INFO.set_client_info", [constants.SERVICE_NAME])


def check_for_schema_connections(schema: str, conn: Connection) -> None:
    """
    Check to see if there are any current connections for the target schema
    """
    with conn.cursor() as cur:
        try:
            cur.execute(
                constants.SQL_GET_SCHEMA_CONNECTION_CNT,
                parameters=dict(owner=schema, service_name=constants.SERVICE_NAME),
            )
            connections: int = cast(tuple[int], cur.fetchone())[0]
            logger.debug("current target schema connections: %s", connections)
        except oracledb.DatabaseError as exception:
            (error_obj,) = exception.args
            if error_obj.code == constants.TABLE_DOES_NOT_EXIST:
                logger.warning(
                    "%s does not have read permissions on v$session. "
                    "Cannot check for schema connections",
                    EXECUTING_USER,
                )
            else:
                raise exception
        else:
            if connections > 0:
                logger.warning(
                    "There are %s active connections on %s. All connections should be "
                    "terminated prior to cleaning schema!",
                    connections,
                    schema,
                )


def validate_schema_name(schema: str, conn: Connection):
    """
    Ensure schema is a legal string and is present in the database.
    """

    with conn.cursor() as cur:
        try:
            cur.execute(
                constants.SQL_VALIDATE_SCHEMA_NAME, parameters=dict(schema=schema)
            )
        except oracledb.DatabaseError as e:
            e.add_note(f"{schema} schema does not exist!")
            raise e


def conform_schema_name(schema: str, conn: Connection) -> str:
    """
    Convert supplied schema to uppercase. Prevent execution on mixed
    or lowercase schemas.
    """
    target_schema = schema.upper()

    with conn.cursor() as cur:
        cur.execute(constants.SQL_GET_NON_UPPERCASE_SCHEMAS)
        non_uppercase_schemas = map(lambda r: r[0], cur.fetchall())

    try:
        for s in non_uppercase_schemas:
            if target_schema == s.upper():
                raise ValueError(
                    "A mixed or lowercase schema was found that matches "
                    " the target schema and is not supported! target: "
                    f"{target_schema} schema: {s}"
                )
    except ValueError as e:
        logger.exception(e)
        raise e
    else:
        return target_schema


def get_drop_sql(schema_obj: SchemaObject) -> str:
    """
    Pattern match SchemaObject to fetch the drop SQL for the type.
    """
    match schema_obj:
        case RefConstraintObject(
            SchemaObjectType.R_CONSTRAINT, name, schema, table_name
        ):
            return f'alter table {schema}."{table_name}" drop constraint "{name}"'
        case SchemaObject(SchemaObjectType.DBMS_JOB, schema, name):
            if schema.casefold() == EXECUTING_USER.casefold():
                return f"begin dbms_job.remove('{name}');"
            else:
                return f"begin dbms_ijob.remove('{name}'); end;"
        case SchemaObject(SchemaObjectType.TABLE, schema, name):
            return f'drop table {schema}."{name}" cascade constraints purge'
        case SchemaObject(SchemaObjectType.VIEW, schema, name):
            return f'drop view {schema}."{name}"'
        case SchemaObject(SchemaObjectType.PACKAGE, schema, name):
            return f"drop package {schema}.{name}"
        case SchemaObject(SchemaObjectType.PROCEDURE, schema, name):
            return f"drop procedure {schema}.{name}"
        case SchemaObject(SchemaObjectType.FUNCTION, schema, name):
            return f"drop function {schema}.{name}"
        case SchemaObject(SchemaObjectType.SEQUENCE, schema, name):
            return f'drop sequence {schema}."{name}"'
        case SchemaObject(SchemaObjectType.SYNONYM, schema, name):
            return f'drop synonym {schema}."{name}"'
        case SchemaObject(SchemaObjectType.TYPE, schema, name):
            return f'drop type {schema}."{name}" force'
        case SchemaObject(SchemaObjectType.JOB, schema, name):
            return f"""
            begin
                dbms_scheduler.drop_job('{schema}.{name}', force => TRUE);
            end;
            """
        case SchemaObject(SchemaObjectType.CHAIN, schema, name):
            return f"""
            begin
                dbms_scheduler.drop_chain('{schema}.{name}', force => TRUE);
            end;
            """
        case SchemaObject(SchemaObjectType.PROGRAM, schema, name):
            return f"""
            begin
                dbms_scheduler.drop_program('{schema}.{name}', force => TRUE);
            end;
            """
        case SchemaObject(SchemaObjectType.CREDENTIALS, schema, name):
            return f"""
            begin
                dbms_credential.drop_credential('{schema}.{name}', force => TRUE);
            end;
            """
        case _:
            raise ValueError("Unhandled schema object %s", schema_obj)


def drop_objects(pool: ConnectionPool, q: queue.Queue[SchemaObject]) -> None:
    """
    Worker function that drops database objects from a queue.
    """
    with pool.acquire() as conn:
        with conn.cursor() as cur:
            while not q.empty():
                schema_obj = q.get()
                sql = get_drop_sql(schema_obj)
                logger.debug("dropping %s", schema_obj, extra=dict(sql=sql))
                try:
                    cur.execute(sql)
                except oracledb.Error as exception:
                    (error_obj,) = exception.args
                    exception.add_note(sql)

                    if error_obj.code in (constants.DEADLOCK_DETECTED,):
                        try:
                            RETRY_ERROR_Q.put_nowait(exception)
                        except queue.Full:
                            raise RuntimeError(
                                f"Max retry errors reached: {list(RETRY_ERROR_Q.queue)}"
                            )
                        else:
                            logger.warning(
                                'sql: "%s" returned: %s retrying...',
                                sql,
                                str(error_obj),
                            )
                            q.put(schema_obj)
                    else:
                        raise exception


def drop_object_type(pool: ConnectionPool, q: queue.Queue[SchemaObject], parallel: int):
    """
    Dispatch workers to drop objects. Queue elements should all be of the same
    SchemaObjectType.
    """
    drop_objects_fn = partial(drop_objects, pool, q)

    with ThreadPoolExecutor(max_workers=parallel) as executor:
        fut_results = [executor.submit(drop_objects_fn) for _ in range(parallel)]

        for fut in as_completed(
            fut_results, timeout=int(os.getenv("WORKER_TIMEOUT", 300))
        ):
            try:
                fut.result()
            except Exception as e:
                logger.error(f"sql: '{', '.join(e.__notes__)}' error: {e}")


def protected_schema_guard(target_schema: str, force: bool, conn: Connection):
    """
    Prevent protected schemas or schemas with a name matching the
    PROTECTED_SCHEMA_REGEX pattern from being dropped.
    """
    dotenv_path = pathlib.Path.cwd() / ".env"
    env_loaded = load_dotenv(dotenv_path)
    if not env_loaded:
        logger.debug("environmental file '%s' not found", dotenv_path)

    with conn.cursor() as cur:
        cur.execute(constants.SQL_GET_ORA_MAINTAINED_SCHEMAS)
        protected_schemas: set[str] = set(r[0] for r in cur.fetchall())

    protected_schema_pattern = os.getenv(
        "PROTECTED_SCHEMA_REGEX", constants.MATCH_NOTHING
    )

    logger.info(f"PROTECTED_SCHEMA_REGEX={protected_schema_pattern}")

    try:
        logger.debug("Schema is: %s", target_schema)
        if target_schema in protected_schemas:
            raise ValueError(
                f"Cannot execute against protected schema: {protected_schemas}"
            )

        if not force:
            if protected_schema_pattern == constants.MATCH_NOTHING:
                logger.warning(
                    "PROTECTED_SCHEMA_REGEX environmental is set to default value "
                    " which matches nothing! Set variable to a regular expression "
                    " pattern matching the schema names you wish to protect."
                )
            if re.match(protected_schema_pattern, target_schema, re.IGNORECASE):
                raise ValueError(
                    f"Target schema {target_schema} matches protected schema pattern! "
                    "Supply force:True arg to override",
                )
    except Exception as e:
        logger.exception(e)
        raise


def drop_all(
    user: str,
    password: str,
    host: str,
    database: str,
    target_schema: str,
    parallel=1,
    force=False,
) -> int:
    """
    Drop all database objects in a schema.
    """

    global EXECUTING_USER
    EXECUTING_USER = user

    pool = create_pool(
        user=user,
        password=password,
        dsn=f"{host}/{database}",
        max=parallel + 1,
        session_callback=init_session,
    )
    conn = pool.acquire()

    target_schema = conform_schema_name(target_schema, conn)
    protected_schema_guard(target_schema, force, conn)
    validate_schema_name(target_schema, conn)
    check_for_schema_connections(target_schema, conn)

    purge_recycle_bin(target_schema, conn)
    object_count = get_object_count(target_schema, conn)
    logger.info("%d objects in schema %s", object_count, target_schema)

    # Load database objects into a min-heap to ensure dependant object
    # types are dropped in the proper order.
    priority_queue = []
    for obj in itertools.chain(
        get_ref_constraints(target_schema, conn),
        get_dbms_jobs(target_schema, conn),
        get_db_credentials(target_schema, conn),
        get_all_objects(target_schema, conn),
    ):
        heapq.heappush(priority_queue, obj)

    # Separate database objects of each type into queues to dispatch to workers.
    for _, group in itertools.groupby(priority_queue):
        q = queue.Queue()
        for schema_obj in group:
            q.put(schema_obj)

        drop_object_type(pool, q, parallel)

    purge_recycle_bin(target_schema, conn)
    return get_object_count(target_schema, conn)
