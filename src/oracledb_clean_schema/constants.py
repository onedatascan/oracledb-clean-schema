SERVICE_NAME = "oracledb-clean-schema"
DEADLOCK_DETECTED = 60
TABLE_DOES_NOT_EXIST = 942
MATCH_NOTHING = "$."

SQL_GET_OBJECT_TYPE = """
select ao.object_type,
       ao.owner,
       ao.object_name
from all_objects ao
where ao.object_type = :kind
and not exists (
        select 1
        from all_tab_identity_cols atic
        where atic.sequence_name = ao.object_name
        and atic.owner = ao.owner
        and ao.object_type = 'SEQUENCE'
    )
and ao.owner = :owner
order by ao.object_name
"""

SQL_GET_REF_CONSTRAINTS = """
select constraint_name,
       owner,
       table_name
from all_constraints
where constraint_type = 'R'
and owner = :owner
"""

SQL_GET_CREDS = """
select owner,
       credential_name
FROM all_credentials
where owner = :owner
"""

SQL_GET_USER_OBJ_COUNT = """
select count(*)
from all_objects
where owner = :owner
"""

SQL_GET_ORA_MAINTAINED_SCHEMAS = """
select username
from dba_users
where oracle_maintained = 'Y'
"""

SQL_GET_NON_UPPERCASE_SCHEMAS = """
select username
from dba_users
where upper(username) != username
"""

SQL_GET_SCHEMA_TABLESPACES = """
select distinct tablespace_name
from dba_segments
where owner = :owner
"""

SQL_PURGE_RECYCLE_BIN = "purge tablespace {tablespace} USER {owner}"

SQL_PURGE_USER_RECYCLE_BIN = "purge recyclebin"

SQL_GET_JOBS = """
select job
from all_jobs
where log_user = :owner or schema_user = :owner
"""

SQL_SET_DDL_LOCK_TIMEOUT = "alter session set ddl_lock_timeout=30"

SQL_VALIDATE_SCHEMA_NAME = "select dbms_assert.schema_name(:schema) FROM dual"

SQL_GET_SCHEMA_CONNECTION_CNT = """
select count(*)
from v$session
where (client_info <> :service_name or client_info is null)
and schemaname = :owner
"""
