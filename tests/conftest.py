import logging
from typing import cast

import pytest
import oracledb
from oracledb.connection import Connection
from dotenv import dotenv_values, load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(override=True)

CONNECTION: Connection | None = None


def mock_env_config() -> dict[str, str | int]:
    config_map = dotenv_values(verbose=True)
    assert config_map["ORACLE_USER"]
    assert config_map["ORACLE_PWD"]
    assert config_map["ORACLE_HOST"]
    assert config_map["ORACLE_DATABASE"]
    assert config_map["PARALLEL"]
    assert config_map["SCHEMA1"]
    return cast(dict[str, str | int], config_map)


@pytest.fixture(scope="session", autouse=True)
def env_config():
    return mock_env_config()


@pytest.fixture(scope="session", autouse=True)
def connect_params(env_config):
    return {
        "user": env_config["ORACLE_USER"],
        "password": env_config["ORACLE_PWD"],
        "host": env_config["ORACLE_HOST"],
        "database": env_config["ORACLE_DATABASE"],
    }


@pytest.fixture(scope="session", autouse=True)
def connection(connect_params) -> Connection:
    global CONNECTION
    if CONNECTION:
        return CONNECTION

    CONNECTION = oracledb.connect(
        user=connect_params["user"],
        password=connect_params["password"],
        dsn=f"{connect_params['host']}/{connect_params['database']}",
    )
    return CONNECTION


@pytest.fixture(scope="session", autouse=True)
def target_schema(env_config):
    return env_config["SCHEMA1"]


@pytest.fixture(scope="session", autouse=True)
def protected_schema(env_config):
    return env_config["PROTECTED_SCHEMA"]


@pytest.fixture(scope="session", autouse=True)
def run_tests(connection, target_schema):
    yield
    logger.info("Dropping schema %s", target_schema)
    with connection.cursor() as cursor:
        cursor.execute(f"drop user {target_schema} cascade")
    connection.close()
