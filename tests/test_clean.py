import logging

import pytest
from oracledb_clean_schema.core import drop_all

logger = logging.getLogger(__name__)

def test_protected_schema(connect_params, protected_schema):
    with pytest.raises(ValueError, match=r".* matches protected schema pattern!.*"):
        drop_all(**connect_params, target_schema=protected_schema)


def test_clean_schema(connect_params, target_schema):
    remaining = drop_all(**connect_params, target_schema=target_schema)
    logger.info("remaining: %s", remaining)
    assert remaining == 0
