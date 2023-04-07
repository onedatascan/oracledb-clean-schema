import argparse
import logging
import os

from oracledb_clean_schema.core import drop_all

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
    )

    parser = argparse.ArgumentParser(description="Drop all objects in Oracle schema")
    parser.add_argument("--user", required=True, help="Database login user")
    parser.add_argument("--password", required=True, help="Database login password")
    parser.add_argument("--host", required=True, help="Database service host")
    parser.add_argument("--database", required=True, help="Database service name")
    parser.add_argument(
        "--target-schema", required=True, help="Database schema to clear"
    )
    parser.add_argument(
        "--parallel", default=1, help="Number of worker threads", type=int
    )
    parser.add_argument(
        "--force", default=False, help="Override a protected schema exception"
    )

    args = parser.parse_args()
    remaining_object_count = drop_all(
        args.user,
        args.password,
        args.host,
        args.database,
        args.target_schema,
        args.parallel,
        args.force,
    )

    if remaining_object_count == 0:
        logger.info(
            f"{remaining_object_count} objects remaining in schema {args.target_schema}"
        )
    else:
        logger.error(
            f"{remaining_object_count} objects remaining in schema {args.target_schema}"
        )

    return exit(remaining_object_count)
