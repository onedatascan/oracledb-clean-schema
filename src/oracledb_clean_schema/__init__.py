from oracledb_clean_schema.core import drop_all


def lambda_handler(event, context):
    from oracledb_clean_schema.entrypoints.aws_lambda import lambda_handler as handler
    return handler(event, context)
