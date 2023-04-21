# oracledb-clean-schema
oracledb-clean-schema is a Python package for dropping all Oracle database objects
in a schema without dropping the user itself.

## Functionality
* Drop all database objects in an Oracle database schema without dropping the user.
* Objects can be dropped in parallel using multiple workers.

## Quick Start
There are three primary modes of usage:
* Python package
* CLI
* AWS Lambda

The environmental variable `PROTECTED_SCHEMA_REGEX` should be set prior to execution. The value must be a regular expression of schema names to guard against being dropped. The guard can be overridden by proving a `force=true` argument. A `.env` file in the current working directory can be used to supply this value.


### Python package
```python
import logging

from oracledb_clean_schema import drop_all

logging.basicConfig(level="DEBUG")

objs_remaining = drop_all(
    username="system",
    password="manager",
    hostname="localhost",
    database="orclpdb1",
    target_schema="hr",
    parallel=8
)
print(objs_remaining)
```

### CLI
```bash
oracledb-clean-schema --help
usage: oracledb-clean-schema [-h] --username USERNAME --password PASSWORD --hostname HOSTNAME --database DATABASE --target-schema
                             TARGET_SCHEMA [--parallel PARALLEL] [--force FORCE]

Drop all objects in Oracle schema

options:
  -h, --help            show this help message and exit
  --username USERNAME   Database login user
  --password PASSWORD   Database login password
  --hostname HOSTNAME   Database service host
  --database DATABASE   Database service name
  --target-schema TARGET_SCHEMA
                        Database schema to clear
  --parallel PARALLEL   Number of worker threads
  --force FORCE         Override a protected schema exception
```

```bash
oracledb-clean-schema --username hr --password hr --hostname localhost --database orclpdb1 --target-schema hr --parallel 8
```

### AWS Lambda
This example assumes the use of a custom domain name mapped to an API Gateway or ALB where the datapump lambda is mapped to a `clean` endpoint.

```bash
curl -XPOST "https://oracledb-util-api.somedomain.com/clean" -d \
'{
      "connection": {
        "username": "system",
        "password": "manager",
        "hostname": "some-host",
        "database": "ORCLPDB1",
        "secret": "Optional AWS SecretsManger secret name/arn with the above fields"
      },
      "payload": {
         "target_schema": "hr",
         "force": false,
         "parallel": 8
      }
}'
```