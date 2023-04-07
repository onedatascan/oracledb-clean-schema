## Setup
- Requires Python 3.11+
- Install Make
- Install Docker
- Create and activate virtual environment
```
make create-venv
. .venv/bin/activate
```

## Build
```
make build
```

### Setup Test
Create a `.env` file in the `/tests` directory. Tests expect a schema to be present in the database built by docker-compose.

`### .env ###`
```
PROTECTED_SCHEMA_REGEX='PROD.*' # Regex for schemas you do not want to be cleaned without a force arg.
PROTECTED_SCHEMA=PROD           # Schema matching protected schema regex (does not need to exist)
ORACLE_USER=SYSTEM              # Test database user
ORACLE_PWD=manager              # Test database password
ORACLE_HOST=localhost           # Test database host
ORACLE_DATABASE=ORCLPDB1        # Test database name
PARALLEL=2                      # Number of clean workers
SCHEMA1=HR                      # Target schema to clean created during db startup
```

### Run tests
```
make test
make clean
`