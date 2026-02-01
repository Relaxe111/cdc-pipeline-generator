# CDC Pipeline Project

Generated with `cdc init`.

## Quick Start

### 1. Enter Dev Container

```bash
docker compose up -d
docker compose exec dev fish
```

### 2. Configure Server Groups

Create a new server group (this will automatically update docker-compose.yml with database services):

```bash
# For MSSQL source (db-per-tenant pattern)
cdc manage-server-group --create my-group \
  --pattern db-per-tenant \
  --source-type mssql \
  --extraction-pattern '(?P<customer_id>\w+)_(?P<env>\w+)'

# For PostgreSQL source (db-shared pattern)
cdc manage-server-group --create my-group \
  --pattern db-shared \
  --source-type postgresql \
  --extraction-pattern '(?P<customer_id>\w+)' \
  --environment-aware
```

This automatically:
- Creates `server-groups.yaml`
- Updates `docker-compose.yml` with MSSQL/PostgreSQL services
- Adds volume definitions
- Configures service dependencies

### 3. Create Service Configuration

```bash
cdc manage-service --create my-service --server-group my-server-group
```

### 4. Add Tables to Service

```bash
cdc manage-service --service my-service --add-table MyTable --primary-key id
```

### 5. Generate Pipelines

```bash
cdc generate --service my-service --environment dev
```

## Project Structure

```
.
├── docker-compose.yml           # Dev container configuration
├── Dockerfile.dev               # Dev container image
├── server-groups.yaml           # Server group definitions
├── 2-services/                  # Service configurations (auto-created)
├── 2-customers/                 # Customer configurations (auto-created)
├── 3-pipeline-templates/        # Pipeline templates (auto-created)
└── generated/                   # Generated pipelines and schemas
    ├── pipelines/
    ├── schemas/
    └── table-definitions/
```

## Documentation

- [Generator Documentation](https://github.com/carasent/cdc-pipeline-generator)
- [Service Management](https://github.com/carasent/cdc-pipeline-generator#cli-commands)
- [Pipeline Generation](https://github.com/carasent/cdc-pipeline-generator#architecture-patterns)

## Environment Variables

Create `.env` file for your database credentials:

```bash
# Source database (MSSQL example)
MSSQL_HOST=localhost
MSSQL_PORT=1433
MSSQL_USER=sa
MSSQL_PASSWORD=YourPassword123

# Target database (PostgreSQL)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=cdc_target
```
