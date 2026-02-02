# CDC Pipeline Generator

[![Docker Hub](https://img.shields.io/docker/v/asmacarma/cdc-pipeline-generator?label=docker&logo=docker)](https://hub.docker.com/r/asmacarma/cdc-pipeline-generator)

**Generate Redpanda Connect pipeline configurations for Change Data Capture (CDC) workflows.**

A CLI-first tool for managing CDC pipelines with automatic Docker dev container setup, supporting both **db-per-tenant** (one database per customer) and **db-shared** (single database, multi-tenant) patterns.

## ✨ Features

- 🚀 **Zero-dependency setup**: Only Docker required
- 🐳 **Docker-first**: Run from Docker Hub image - no local installation needed
- 🔄 **Multi-tenant patterns**: Support for db-per-tenant and db-shared architectures
- 📝 **Template-based generation**: Jinja2 templates for flexible pipeline configuration
- ✅ **CLI-first philosophy**: All operations via `cdc` commands, no manual YAML editing
- 🛠️ **Database integration**: Auto-updates docker-compose.yml with database services
- 🔖 **Automated releases**: Semantic versioning with conventional commits

## 📦 Installation

**Only Docker required - zero dependencies!**

Supports **Intel (x86_64)** and **Apple Silicon (ARM64)** platforms.

```bash
# Pull latest version
docker pull asmacarma/cdc-pipeline-generator:latest

# Verify platform support
docker image inspect asmacarma/cdc-pipeline-generator:latest | grep Architecture
```

## 🔄 Updating

```bash
# Pull latest version
docker pull asmacarma/cdc-pipeline-generator:latest
```

## 🚀 Quick Start (Docker Compose Workflow)

> **⚠️ CLI-First Philosophy**: All configuration is managed through `cdc` commands. **Never edit YAML files manually.** The CLI is the sole interface for configuration management.

### 1. Create Docker Compose File

Create a `docker-compose.yml` in your project directory:

```yaml
services:
  dev:
    image: asmacarma/cdc-pipeline-generator:latest
    volumes:
      - .:/workspace
    working_dir: /workspace
    stdin_open: true
    tty: true
    entrypoint: ["/bin/bash", "-c"]
    command: ["fish"]

# When you run 'cdc scaffold', database services (mssql/postgres) will be
# automatically inserted below, while this dev service remains unchanged.

# Version pinning options:
# - :latest - Always pulls newest version (auto-updates on docker compose pull)
# - :0      - Pins to major version 0.x.x (stable, gets minor/patch updates)
# - :0.2    - Pins to minor version 0.2.x (only patch updates)
# - :0.2.4  - Pins to exact version (no updates)
```

**Version strategy:**
- Development: Use `:latest` for newest features
- Production: Use `:0` to auto-update within major version
- Critical systems: Use exact version like `:0.2.4`

**⚠️ Important:** This docker-compose.yml will be **automatically updated** when you run `cdc scaffold`. New database services will be inserted while preserving the `dev` service.

### 2. Initialize Project and Start Dev Container

```bash
# Create project directory
mkdir my-cdc-project
cd my-cdc-project

# Copy the docker-compose.yml from above, then initialize:
docker compose run --rm dev init
# ✅ Creates project structure, Dockerfile.dev, pipeline templates, directories

# Start the dev container
docker compose up -d

# Enter the dev container shell
docker compose exec dev fish
# 🐚 You are now inside the container with full cdc CLI and Fish completions
```

**Inside the dev container**, you'll see a Fish shell prompt with:
- ✅ `cdc` command available with tab completion
- ✅ All dependencies pre-installed
- ✅ Your project directory mounted at `/workspace`

### 3. Scaffold Server Group (Inside Dev Container)

**Now working inside the container shell**, run the scaffold command:

```fish
# 🐚 Inside dev container

# For db-per-tenant pattern (one database per customer)
cdc scaffold my-group \
  --pattern db-per-tenant \
  --source-type mssql \
  --extraction-pattern "^myapp_(?P<customer>[^_]+)$"

# For db-shared pattern (multi-tenant, single database)
cdc scaffold my-group \
  --pattern db-shared \
  --source-type postgres \
  --extraction-pattern "^myapp_(?P<service>[^_]+)_(?P<env>(dev|stage|prod))$" \
  --environment-aware
```

**Required flags explained:**

| Flag | Values | Description |
|------|--------|-------------|
| `--pattern` | `db-per-tenant` or `db-shared` | Choose your multi-tenancy model |
| `--source-type` | `postgres` or `mssql` | Source database type |
| `--extraction-pattern` | Regex string | Pattern to extract identifiers from DB names |
| `--environment-aware` | (flag, no value) | **Required for db-shared only** - enables env grouping |

**Pattern-specific requirements:**

For `--pattern db-per-tenant`:
- Regex must have named group: `(?P<customer>...)`
- Example: `"^myapp_(?P<customer>[^_]+)$"` matches `myapp_customer1`

For `--pattern db-shared`:
- Regex must have named groups: `(?P<service>...)` and `(?P<env>...)`
- Must include `--environment-aware` flag
- Example: `"^myapp_(?P<service>users)_(?P<env>dev|stage|prod)$"`

**Fish shell autocomplete** (inside dev container):
- Type `cdc scaffold my-group --pattern ` + TAB → shows `db-per-tenant` and `db-shared`
- Type `cdc scaffold my-group --source-type ` + TAB → shows `postgres` and `mssql`

**What gets created:**
- ✅ `server_group.yaml` with your configuration
- ✅ **Updates `docker-compose.yml`** - inserts database services (mssql/postgres) after `dev` service
- ✅ Directory structure: `services/`, `generated/`, `pipeline-templates/`
- ✅ Connection credentials use env vars: `${POSTGRES_SOURCE_HOST}`, etc.

**Docker Compose update example:**
After scaffold, your docker-compose.yml will have new services added:
```yaml
services:
  dev:  # ← Your original service (preserved)
    image: asmacarma/cdc-pipeline-generator:latest
    # ... unchanged ...
  
  mssql:  # ← Added by scaffold
    image: mcr.microsoft.com/mssql/server:2022-latest
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_SA_PASSWORD: ${MSSQL_PASSWORD}
  
  postgres-target:  # ← Added by scaffold
    image: postgres:16-alpine
    environment:
      POSTGRES_PASSWORD: ${POSTGRES_TARGET_PASSWORD}
```

### 4. Configure Environment Variables

```bash
# Copy example and edit with your credentials
cp .env.example .env
nano .env  # or use your preferred editor
```

Example `.env`:
```bash
# Source Database (MSSQL)
MSSQL_HOST=mssql
MSSQL_PORT=1433
MSSQL_USER=sa
MSSQL_PASSWORD=YourPassword123!

# Target Database (PostgreSQL)
POSTGRES_TARGET_HOST=postgres-target
POSTGRES_TARGET_PORT=5432
POSTGRES_TARGET_USER=postgres
POSTGRES_TARGET_PASSWORD=postgres
POSTGRES_TARGET_DB=cdc_target
```

### 5. Start All Services

```bash
# Exit container temporarily
exit

# Start databases and dev container
docker compose up -d

# Re-enter dev container
docker compose exec dev fish
```

### 6. Create Service and Add Tables

```bash
# Create service
cdc manage-service --create my-service

# Add tables to track
cdc manage-service --service my-service --add-table Users --primary-key id
cdc manage-service --service my-service --add-table Orders --primary-key order_id

# Inspect available tables (optional)
cdc manage-service --service my-service --inspect --schema dbo
```

### 7. Generate CDC Pipelines

```bash
# Generate pipelines for development environment
cdc generate-pipelines --service my-service --environment dev

# Check generated files
ls generated/pipelines/
ls generated/schemas/
```

### 8. Deploy Pipelines

Generated pipeline files in `generated/pipelines/` are ready to deploy to your Redpanda Connect infrastructure.

---

## 📋 Complete Command Reference

### Project Initialization

```bash
docker run --rm -v $PWD:/workspace -w /workspace asmacarma/cdc-pipeline-generator:latest init
```

### Scaffolding (New in 0.2.x)

```bash
docker run --rm -v $PWD:/workspace -w /workspace asmacarma/cdc-pipeline-generator:latest scaffold <name> \
  --pattern <db-per-tenant|db-shared> \
  --source-type <postgres|mssql> \
  --extraction-pattern "<regex>" \
  [--environment-aware]

# Required for db-per-tenant:
#   --pattern db-per-tenant
#   --source-type postgres|mssql
#   --extraction-pattern with 'customer' named group

# Required for db-shared:
#   --pattern db-shared
#   --source-type postgres|mssql
#   --extraction-pattern with 'service' and 'env' named groups
#   --environment-aware (mandatory flag)

# Optional connection overrides:
#   --host <host>         # Default: ${POSTGRES_SOURCE_HOST} or ${MSSQL_SOURCE_HOST}
#   --port <port>         # Default: ${POSTGRES_SOURCE_PORT} or ${MSSQL_SOURCE_PORT}
#   --user <user>         # Default: ${POSTGRES_SOURCE_USER} or ${MSSQL_SOURCE_USER}
#   --password <password> # Default: ${POSTGRES_SOURCE_PASSWORD} or ${MSSQL_SOURCE_PASSWORD}

# Example patterns:
# - db-per-tenant: "^adopus_(?P<customer>[^_]+)$"
# - db-shared: "^asma_(?P<service>[^_]+)_(?P<env>(dev|stage|prod))$"
# - Empty pattern "" for simple fallback matching
```

### Service Management

```bash
# Create service
cdc manage-service --create <name>

# Add tables
cdc manage-service --service <name> --add-table <TableName> --primary-key <column>

# Remove tables
cdc manage-service --service <name> --remove-table <TableName>

# Inspect database schema
cdc manage-service --service <name> --inspect --schema <schema-name>
```

### Pipeline Generation

```bash
# Generate all pipelines
cdc generate-pipelines --service <name> --environment <dev|stage|prod>

# Generate with snapshot
cdc generate-pipelines --service <name> --environment dev --snapshot
```

# Show server group info
cdc manage-server-group --info

# List all server groups
cdc manage-server-group --list
```

### Pipeline Generation

```bash
# Generate for specific service
cdc generate --service <name> --environment <dev|stage|prod>

# Generate for all services
cdc generate --all --environment <env>
```

### Validation

```bash
# Validate all configurations
cdc validate
```

---

### db-per-tenant (One database per customer)

**Use case:** Each customer has a dedicated source database.

**Example:** AdOpus system with 26 customer databases.

**Pipeline generation:** Creates one source + sink pipeline per customer.

See: [`examples/db-per-tenant/`](examples/db-per-tenant/)

### db-shared (Single database, multi-tenant)

**Use case:** All customers share one database, differentiated by `customer_id`.

**Example:** ASMA directory service with customer isolation via schema/column.

**Pipeline generation:** Creates one source + sink pipeline for all customers.

See: [`examples/db-shared/`](examples/db-shared/)

---

## 🏗️ Architecture Patterns

### db-per-tenant (One database per customer)

**Use case:** Each customer has a dedicated source database.

**Example:** SaaS application with isolated customer databases (customer_a_prod, customer_b_prod, etc.)

**Pipeline generation:** Creates one source + sink pipeline per customer database.

**Setup:**
```bash
cdc manage-server-group --create my-group \
  --pattern db-per-tenant \
  --source-type mssql \
  --extraction-pattern '(?P<customer_id>\w+)_(?P<env>\w+)'
```

### db-shared (Single database, multi-tenant)

**Use case:** All customers share one database, differentiated by `customer_id` column or schema.

**Example:** Multi-tenant application with customer isolation via tenant_id field

**Pipeline generation:** Creates one source + sink pipeline for all customers, with customer filtering.

**Setup:**
```bash
cdc manage-server-group --create my-group \
  --pattern db-shared \
  --source-type postgresql \
  --extraction-pattern '(?P<customer_id>\w+)' \
  --environment-aware
```

---

## 🐳 Docker Container Workflow

```
cdc-pipeline-generator/
├── cdc_generator/           # Core library
│   ├── core/               # Pipeline generation logic
│   ├── helpers/            # Utility functions
│   ├── validators/         # Configuration validation
│   └── cli/                # Command-line interface
└── examples/               # Reference implementations
    ├── db-per-tenant/     # Multi-database pattern
    └── db-shared/         # Single-database pattern
```

---

## 🐳 Docker Container Workflow

The recommended way to use this tool is inside the auto-generated dev container:

### Why Use the Container?

✅ **Isolated environment** - No conflicts with host Python/packages  
✅ **All dependencies pre-installed** - Python 3.11, Fish shell, database clients  
✅ **Database services included** - MSSQL/PostgreSQL auto-configured  
✅ **Consistent across team** - Same environment for everyone  

### Container Commands

```bash
# Start all services (databases + dev container)
docker compose up -d

# Enter dev container
docker compose exec dev fish

# Stop all services
docker compose down

# Rebuild container (after updating generator version)
docker compose up -d --build

# View logs
docker compose logs -f dev
docker compose logs -f mssql
docker compose logs -f postgres-target
```

### Working Inside Container

Once inside (`docker compose exec dev fish`), you have:

- ✅ `cdc` command available
- ✅ Access to source and target databases
- ✅ Fish shell with auto-completions
- ✅ Git configured (via volume mount)
- ✅ SSH keys available (via volume mount)

All your project files are mounted at `/workspace`, so changes are reflected immediately.

---

## 📁 Project Structure

---

## 📁 Project Structure

After running `cdc init`, your project will have:

```
my-cdc-project/
├── docker-compose.yml           # Dev container + database services
├── Dockerfile.dev               # Container image definition
├── .env.example                 # Environment variables template
├── .env                         # Your credentials (git-ignored)
├── .gitignore                   # Git ignore rules
├── server-groups.yaml           # Server group config (generated by cdc)
├── README.md                    # Quick start guide
├── 2-services/                  # Service definitions (generated by cdc)
│   └── my-service.yaml
├── 2-customers/                 # Customer configs (for db-per-tenant)
├── 3-pipeline-templates/        # Custom pipeline templates (optional)
└── generated/                   # Generated output (git-ignored)
    ├── pipelines/               # Redpanda Connect pipeline YAML
    ├── schemas/                 # PostgreSQL schemas
    └── table-definitions/       # Table metadata
```

---

## 🔧 Advanced Usage

### Using as Python Library

```python
from cdc_generator.core.pipeline_generator import generate_pipelines

# Generate pipelines programmatically
generate_pipelines(
    service='my-service',
    environment='dev',
    output_dir='./generated/pipelines'
)
```

### Custom Pipeline Templates

Place custom Jinja2 templates in `3-pipeline-templates/`:

```yaml
# 3-pipeline-templates/source-pipeline.yaml
input:
  mssql_cdc:
    dsn: "{{ dsn }}"
    tables: {{ tables | tojson }}
    # Your custom configuration
```

### Environment-Specific Configuration

Use environment variables in server-groups.yaml:

```yaml
server:
  host: ${MSSQL_HOST}        # Replaced at runtime
  port: ${MSSQL_PORT}
  user: ${MSSQL_USER}
  password: ${MSSQL_PASSWORD}
```

---

## 🤝 Contributing

### For Library Contributors

If you want to contribute to the cdc-pipeline-generator library itself:

```bash
# Clone repository
git clone https://github.com/Relaxe111/cdc-pipeline-generator.git
cd cdc-pipeline-generator

# Install in editable mode with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black .
ruff check .
```

### For Users

If you're using the library in your project, just install from PyPI as shown in [Installation](#-installation).

---

## 📚 Resources
