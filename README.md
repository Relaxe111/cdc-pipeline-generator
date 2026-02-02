# CDC Pipeline Generator

[![Docker Hub](https://img.shields.io/docker/v/asmacarma/cdc-pipeline-generator?label=docker&logo=docker)](https://hub.docker.com/r/asmacarma/cdc-pipeline-generator)

**Generate Redpanda Connect pipeline configurations for Change Data Capture (CDC) workflows.**

A CLI-first tool for managing CDC pipelines with automatic Docker dev container setup, supporting both **db-per-tenant** (one database per customer) and **db-shared** (single database, multi-tenant) patterns.

## ✨ Features

- 🚀 **Zero-config setup**: `pip install` → `cdc init` → ready to develop
- 🐳 **Docker dev container**: Automatic environment setup with all dependencies
- 🔄 **Multi-tenant patterns**: Support for db-per-tenant and db-shared architectures
- 📝 **Template-based generation**: Jinja2 templates for flexible pipeline configuration
- ✅ **CLI-first philosophy**: All operations via `cdc` commands, no manual YAML editing
- 🛠️ **Database integration**: Auto-updates docker-compose.yml with database services

## 📦 Installation

```bash
pip install cdc-pipeline-generator
```

That's it! The `cdc` command is now available globally.

## 🚀 Quick Start (Recommended Workflow)

> **⚠️ CLI-First Philosophy**: All configuration is managed through `cdc` commands. **Never edit YAML files manually.** The CLI is the sole interface for configuration management.

### 1. Initialize New Project

```bash
# Create project directory
mkdir my-cdc-project
cd my-cdc-project

# Initialize with dev container
cdc init
# ✅ Creates docker-compose.yml, Dockerfile.dev, project structure
# ✅ Builds dev container with Python, Fish shell, all dependencies
# ✅ Prompts to start container automatically
```

### 2. Enter Dev Container

```bash
docker compose exec dev fish
# Now inside container with cdc commands ready to use
```

### 3. Create Server Group (Auto-configures Docker Compose)

```bash
# For MSSQL source (db-per-tenant pattern)
cdc manage-server-group --create my-group \
  --pattern db-per-tenant \
  --source-type mssql \
  --extraction-pattern '(?P<customer_id>\w+)_(?P<env>\w+)' \
  --host '${MSSQL_HOST}' \
  --port 1433 \
  --user '${MSSQL_USER}' \
  --password '${MSSQL_PASSWORD}'

# ✅ Creates server-groups.yaml
# ✅ Auto-updates docker-compose.yml with MSSQL + PostgreSQL services
# ✅ Adds volume definitions and service dependencies
```

Or for PostgreSQL source (db-shared pattern):

```bash
cdc manage-server-group --create my-group \
  --pattern db-shared \
  --source-type postgresql \
  --extraction-pattern '(?P<customer_id>\w+)' \
  --environment-aware \
  --host '${POSTGRES_SOURCE_HOST}' \
  --port 5432 \
  --user '${POSTGRES_SOURCE_USER}' \
  --password '${POSTGRES_SOURCE_PASSWORD}'
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
cdc manage-service --create my-service --server-group my-group

# Add tables to track
cdc manage-service --service my-service --add-table Users --primary-key id
cdc manage-service --service my-service --add-table Orders --primary-key order_id

# Inspect available tables (optional)
cdc manage-service --service my-service --inspect --schema dbo
```

### 7. Update Server Group (Populate Databases)

```bash
# Inspect source database and populate server-groups.yaml
cdc manage-server-group --update
# ✅ Auto-discovers databases
# ✅ Maps databases to environments (dev/stage/prod)
# ✅ Populates table counts and statistics
```

### 8. Generate CDC Pipelines

```bash
# Generate pipelines for development environment
cdc generate --service my-service --environment dev

# Check generated files
ls generated/pipelines/
ls generated/schemas/
```

### 9. Deploy Pipelines

Generated pipeline files in `generated/pipelines/` are ready to deploy to your Redpanda Connect infrastructure.

---

## 📋 Complete Command Reference

---

## 📋 Complete Command Reference

### Project Initialization

```bash
cdc init                      # Initialize new CDC project with dev container
```

### Service Management

```bash
# Create service
cdc manage-service --create <name> --server-group <group-name>

# Add tables
cdc manage-service --service <name> --add-table <TableName> --primary-key <column>

# Remove tables
cdc manage-service --service <name> --remove-table <TableName>

# Inspect database schema
cdc manage-service --service <name> --inspect --schema <schema-name>
```

### Server Group Management

```bash
# Create server group (auto-updates docker-compose.yml)
cdc manage-server-group --create <name> \
  --pattern <db-per-tenant|db-shared> \
  --source-type <mssql|postgresql> \
  --extraction-pattern '<regex>' \
  [--environment-aware]  # Required for db-shared

# Update from database inspection
cdc manage-server-group --update

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
