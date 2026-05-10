# CDC Pipeline Generator

**Generate pipeline configurations for Change Data Capture (CDC) workflows.**

A CLI-first tool that reads YAML service definitions and produces streaming pipeline configurations, SQL migrations, and deployment artifacts. Supports **db-per-tenant** and **db-shared** multi-tenancy patterns with configurable data transport backends.

---

## Architecture

The generator sits at the centre of a CDC pipeline — it reads source database schemas, produces sink table definitions and pipeline configurations, and renders the runtime artifacts consumed by the streaming layer.

### Data Transport Options

CDC data can be moved from source to sink through one of two paths:

| Path | Transport | Typical Use |
|------|-----------|-------------|
| **Streaming** | Redpanda / Kafka | High-throughput, low-latency CDC with exactly-once semantics |
| **FDW** | PostgreSQL Foreign Data Wrappers | Direct MSSQL→PG pull without an external message broker |

#### Streaming (Redpanda / Kafka)

Source change events are captured, streamed through a message broker, and consumed by sink processors that write to the target PostgreSQL database.

```text
MSSQL → CDC capture → Redpanda/Kafka → Bento sink → PostgreSQL
```

#### FDW (Foreign Data Wrapper)

The generator can produce configurations that use PostgreSQL Foreign Data Wrappers (`tds_fdw`) to pull data directly from MSSQL into staging tables, followed by merge procedures that apply changes to the target tables.

```text
MSSQL ← tds_fdw ← PostgreSQL (staging → merge → target)
```

#### Native PostgreSQL-to-PostgreSQL

For PostgreSQL source databases, native logical replication or polling-based CDC can be used without an external broker.

```text
PostgreSQL → native CDC polling → PostgreSQL target
```

All three paths are configuration-driven — the generator produces the correct pipeline YAML, SQL migrations, and runtime helpers based on the chosen transport and source database type.

---

## Installation

### Option A: Docker (zero host dependencies)

```bash
docker pull asmacarma/cdc-pipeline-generator:latest
```

### Option B: Host install via pip

```bash
# Editable install for active development
pip install -e .

# Or install directly from the repository
pip install .
```

After host install, the `cdc` command is available on your shell PATH.

---

## Quick Start

### 1. Create a project and initialize

```bash
mkdir my-cdc-project && cd my-cdc-project
cdc init
```

This creates the project structure: `source-groups.yaml`, `services/`, `pipelines/`, directories.

### 2. Scaffold a server group

```bash
# db-per-tenant (one database per customer)
cdc scaffold my-group \
  --pattern db-per-tenant \
  --source-type mssql \
  --extraction-pattern "^myapp_(?P<customer>[^_]+)$"

# db-shared (single database, multi-tenant)
cdc scaffold my-group \
  --pattern db-shared \
  --source-type postgres \
  --extraction-pattern "^myapp_(?P<service>[^_]+)_(?P<env>(dev|stage|prod))$" \
  --environment-aware
```

### 3. Configure services and tables

```bash
# Create a service
cdc manage-services config --create-service my-service

# Add source tables
cdc manage-services config --service my-service --add-source-table dbo.Users --primary-key id
cdc manage-services config --service my-service --add-source-table dbo.Orders --primary-key order_id

# Inspect and save source schemas
cdc manage-services config --service my-service --inspect --all --save
```

### 4. Manage schemas and migrations

```bash
# Generate DDL migrations for the sink database
cdc manage-migrations generate

# Review changes
cdc manage-migrations diff

# Apply migrations
cdc manage-migrations apply
```

### 5. Generate pipeline configurations

```bash
# Generate for a single service
cdc generate --service my-service --environment dev

# Generate for all services
cdc generate --all --environment dev
```

---

## Multi-Tenancy Patterns

### db-per-tenant

Each customer has a dedicated source database. The generator creates one source+sink pipeline per customer database.

```
Extraction pattern: ^myapp_(?P<customer>[^_]+)$
Matches: myapp_customer_a, myapp_customer_b
```

### db-shared

All customers share a single database, differentiated by a column (e.g. `customer_id`) or schema. Requires `--environment-aware`.

```
Extraction pattern: ^myapp_(?P<service>[^_]+)_(?P<env>(dev|stage|prod))$
Matches: myapp_users_dev, myapp_users_prod
```

---

## Command Reference

| Command | Description |
| ------- | ----------- |
| `cdc init` | Initialize a new CDC project |
| `cdc scaffold <name>` | Scaffold a server group with database services |
| `cdc manage-services config` | Create, list, inspect services and tables |
| `cdc manage-services config --inspect-sink` | Inspect and save target sink schemas |
| `cdc manage-migrations generate` | Generate PostgreSQL DDL migrations |
| `cdc manage-migrations diff` | Show pending schema changes |
| `cdc manage-migrations apply` | Apply migrations to target database |
| `cdc generate` | Generate pipeline YAML configurations |
| `cdc manage-source-groups` | Manage source database groups |
| `cdc manage-sink-groups` | Manage sink/target groups |
| `cdc validate` | Validate all configurations |

---

## Project Structure

```text
cdc-pipeline-generator/
├── cdc_generator/           # Core library
│   ├── cli/                # Click command groups
│   ├── core/               # Pipeline generation, migration engine
│   ├── helpers/            # Database, FDW, MSSQL utilities
│   ├── service-schemas/    # YAML schema definitions and type adapters
│   ├── templates/          # Jinja2 pipeline templates
│   └── validators/         # Configuration and schema validation
├── tests/                   # Test suite
├── _docs/                   # Architecture, getting started, CLI reference
├── examples/                # db-per-tenant and db-shared reference implementations
├── setup.py / pyproject.toml  # Package metadata
└── Dockerfile               # Docker runtime image
```

---

## Development

See `_docs/getting-started/` for setup instructions, `_docs/architecture/` for design decisions, and `_docs/cli/` for the full CLI command reference.

The CDC CLI runs directly on the host. Install once and use `cdc` from any directory.

- ✅ `cdc` command available everywhere on your host
- ✅ Access to source and target databases
- ✅ Fish shell with auto-completions (reload with `cdc reload-cdc-autocompletions`)
- ✅ Git and SSH keys available

Optionally, a dev container is available if you prefer an isolated environment:
```bash
docker compose exec dev fish
```

---

## 📁 Project Structure

---

## 📁 Project Structure

After running `cdc scaffold`, your project will have:

```
my-cdc-project/
├── docker-compose.yml           # Optional infrastructure (databases, streaming)
├── Dockerfile.dev               # Optional dev container image
├── .env.example                 # Environment variables template
├── .env                         # Your credentials (git-ignored)
├── .gitignore                   # Git ignore rules
├── source-groups.yaml           # Server group config (generated by cdc)
├── README.md                    # Quick start guide
├── services/                    # Service definitions (generated by cdc)
│   └── my-service.yaml
├── pipelines/                   # Pipeline templates + generated YAML
│   ├── templates/               # source-pipeline.yaml, sink-pipeline.yaml
│   └── generated/
│       ├── sources/
│       └── sinks/
└── generated/                   # Generated non-pipeline output (git-ignored)
  ├── schemas/                 # PostgreSQL schemas
  └── pg-migrations/           # PostgreSQL migrations
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
  output_dir='./pipelines/generated'
)
```

### Custom Pipeline Templates

Place custom Jinja2 templates in `pipelines/templates/`:

```yaml
# pipelines/templates/source-pipeline.yaml
input:
  mssql_cdc:
    dsn: "{{ dsn }}"
    tables: {{ tables | tojson }}
    # Your custom configuration
```

### Environment-Specific Configuration

Use environment variables in source-groups.yaml:

```yaml
server:
  host: ${MSSQL_HOST}        # Replaced at runtime
  port: ${MSSQL_PORT}
  user: ${MSSQL_USER}
  password: ${MSSQL_PASSWORD}
```

### SQL-Based Source Custom Keys (Source + Sink)

Use custom keys to compute per-database values during `--update` and write them
into each source environment entry (for example `customer_id`).

```bash
# Source groups: persist SQL custom key definition
cdc manage-source-groups \
  --add-source-custom-key customer_id \
  --custom-key-value "SELECT customer_id FROM dbo.settings" \
  --custom-key-exec-type sql

# Run update to execute the SQL per discovered database
cdc manage-source-groups --update
```

```bash
# Sink groups: same custom key model
cdc manage-sink-groups \
  --sink-group sink_analytics \
  --add-source-custom-key customer_id \
  --custom-key-value "SELECT customer_id FROM public.settings" \
  --custom-key-exec-type sql

# Run sink update to execute SQL per discovered sink database
cdc manage-sink-groups --update --sink-group sink_analytics
```

Generated shape (simplified):

```yaml
sources:
  directory:
    schemas: [public]
    nonprod:
      server: default
      database: directory_db
      table_count: 42
      customer_id: cust-001
```

If a key returns no value for a specific server/database, the update continues and
prints a warning with that server/database context.

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
