# CDC Pipeline Generator

**Generate Redpanda Connect pipeline configurations for Change Data Capture (CDC) workflows.**

This library provides tools to generate CDC pipeline configurations from templates and service definitions, supporting both **db-per-tenant** (one database per customer) and **db-shared** (single database, multi-tenant) patterns.

## Features

- 🔄 **Multi-tenant CDC patterns**: Support for both db-per-tenant and db-shared architectures
- 📝 **Template-based generation**: Jinja2 templates for flexible pipeline configuration
- ✅ **Validation**: Schema validation for service configurations
- 🛠️ **CLI tools**: Commands for managing services, server groups, and pipeline generation
- 🐍 **Python library**: Use as a library in your own projects

## Installation

### Development (Local)

```bash
# Clone the repository
git clone https://github.com/carasent/cdc-pipeline-generator.git
cd cdc-pipeline-generator

# Install in editable mode
pip install -e .
```

### Production (From GitHub)

```bash
pip install git+https://github.com/carasent/cdc-pipeline-generator.git@v1.0.0
```

## Quick Start

### 1. Create Server Groups Configuration

```yaml
# server-groups.yaml
server_groups:
  - name: adopus
    server_group_type: db-per-tenant
    service: adopus
    server:
      type: mssql
      host: ${MSSQL_HOST}
      port: ${MSSQL_PORT}
```

### 2. Create Service Configuration

```yaml
# services/adopus.yaml
service: adopus
server_group: adopus

source_tables:
  - schema: dbo
    tables:
      - name: Actor
      - name: Fraver
```

### 3. Generate Pipelines

```bash
cdc-generator generate --service adopus --environment local
```

## Architecture Patterns

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

## Project Structure

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

## Usage in Your Project

### Option 1: Mounted Development

**Best for:** Active development, testing changes immediately.

```yaml
# docker-compose.yml
services:
  dev:
    volumes:
      - .:/workspace
      - ../cdc-pipeline-generator:/generator:rw
    environment:
      PYTHONPATH: /generator:/workspace
```

```python
# requirements-dev.txt
-e /generator
```

### Option 2: Pinned Version

**Best for:** Production, stable deployments.

```python
# requirements.txt
cdc-pipeline-generator @ git+https://github.com/carasent/cdc-pipeline-generator.git@v1.2.0
```

## CLI Commands

```bash
# Generate pipelines
cdc-generator generate --service adopus --environment local

# Manage services
cdc-generator manage-service --service adopus --add-table Actor --primary-key actno
cdc-generator manage-service --service adopus --inspect --schema dbo

# Manage server groups
cdc-generator manage-server-group --update
cdc-generator manage-server-group --add-group asma

# Validate configurations
cdc-generator validate
```

## Development

### Setup

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black .
ruff check .
```

### Docker Development Container

```bash
docker compose up -d
docker compose exec dev fish
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Links

- **GitHub**: https://github.com/carasent/cdc-pipeline-generator
- **Issues**: https://github.com/carasent/cdc-pipeline-generator/issues
- **Documentation**: See `examples/` directory for reference implementations

## Example Projects

- **adopus-cdc-pipeline**: db-per-tenant pattern with MSSQL source
- **asma-cdc-pipeline**: db-shared pattern with PostgreSQL source
