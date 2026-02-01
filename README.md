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

> **⚠️ CLI-First Philosophy**: All configuration is managed through `cdc` commands. **Do not create or edit YAML files manually.** The CLI is the sole entry point for managing configuration files.

### 1. Create Service

```bash
# Create a new service configuration
cdc manage-service --create adopus --server-group adopus

# Add tables to the service
cdc manage-service --service adopus --add-table Actor --primary-key actno
cdc manage-service --service adopus --add-table Fraver --primary-key fraverid
```

### 2. Configure Server Group

```bash
# Add server group (interactive prompts for server_group_type, server details, etc.)
cdc manage-server-group --add-group adopus

# Or update server group configuration from database inspection
cdc manage-server-group --update
```

### 3. Generate Pipelines

```bash
# Generate CDC pipelines for a service
cdc generate --service adopus --environment local
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

All configuration is managed through `cdc` commands:

### Service Management
```bash
# Create new service
cdc manage-service --create <service-name> --server-group <group-name>

# Add tables to service
cdc manage-service --service <name> --add-table <TableName> --primary-key <column>

# Inspect source database schema
cdc manage-service --service <name> --inspect --schema dbo

# Remove table from service
cdc manage-service --service <name> --remove-table <TableName>
```

### Server Group Management
```bash
# Add new server group (interactive)
cdc manage-server-group --add-group <name>

# Update server group from database inspection
cdc manage-server-group --update

# Refresh database/table metadata
cdc manage-server-group --refresh
```

### Pipeline Generation
```bash
# Generate CDC pipelines for service
cdc generate --service <name> --environment <env>

# Generate for all services
cdc generate --all --environment <env>
```

### Validation
```bash
# Validate all configurations
cdc validate
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
