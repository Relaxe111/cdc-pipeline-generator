"""Project scaffolding for new CDC pipeline implementations."""

from pathlib import Path
from typing import Dict, Any


def _get_docker_compose_template(server_group_name: str, pattern: str) -> str:
    """Generate docker-compose.yml template with server_group naming."""
    container_prefix = server_group_name.replace('_', '-')
    
    return f"""# =============================================================================
# Docker Compose: Infrastructure for {server_group_name.title()} CDC Pipeline
# =============================================================================
# This setup provides infrastructure services for CDC replication.
# 
# Services:
# - Dev Container (Python development environment with Fish shell)
# - PostgreSQL (target database for CDC)
# - Redpanda (Kafka-compatible streaming platform)
# - Redpanda Connect Source (Source DB CDC → Redpanda)
# - Redpanda Connect Sink (Redpanda → PostgreSQL)
# - Redpanda Console (Web UI for monitoring)
# - Adminer (Database management UI)
#
# Optional Services (use profiles):
# - SQL Server 2022 (use --profile local-mssql for local testing)
#   By default, we connect to remote source servers
#
# Usage:
# Usage:
#   docker compose up -d                          # Start dev container only
#   docker compose exec dev fish                  # Enter dev container
#   cdc setup-local --enable-local-sink           # Start PostgreSQL (target)
#   cdc setup-local --enable-local-source         # Start MSSQL (source)
#   cdc setup-local --enable-local-sink --enable-local-source  # Start both
# =============================================================================

services:
  # ===========================================================================
  # Dev Container - Python Development Environment (Standalone)
  # ===========================================================================
  dev:
    build:
      context: .
      dockerfile: Dockerfile.dev
    hostname: {container_prefix}-dev
    container_name: {container_prefix}-dev
    volumes:
      - .:/workspace
      - ~/.ssh:/root/.ssh:ro
      - ~/.gitconfig:/root/.gitconfig:ro
    environment:
      PYTHONPATH: /workspace
    working_dir: /workspace
    stdin_open: true
    tty: true
    command: fish
    networks:
      - cdc-network
    # No depends_on - dev container is standalone
    # Use 'cdc setup-local' to start databases on demand

  # ===========================================================================
  # PostgreSQL - CDC Target Database (Start with --enable-local-sink)
  # ===========================================================================
  postgres:
    image: postgres:17-alpine
    hostname: {container_prefix}-postgres
    container_name: {container_prefix}-replica
    ports:
      - "${{POSTGRES_PORT:-5432}}:5432"
    environment:
      POSTGRES_USER: ${{POSTGRES_LOCAL_USER:-postgres}}
      POSTGRES_PASSWORD: ${{POSTGRES_LOCAL_PASSWORD:-postgres}}
      POSTGRES_DB: ${{POSTGRES_LOCAL_DB:-{server_group_name}_db}}
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./scripts/postgres-init:/docker-entrypoint-initdb.d
    networks:
      - cdc-network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5
    profiles:
      - local-sink
      - full

  # ===========================================================================
  # Redpanda - Kafka-Compatible Streaming Platform
  # ===========================================================================
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:latest
    hostname: {container_prefix}-redpanda
    container_name: {container_prefix}-redpanda
    command:
      - redpanda
      - start
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
      - --pandaproxy-addr internal://0.0.0.0:8082,external://0.0.0.0:18082
      - --advertise-pandaproxy-addr internal://redpanda:8082,external://localhost:18082
      - --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:18081
      - --rpc-addr redpanda:33145
      - --advertise-rpc-addr redpanda:33145
      - --smp 1
      - --memory 1G
      - --mode dev-container
      - --default-log-level=info
    ports:
      - "18081:18081"  # Schema Registry
      - "18082:18082"  # Pandaproxy
      - "19092:19092"  # Kafka API
      - "19644:9644"   # Admin API
    networks:
      - cdc-network
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -E 'Healthy:.+true' || exit 1"]
      interval: 15s
      timeout: 3s
      retries: 5
      start_period: 5s
    profiles:
      - streaming
      - full

  # ===========================================================================
  # Redpanda Console - Web UI for Monitoring
  # ===========================================================================
  console:
    image: docker.redpanda.com/redpandadata/console:latest
    hostname: {container_prefix}-console
    container_name: {container_prefix}-console
    entrypoint: /bin/sh
    command: -c 'echo "$$CONSOLE_CONFIG_FILE" > /tmp/config.yml; /app/console'
    environment:
      CONFIG_FILEPATH: /tmp/config.yml
      CONSOLE_CONFIG_FILE: |
        kafka:
          brokers: ["redpanda:9092"]
          schemaRegistry:
            enabled: true
            urls: ["http://redpanda:8081"]
        redpanda:
          adminApi:
            enabled: true
            urls: ["http://redpanda:9644"]
    ports:
      - "8080:8080"
    networks:
      - cdc-network
    depends_on:
      - redpanda
    profiles:
      - streaming
      - full

  # ===========================================================================
  # Adminer - Database Management UI
  # ===========================================================================
  adminer:
    image: adminer:latest
    hostname: {container_prefix}-adminer
    container_name: {container_prefix}-adminer
    ports:
      - "8090:8080"
    environment:
      ADMINER_DEFAULT_SERVER: postgres
    networks:
      - cdc-network
    depends_on:
      - postgres
    profiles:
      - local-sink
      - full

  # ===========================================================================
  # Redpanda Connect Source (Source DB → Redpanda)
  # ===========================================================================
  # Uncomment and configure when ready to set up CDC source pipelines
  # redpanda-connect-source:
  #   image: docker.redpanda.com/redpandadata/connect:latest
  #   hostname: {container_prefix}-source
  #   container_name: {container_prefix}-source
  #   volumes:
  #     - ./generated/pipelines:/pipelines:ro
  #   command: run /pipelines/source-pipeline.yaml
  #   networks:
  #     - cdc-network
  #   depends_on:
  #     - redpanda

  # ===========================================================================
  # Redpanda Connect Sink (Redpanda → PostgreSQL)
  # ===========================================================================
  # Uncomment and configure when ready to set up CDC sink pipelines
  # redpanda-connect-sink:
  #   image: docker.redpanda.com/redpandadata/connect:latest
  #   hostname: {container_prefix}-sink
  #   container_name: {container_prefix}-sink
  #   volumes:
  #     - ./generated/pipelines:/pipelines:ro
  #   command: run /pipelines/sink-pipeline.yaml
  #   networks:
  #     - cdc-network
  #   depends_on:
  #     - redpanda
  #     - postgres

  # ===========================================================================
  # Optional: Local SQL Server 2022 (Start with --enable-local-source)
  # ===========================================================================
  mssql:
    image: mcr.microsoft.com/mssql/server:2022-latest
    hostname: {container_prefix}-mssql
    container_name: {container_prefix}-mssql
    profiles:
      - local-source
      - full
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_SA_PASSWORD: ${{MSSQL_SA_PASSWORD:-YourStrong!Passw0rd}}
      MSSQL_PID: Developer
      MSSQL_AGENT_ENABLED: "true"
    ports:
      - "${{MSSQL_PORT:-1433}}:1433"
    volumes:
      - mssql-data:/var/opt/mssql
      - ./scripts/mssql-init:/docker-entrypoint-initdb.d
    networks:
      - cdc-network
    healthcheck:
      test: /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "${{MSSQL_SA_PASSWORD:-YourStrong!Passw0rd}}" -Q "SELECT 1" -b -o /dev/null
      interval: 10s
      timeout: 3s
      retries: 10
      start_period: 10s

networks:
  cdc-network:
    name: {container_prefix}-network
    driver: bridge

volumes:
  postgres-data:
    name: {container_prefix}-postgres-data
  mssql-data:
    name: {container_prefix}-mssql-data
"""


def _get_env_example_template(server_group_name: str, pattern: str, source_type: str) -> str:
    """Generate .env.example template."""
    source_prefix = "POSTGRES_SOURCE" if source_type == "postgres" else "MSSQL_SOURCE"
    
    content = f"""# =============================================================================
# Environment Variables for {server_group_name.title()} CDC Pipeline
# =============================================================================
# This file contains all configuration for the CDC pipeline.
# DO NOT commit .env file to version control in production!
# 
# SETUP INSTRUCTIONS:
# 1. Copy this file: cp .env.example .env
# 2. Fill in actual values for your environment
# 3. Start services: docker compose up -d
# 
# MONITORING ACCESS:
# - Redpanda Console: http://localhost:8080 (no login)
# - Adminer: http://localhost:8090
#   PostgreSQL: postgres / postgres / {server_group_name}_db
# =============================================================================

# ===========================================================================
# Source Database Configuration ({source_type.upper()})
# ===========================================================================
{source_prefix}_HOST=
{source_prefix}_PORT={"5432" if source_type == "postgres" else "1433"}
{source_prefix}_USER=
{source_prefix}_PASSWORD=
{source_prefix}_DB=
"""

    if source_type == "mssql":
        content += """
# ===========================================================================
# Optional: Local MSSQL Configuration (for testing)
# ===========================================================================
# Used when running with --profile local-mssql
MSSQL_SA_PASSWORD=YourStrong!Passw0rd
MSSQL_PORT=1433
"""

    content += """
# ===========================================================================
# Target PostgreSQL Configuration
# ===========================================================================
POSTGRES_LOCAL_USER=postgres
POSTGRES_LOCAL_PASSWORD=postgres
POSTGRES_LOCAL_DB=""" + f"{server_group_name}_db" + """
POSTGRES_PORT=5432

# ===========================================================================
# Redpanda/Kafka Configuration
# ===========================================================================
REDPANDA_BROKERS=redpanda:9092
REDPANDA_SCHEMA_REGISTRY=http://redpanda:8081

# ===========================================================================
# CDC Pipeline Configuration
# ===========================================================================
CDC_BUFFER_SIZE=1000
CDC_BATCH_TIMEOUT=5s
CDC_MAX_IN_FLIGHT=64
"""

    return content


def _get_dockerfile_dev_template() -> str:
    """Generate Dockerfile.dev for development container."""
    return """FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    git \\
    fish \\
    curl \\
    postgresql-client \\
    freetds-dev \\
    gcc \\
    && rm -rf /var/lib/apt/lists/*

# Install CDC Pipeline Generator from PyPI
RUN pip install --no-cache-dir cdc-pipeline-generator

# Set up Fish shell
RUN fish -c "curl -sL https://raw.githubusercontent.com/jorgebucaran/fisher/main/functions/fisher.fish | source && fisher install jorgebucaran/fisher"

WORKDIR /workspace

# Note: Fish completions for cdc command are included in the installed package
# They will be available automatically when using the cdc command

# Default command
CMD ["fish"]
"""


def _get_readme_template(server_group_name: str, pattern: str) -> str:
    """Generate README.md template."""
    pattern_desc = "db-per-tenant" if pattern == "db-per-tenant" else "db-shared"
    
    return f"""# {server_group_name.title()} CDC Pipeline

CDC (Change Data Capture) pipeline for {server_group_name} using Redpanda Connect.

## Pattern

**{pattern_desc}**: {"Each customer has their own database" if pattern == "db-per-tenant" else "Multiple customers share databases with environment separation"}

## Quick Start

1. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

2. **Start infrastructure**:
   ```bash
   docker compose up -d
   ```

3. **Update server group configuration**:
   ```bash
   cdc manage-server-group --update
   ```

4. **Create service configuration**:
   ```bash
   cdc manage-service --service <service-name> --create-service
   ```

5. **Generate pipelines**:
   ```bash
   cdc generate
   ```

## Development

For development work, use the cdc-pipeline-generator dev container:

```bash
cd ../cdc-pipeline-generator
docker compose exec dev fish
cd /implementations/{server_group_name}
```

## Monitoring

- **Redpanda Console**: http://localhost:8080
- **Adminer (DB UI)**: http://localhost:8090
- **Redpanda Admin API**: http://localhost:19644

## Directory Structure

```
.
├── services/             # Service YAML configurations
├── pipeline-templates/   # Redpanda Connect pipeline templates
├── generated/           # Auto-generated files
│   ├── pipelines/      # Redpanda Connect pipelines
│   ├── schemas/        # Database schemas
│   └── table-definitions/ # Table definitions
├── server_group.yaml   # Server group configuration
├── docker-compose.yml  # Infrastructure services
└── .env               # Environment variables (not in git)
```

**Note**: All helper scripts are in the cdc-pipeline-generator project and accessed via `cdc` commands.

## Documentation

See the `docs/` directory for detailed documentation.
"""


def _create_vscode_settings() -> Dict[str, Any]:
    """Create .vscode/settings.json with useful defaults."""
    return {
        "files.associations": {
            "*.yaml": "yaml",
            "docker-compose*.yml": "dockercompose"
        },
        "yaml.schemas": {
            ".vscode/service-schema.json": "services/*.yaml"
        },
        "files.exclude": {
            "**/__pycache__": True,
            "**/.pytest_cache": True,
            "**/*.pyc": True,
            ".lsn_cache": True
        },
        "search.exclude": {
            "**/generated": True,
            "**/.venv": True
        }
    }


def scaffold_project_structure(
    server_group_name: str,
    pattern: str,
    source_type: str,
    project_root: Path
) -> None:
    """Create complete directory structure and template files for new implementation.
    
    Args:
        server_group_name: Name of the server group (e.g., 'adopus', 'asma')
        pattern: 'db-per-tenant' or 'db-shared'
        source_type: 'mssql' or 'postgres'
        project_root: Root directory of the implementation
    """
    # Create directory structure
    directories = [
        "services",
        "pipeline-templates",
        "generated/pipelines",
        "generated/schemas",
        "generated/table-definitions",
        "generated/pg-migrations",
        "docs",
        ".vscode",
        "service-schemas",
    ]
    
    for directory in directories:
        dir_path = project_root / directory
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"✓ Created directory: {directory}")
    
    # Create .gitkeep files in generated directories
    for gen_dir in ["pipelines", "schemas", "table-definitions", "pg-migrations"]:
        gitkeep = project_root / "generated" / gen_dir / ".gitkeep"
        gitkeep.touch()
    
    # Create template files
    files_to_create = {
        "docker-compose.yml": _get_docker_compose_template(server_group_name, pattern),
        "Dockerfile.dev": _get_dockerfile_dev_template(),
        ".env.example": _get_env_example_template(server_group_name, pattern, source_type),
        "README.md": _get_readme_template(server_group_name, pattern),
        ".gitignore": """.env
.venv
__pycache__/
*.pyc
.pytest_cache/
.lsn_cache/
generated/pipelines/*
generated/schemas/*
generated/table-definitions/*
!generated/**/.gitkeep
.DS_Store
*.swp
*.swo
*~
""",
    }
    
    # docker-compose.yml should always be created/overwritten with full CDC infrastructure
    # (init template only has basic postgres, server-group scaffold has full Redpanda setup)
    docker_compose_path = project_root / "docker-compose.yml"
    if docker_compose_path.exists():
        # Backup existing file
        backup_path = project_root / "docker-compose.yml.bak"
        import shutil
        shutil.copy2(docker_compose_path, backup_path)
        print(f"✓ Backed up existing docker-compose.yml to docker-compose.yml.bak")
    
    docker_compose_path.write_text(files_to_create["docker-compose.yml"])
    print(f"✓ Created file: docker-compose.yml (with Redpanda CDC infrastructure)")
    
    # Create other files (skip if they exist)
    for filename, content in files_to_create.items():
        if filename == "docker-compose.yml":
            continue  # Already handled above
        
        file_path = project_root / filename
        if not file_path.exists():  # Don't overwrite existing files
            file_path.write_text(content)
            print(f"✓ Created file: {filename}")
        else:
            print(f"⊘ Skipped (exists): {filename}")
    
    # Create .vscode/settings.json
    vscode_settings = project_root / ".vscode" / "settings.json"
    if not vscode_settings.exists():
        try:
            import json
            vscode_settings.write_text(json.dumps(_create_vscode_settings(), indent=2))
            print("✓ Created file: .vscode/settings.json")
        except ImportError:
            print("⚠️  Could not create .vscode/settings.json (json module not available)")
    
    # Create basic pipeline templates
    source_template = project_root / "pipeline-templates" / "source-pipeline.yaml"
    if not source_template.exists():
        source_template.write_text(f"""# Source Pipeline Template for {server_group_name}
# This file will be used to generate actual source pipelines
# See cdc-pipeline-generator examples for complete templates
""")
        print("✓ Created file: pipeline-templates/source-pipeline.yaml")
    
    sink_template = project_root / "pipeline-templates" / "sink-pipeline.yaml"
    if not sink_template.exists():
        sink_template.write_text(f"""# Sink Pipeline Template for {server_group_name}
# This file will be used to generate actual sink pipelines
# See cdc-pipeline-generator examples for complete templates
""")
        print("✓ Created file: pipeline-templates/sink-pipeline.yaml")
    
    print(f"\n✅ Project scaffolding complete for '{server_group_name}'!")
    print(f"\n📋 Next steps:")
    print(f"   1. cp .env.example .env")
    print(f"   2. Edit .env with your database credentials")
    print(f"   3. docker compose up -d")
    print(f"   4. cdc manage-server-group --update")
