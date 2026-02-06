"""Template generation functions for project scaffolding."""


def get_docker_compose_template(server_group_name: str, pattern: str) -> str:
    """Generate docker-compose.yml template with server_group naming.
    
    Args:
        server_group_name: Name of the server group (e.g., 'adopus', 'asma')
        pattern: 'db-per-tenant' or 'db-shared'
        
    Returns:
        Complete docker-compose.yml content as string
    """
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
    image: asmacarma/cdc-pipeline-generator:latest
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
    entrypoint: ""

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
    # Redpanda starts by default for CDC streaming

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
    # Console starts by default with Redpanda

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
    # Adminer starts by default for database management

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


def get_env_example_template(
    server_group_name: str,
    pattern: str,
    source_type: str,
    kafka_topology: str = "shared",
    servers: "dict[str, dict[str, str]] | None" = None,
) -> str:
    """Generate .env.example template with multi-server support.
    
    Args:
        server_group_name: Name of the server group
        pattern: 'db-per-tenant' or 'db-shared'
        source_type: 'mssql' or 'postgres'
        kafka_topology: 'shared' or 'per-server'
        servers: Dict of server configurations (optional, for multi-server support)
        
    Returns:
        Complete .env.example content as string
    """
    # Default to single 'default' server if not provided
    if servers is None:
        servers = {"default": {"type": source_type}}

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

"""

    # Generate source database config for each server
    for server_name, server_config in servers.items():
        server_type = server_config.get("type", source_type)
        server_source_prefix = "POSTGRES_SOURCE" if server_type == "postgres" else "MSSQL_SOURCE"

        # Non-default servers get postfix
        postfix = "" if server_name == "default" else f"_{server_name.upper()}"

        if server_name == "default":
            content += f"""# ===========================================================================
# Source Database Configuration ({server_type.upper()})
# ===========================================================================
"""
        else:
            content += f"""# ===========================================================================
# Source Database Configuration - {server_name.upper()} Server ({server_type.upper()})
# ===========================================================================
"""

        content += f"""{server_source_prefix}_HOST{postfix}=
{server_source_prefix}_PORT{postfix}={"5432" if server_type == "postgres" else "1433"}
{server_source_prefix}_USER{postfix}=
{server_source_prefix}_PASSWORD{postfix}=
{server_source_prefix}_DB{postfix}=

"""

        if server_type == "mssql" and server_name == "default":
            content += """# ===========================================================================
# Optional: Local MSSQL Configuration (for testing)
# ===========================================================================
# Used when running with --profile local-mssql
MSSQL_SA_PASSWORD=YourStrong!Passw0rd
MSSQL_PORT=1433

"""

    content += """# ===========================================================================
# Target PostgreSQL Configuration
# ===========================================================================
POSTGRES_LOCAL_USER=postgres
POSTGRES_LOCAL_PASSWORD=postgres
POSTGRES_LOCAL_DB=""" + f"{server_group_name}_db" + """
POSTGRES_PORT=5432

"""

    # Kafka configuration based on topology
    if kafka_topology == "per-server":
        content += """# ===========================================================================
# Redpanda/Kafka Configuration (per-server topology)
# ===========================================================================
# Each server has its own Kafka cluster
"""
        for server_name in servers.keys():
            postfix = f"_{server_name.upper()}"
            content += f"""KAFKA_BOOTSTRAP_SERVERS{postfix}=redpanda:9092
"""
        content += """REDPANDA_SCHEMA_REGISTRY=http://redpanda:8081

"""
    else:
        content += """# ===========================================================================
# Redpanda/Kafka Configuration (shared topology)
# ===========================================================================
KAFKA_BOOTSTRAP_SERVERS=redpanda:9092
REDPANDA_BROKERS=redpanda:9092
REDPANDA_SCHEMA_REGISTRY=http://redpanda:8081

"""

    content += """# ===========================================================================
# CDC Pipeline Configuration
# ===========================================================================
CDC_BUFFER_SIZE=1000
CDC_BATCH_TIMEOUT=5s
CDC_MAX_IN_FLIGHT=64
"""

    return content


def get_readme_template(server_group_name: str, pattern: str) -> str:
    """Generate README.md template.
    
    Args:
        server_group_name: Name of the server group
        pattern: 'db-per-tenant' or 'db-shared'
        
    Returns:
        Complete README.md content as string
    """
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
   cdc manage-source-groups --update
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
│   └── pg-migrations/  # PostgreSQL migrations
├── source-groups.yaml   # Server group configuration
├── docker-compose.yml  # Infrastructure services
└── .env               # Environment variables (not in git)
```

**Note**: All helper scripts are in the cdc-pipeline-generator project and accessed via `cdc` commands.

## Documentation

See the `_docs/` directory for detailed documentation.
"""


def get_gitignore_template() -> str:
    """Generate .gitignore template.
    
    Returns:
        Complete .gitignore content as string
    """
    return """.env
.venv
__pycache__/
*.pyc
.pytest_cache/
.lsn_cache/
generated/pipelines/*
generated/schemas/*
!generated/**/.gitkeep
.DS_Store
*.swp
*.swo
*~
"""


# Re-export pipeline templates from dedicated module
from .pipeline_templates import (
    get_sink_pipeline_template,
    get_source_pipeline_template,
)

__all__ = [
    "get_docker_compose_template",
    "get_env_example_template",
    "get_gitignore_template",
    "get_readme_template",
    "get_sink_pipeline_template",
    "get_source_pipeline_template",
]

