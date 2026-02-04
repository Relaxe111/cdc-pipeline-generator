"""Type definitions for server group configuration.

These TypedDict classes define the structure of server_group.yaml,
providing type safety, IDE autocompletion, and self-documenting code.

YAML Structure Examples:

db-shared pattern (asma):
```yaml
asma1:                              # ServerGroupName as root key
  pattern: db-shared
  server:
    type: postgres
    host: ${POSTGRES_SOURCE_HOST}
  environment_aware: true
  services:
    activities:                     # ServiceName as key
      schemas: [public]
      dev:                          # EnvironmentConfig
        database: activities_db_dev
        table_count: 40
      stage:
        database: activities_db_stage
        table_count: 42
```

db-per-tenant pattern (adopus):
```yaml
adopus:                             # ServerGroupName as root key
  pattern: db-per-tenant
  server:
    type: mssql
    host: ${MSSQL_SOURCE_HOST}
  services:
    adopus:                         # ServiceName as key (same as server group)
      schemas: [dbo]
      databases:                    # List of tenant databases
        - name: tenant1_db
          table_count: 100
        - name: tenant2_db
          table_count: 95
```

Key differences:
- db-shared: service contains environment keys (dev, stage, prod) with database per env
- db-per-tenant: service contains databases list (all tenants share same service)
"""
from typing import Dict, List, Union, Literal, TypedDict, TypeAlias


class ServerConfig(TypedDict, total=False):
    """Database server connection configuration.
    
    All values support environment variable placeholders: ${VAR_NAME}
    """
    type: Literal['postgres', 'mssql']
    host: str
    port: Union[str, int]
    user: str
    password: str


class EnvironmentConfig(TypedDict, total=False):
    """Per-environment database configuration within a service.
    
    Used for db-shared pattern where each environment has its own database.
    """
    database: Union[str, List[str]]  # Single db or list if multiple per env
    table_count: int


class TenantDatabaseConfig(TypedDict, total=False):
    """Tenant database configuration for db-per-tenant pattern.
    
    Each tenant has their own database with the same schema structure.
    """
    name: str
    table_count: int


class ServiceConfig(TypedDict, total=False):
    """Service configuration - unified structure for both patterns.
    
    For db-shared pattern:
      - schemas: List of schema names
      - Dynamic environment keys (dev, stage, prod): EnvironmentConfig
      
    For db-per-tenant pattern:
      - schemas: List of schema names
      - databases: List of TenantDatabaseConfig
    """
    schemas: List[str]
    # For db-per-tenant: list of tenant databases
    databases: List[TenantDatabaseConfig]
    # For db-shared: dynamic env keys (dev, stage, test, prod)
    # Access via: cast(EnvironmentConfig, service.get('dev'))


class ServerGroupConfig(TypedDict, total=False):
    """Complete server group configuration.
    
    This represents the value under the server group name key in YAML.
    The 'name' field is injected at runtime by get_single_server_group().
    
    Both patterns now use 'services' dict with service name as root key:
    - db-shared: services contain environment configs (dev/stage/prod databases)
    - db-per-tenant: services contain databases list (tenant databases)
    """
    # Runtime-injected field (not in YAML)
    name: str
    
    # Core configuration
    pattern: Literal['db-shared', 'db-per-tenant']
    description: str
    server: ServerConfig
    
    # Feature flags
    environment_aware: bool
    
    # Filtering
    include_pattern: str  # Regex to filter databases
    database_ref: str     # Reference database for schema discovery
    database_exclude_patterns: List[str]
    schema_exclude_patterns: List[str]
    
    # Unified data storage - service name as root key for both patterns
    services: Dict[str, ServiceConfig]


# Type alias for the full configuration file
# Key is server group name (e.g., 'asma1', 'adopus')
ServerGroupFile: TypeAlias = Dict[str, ServerGroupConfig]


class DatabaseInfo(TypedDict):
    """Information about a discovered database.
    
    Returned by list_mssql_databases() and list_postgres_databases().
    """
    name: str
    service: str
    environment: str
    customer: str
    schemas: List[str]
    table_count: int

class ExtractedIdentifiers(TypedDict):
    """Identifiers extracted from database name.
    
    Returned by extract_identifiers().
    """
    customer: str
    service: str
    env: str
    suffix: str