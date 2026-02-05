"""Type definitions for server group configuration.

These TypedDict classes define the structure of server_group.yaml,
providing type safety, IDE autocompletion, and self-documenting code.

YAML Structure Examples:

db-shared pattern (multi-server):
```yaml
asma:
  pattern: db-shared
  type: postgres                    # Database type (enforced for all servers)
  kafka_topology: shared            # "shared" | "per-server"
  environment_aware: true
  
  servers:
    default:
      host: ${POSTGRES_SOURCE_HOST}
      port: ${POSTGRES_SOURCE_PORT}
      user: ${POSTGRES_SOURCE_USER}
      password: ${POSTGRES_SOURCE_PASSWORD}
      kafka_bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
    prod:
      host: ${POSTGRES_SOURCE_HOST_PROD}
      port: ${POSTGRES_SOURCE_PORT_PROD}
      user: ${POSTGRES_SOURCE_USER_PROD}
      password: ${POSTGRES_SOURCE_PASSWORD_PROD}
      kafka_bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
  
  sources:
    activities:
      schemas:
        - public
      dev:
        server: default
        database: activities_db_dev
        table_count: 40
      prod:
        server: prod
        database: activities_db_prod
        table_count: 40
```

db-per-tenant pattern (multi-server):
```yaml
adopus:
  pattern: db-per-tenant
  type: mssql                       # Database type (enforced for all servers)
  kafka_topology: shared
  extraction_pattern: '^AdOpus(?P<customer>.+)$'
  database_ref: AdOpusTest          # Reference database for schema discovery
  
  servers:
    default:
      host: ${MSSQL_SOURCE_HOST}
      port: ${MSSQL_SOURCE_PORT}
      user: ${MSSQL_SOURCE_USER}
      password: ${MSSQL_SOURCE_PASSWORD}
      kafka_bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
    europe:
      host: ${MSSQL_SOURCE_HOST_EUROPE}
      port: ${MSSQL_SOURCE_PORT_EUROPE}
      user: ${MSSQL_SOURCE_USER_EUROPE}
      password: ${MSSQL_SOURCE_PASSWORD_EUROPE}
      kafka_bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS_EUROPE}
  
  sources:
    AVProd:                         # Customer name (for db-per-tenant)
      schemas:
        - dbo
      default:
        server: default
        database: AdOpusAVProd
        table_count: 150
    EuropeClient:
      schemas:
        - dbo
      default:
        server: europe
        database: AdOpusEuropeClient
        table_count: 200
```
"""
from typing import Dict, List, Union, Literal, TypedDict, TypeAlias


class ServerConfig(TypedDict, total=False):
    """Database server connection configuration.
    
    Note: 'type' is NOT here - it's at the server group level to enforce
    all servers have the same database type.
    
    All values support environment variable placeholders: ${VAR_NAME}
    
    The kafka_bootstrap_servers value depends on kafka_topology:
    - shared: All servers use same value (e.g., ${KAFKA_BOOTSTRAP_SERVERS})
    - per-server: Each server has postfixed value (e.g., ${KAFKA_BOOTSTRAP_SERVERS_EUROPE})
    
    extraction_pattern: Regex to extract identifiers from database names.
    Different servers may have different naming conventions.
    """
    host: str
    port: Union[str, int]
    user: str
    password: str
    kafka_bootstrap_servers: str
    extraction_pattern: str


class DatabaseEntry(TypedDict, total=False):
    """Single database entry within a source.
    
    Used in: sources.{source_name}.{environment_key}
    
    The 'server' field references a server name from the 'servers' dict.
    This allows different environments/databases to be on different servers.
    """
    server: str         # References servers.{name} (default: "default")
    database: str       # Database name
    table_count: int    # Number of tables discovered


class SourceConfig(TypedDict, total=False):
    """Source configuration - unified structure for both patterns.
    
    For db-shared pattern:
      - Source name = service name (activities, directory, chat)
      - Environment keys = dev, stage, prod, test, etc.
      
    For db-per-tenant pattern:
      - Source name = customer name (AVProd, Contoso, Brukerforum)
      - Environment keys = "default" or specific environments if customer has multiple
    
    Structure:
      sources:
        {source_name}:
          schemas: [schema1, schema2]
          {env_key}:              # dev, stage, prod, default, etc.
            server: {server_name}
            database: {db_name}
            table_count: {count}
    """
    schemas: List[str]
    # Dynamic environment keys - common ones typed for autocompletion
    dev: DatabaseEntry
    stage: DatabaseEntry
    test: DatabaseEntry
    prod: DatabaseEntry
    default: DatabaseEntry
    # Additional environment keys accessed via: cast(DatabaseEntry, source.get('custom_env'))


class ServerGroupConfig(TypedDict, total=False):
    """Complete server group configuration.
    
    This represents the value under the server group name key in YAML.
    The 'name' field is injected at runtime by get_single_server_group().
    
    Multi-server support:
    - type: Database type enforced for ALL servers (postgres | mssql)
    - servers: Dict of named server configurations
    - kafka_topology: "shared" (same Kafka for all) or "per-server" (isolated Kafka)
    - sources.{name}.{env}.server: References which server a database is on
    """
    # Runtime-injected field (not in YAML)
    name: str
    
    # Core configuration
    pattern: Literal['db-shared', 'db-per-tenant']
    type: Literal['postgres', 'mssql']  # Database type (enforced for all servers)
    description: str
    
    # Multi-server configuration
    servers: Dict[str, ServerConfig]
    kafka_topology: Literal['shared', 'per-server']
    
    # Feature flags
    environment_aware: bool
    
    # Filtering and extraction
    include_pattern: str                    # Regex to filter databases
    extraction_pattern: str                 # DEPRECATED: Global pattern, use servers.{name}.extraction_pattern instead
    database_ref: str                       # Reference database for schema discovery (db-per-tenant)
    database_exclude_patterns: List[str]
    schema_exclude_patterns: List[str]
    env_mappings: Dict[str, str]           # e.g., {"production": "prod", "staging": "stage"}
    
    # Unified source storage
    # For db-shared: source name = service name
    # For db-per-tenant: source name = customer name
    sources: Dict[str, SourceConfig]


# Type alias for the full configuration file
# Key is server group name (e.g., 'asma', 'adopus')
ServerGroupFile: TypeAlias = Dict[str, ServerGroupConfig]


class DatabaseInfo(TypedDict):
    """Information about a discovered database.
    
    Returned by list_mssql_databases() and list_postgres_databases().
    Includes server information for multi-server support.
    """
    name: str
    server: str         # Which server this database was found on
    service: str        # Inferred service/source name
    environment: str    # Inferred environment (dev, stage, prod, default)
    customer: str       # For db-per-tenant: customer name
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