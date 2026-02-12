"""Type definitions for server group configuration.

These TypedDict classes define the structure of source-groups.yaml,
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
from typing import Literal, TypeAlias, TypedDict


class ExtractionPattern(TypedDict, total=False):
    r"""Single extraction pattern for database name decomposition.

    Patterns are tried in order. First match wins.

    Fields:
        pattern: Regex with named groups (?P<service>...) and optionally (?P<env>...)
        env: Fixed environment name (overrides captured (?P<env>) group if present)
        strip_patterns: List of regex patterns to remove from service name
          (e.g., ['_db', '_database$'])
        env_mapping: Optional dict to transform extracted env
          (e.g., {'prod_adcuris': 'prod-adcuris'})
        description: Human-readable description of what this pattern matches

    Examples:
        # Match {service}_db_prod_adcuris -> service={service}, env=prod-adcuris (transformed)
        {
            'pattern': r'^(?P<service>\w+)_db_prod_adcuris$',
            'env': 'prod_adcuris',
            'strip_patterns': ['_db$'],  # Strip _db from end only
            'env_mapping': {'prod_adcuris': 'prod-adcuris'},  # Transform env
            'description': 'Service with _db suffix and prod_adcuris environment'
        }

        # Match adopus_db_{service}_prod_adcuris -> service=adopus_{service}, env=prod-adcuris
        {
            'pattern': r'^(?P<service>adopus_db_\w+)_prod_adcuris$',
            'env': 'prod_adcuris',
            'strip_patterns': ['_db'],  # Strip _db from anywhere
            'env_mapping': {'prod_adcuris': 'prod-adcuris'},
            'description': 'AdOpus service with _db infix and prod_adcuris environment'
        }

        # Match {service}_{env} -> service={service}, env={env}
        {
            'pattern': r'^(?P<service>\w+)_(?P<env>\w+)$',
            'description': 'Standard service_env pattern'
        }

        # Match {service} (single word) -> service={service}, env=prod (implicit)
        {
            'pattern': r'^(?P<service>\w+)$',
            'env': 'prod',
            'description': 'Single word service name (implicit prod environment)'
        }
    """
    pattern: str
    env: str
    strip_patterns: list[str]
    env_mapping: dict[str, str]
    description: str


class ServerConfig(TypedDict, total=False):
    """Database server connection configuration.

    Note: 'type' is NOT here - it's at the server group level to enforce
    all servers have the same database type.

    All values support environment variable placeholders: ${VAR_NAME}

    The kafka_bootstrap_servers value depends on kafka_topology:
    - shared: All servers use same value (e.g., ${KAFKA_BOOTSTRAP_SERVERS})
    - per-server: Each server has postfixed value (e.g., ${KAFKA_BOOTSTRAP_SERVERS_EUROPE})

    extraction_pattern: DEPRECATED - Single regex pattern. Use extraction_patterns instead.
    Different servers may have different naming conventions.

    extraction_patterns: List of extraction patterns tried in order. First match wins.
    Allows handling multiple database naming conventions per server.
    Each pattern can have its own regex, fixed env, and strip_suffixes.
    """
    host: str
    port: str | int
    user: str
    password: str
    kafka_bootstrap_servers: str
    extraction_pattern: str
    extraction_patterns: list[ExtractionPattern]
    environments: list[str]


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
    schemas: list[str]
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
    servers: dict[str, ServerConfig]
    kafka_topology: Literal['shared', 'per-server']

    # Feature flags
    environment_aware: bool

    # Filtering and extraction
    include_pattern: str                    # Regex to filter databases
    extraction_pattern: str                 # DEPRECATED: use servers.{name}.extraction_pattern
    database_ref: str                       # Reference DB for schema discovery (db-per-tenant)
    validation_env: str                     # Validation environment used for inspect/validation
    envs: list[str]                         # Discovered available environments
    database_exclude_patterns: list[str]
    schema_exclude_patterns: list[str]
    env_mappings: dict[str, str]           # e.g., {"production": "prod", "staging": "stage"}

    # Unified source storage
    # For db-shared: source name = service name
    # For db-per-tenant: source name = customer name
    sources: dict[str, SourceConfig]


# Type alias for the full configuration file
# Key is server group name (e.g., 'asma', 'adopus')
ServerGroupFile: TypeAlias = dict[str, ServerGroupConfig]


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
    schemas: list[str]
    table_count: int


class ExtractedIdentifiers(TypedDict):
    """Identifiers extracted from database name.

    Returned by extract_identifiers().
    """
    customer: str
    service: str
    env: str
    suffix: str
