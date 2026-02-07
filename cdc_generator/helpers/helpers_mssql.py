"""
MSSQL Helper Functions

Shared utilities for MSSQL operations:
- Database connections
- Table definition loading
- Test data generation
- INSERT statement building
"""

import os
from pathlib import Path
from typing import Any, cast

import yaml  # type: ignore[import-not-found]

# pymssql is optional - only required for MSSQL operations
try:
    import pymssql  # type: ignore[import-not-found]
    _has_pymssql = True
except ImportError:
    _has_pymssql = False
    pymssql = None  # type: ignore[assignment]


class MSSQLNotAvailableError(Exception):
    """Raised when pymssql is not installed but MSSQL operations are requested."""
    pass


def _ensure_pymssql() -> None:
    """Ensure pymssql is available, raise helpful error if not."""
    if not _has_pymssql:
        raise MSSQLNotAvailableError(
            "pymssql is not installed. Install it with: pip install pymssql\\n" +
            "Note: pymssql requires FreeTDS. On macOS: brew install freetds"
        )


def create_mssql_connection(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str
) -> Any:  # noqa: ANN401 - pymssql.Connection is untyped
    """
    Create a pymssql connection with proper typing.
    
    Args:
        host: Server hostname
        port: Server port
        database: Database name
        user: Username
        password: Password
        
    Returns:
        pymssql connection object
        
    Raises:
        MSSQLNotAvailableError: If pymssql is not installed
    """
    _ensure_pymssql()
    return cast(Any, pymssql.connect( # type: ignore[misc,union-attr]
        server=host,
        port=port,
        database=database,
        user=user,
        password=password
    ))


def get_mssql_connection(env: str = 'nonprod', database: str | None = None) -> tuple[Any, str]:
    """
    Create connection to MSSQL database.
    
    Args:
        env: Environment name (local, nonprod, prod)
        database: Optional database name override
        
    Returns:
        Tuple of (connection, database_name)
    """
    env_upper = env.upper()

    # Try environment-specific variables first, then fall back to generic
    host = os.getenv(f'MSSQL_{env_upper}_HOST', os.getenv('MSSQL_HOST'))
    port = os.getenv(f'MSSQL_{env_upper}_PORT', os.getenv('MSSQL_PORT', '1433'))
    db_name: str = database if database is not None else (
        os.getenv(f'MSSQL_{env_upper}_DATABASE') or
        os.getenv('MSSQL_DATABASE') or
        'AdOpusTest'
    )
    user = os.getenv(f'MSSQL_{env_upper}_USER', os.getenv('MSSQL_USER'))
    password = os.getenv(f'MSSQL_{env_upper}_PASSWORD', os.getenv('MSSQL_PASSWORD'))

    if not host:
        raise ValueError(f"MSSQL_{env_upper}_HOST or MSSQL_HOST environment variable not set")
    if not user:
        raise ValueError(f"MSSQL_{env_upper}_USER or MSSQL_USER environment variable not set")
    if not password:
        raise ValueError(f"MSSQL_{env_upper}_PASSWORD or MSSQL_PASSWORD environment variable not set")

    conn = create_mssql_connection(
        host=host,
        port=int(port),
        database=db_name,
        user=user,
        password=password
    )

    return conn, db_name


def load_table_definition(table_name: str) -> dict[str, Any] | None:
    """Load table definition from adopus-db-schema/*.yaml"""
    # Find adopus-db-schema directory
    current_dir = Path(__file__).parent
    while current_dir != current_dir.parent:
        schema_dir = current_dir / 'adopus-db-schema'
        if schema_dir.exists():
            break
        current_dir = current_dir.parent
    else:
        # Fallback to absolute path in workspace
        schema_dir = Path('/workspace/adopus-db-schema')

    yaml_file = schema_dir / f"{table_name}.yaml"
    if not yaml_file.exists():
        return None

    with open(yaml_file) as f:
        return yaml.safe_load(f)


def get_pk_columns(table_def: dict[str, Any]) -> list[str]:
    """Extract primary key columns from table definition"""
    pk_cols: list[str] = []

    # Check for top-level primary_key field (adopus-db-schema format)
    if 'primary_key' in table_def:
        pk: Any = table_def['primary_key']
        if isinstance(pk, list):
            # Cast to list of Any first, then convert to strings
            pk_list = cast(list[Any], pk)
            pk_cols = [str(item) for item in pk_list]
        elif isinstance(pk, str):
            pk_cols = [pk]

    # Check for fields array with primaryKey property (generated format)
    if not pk_cols:
        fields = table_def.get('fields', [])
        if isinstance(fields, list):
            fields_typed = cast(list[Any], fields)
            for field in fields_typed:
                if isinstance(field, dict):
                    field_dict = cast(dict[str, Any], field)
                    if field_dict.get('primaryKey'):
                        mssql_name: Any = field_dict.get('mssql')
                        if mssql_name is not None:
                            pk_cols.append(str(mssql_name))

    return pk_cols


def get_insert_columns(table_def: dict[str, Any], pk_cols: list[str]) -> tuple[list[str], bool]:
    """
    Build list of columns for INSERT statement.
    Excludes IDENTITY columns, includes required nullable=false columns.
    Handles both adopus-db-schema format (columns) and generated format (fields).
    """
    first_pk_field: dict[str, Any] | None = None
    raw_fields = table_def.get('fields') or table_def.get('columns', [])
    fields_list: list[dict[str, Any]] = cast(list[dict[str, Any]], raw_fields if isinstance(raw_fields, list) else [])

    if pk_cols:
        # Find first PK field - check both 'mssql' (generated) and 'name' (source) formats
        for field in fields_list:
            field_name: str = str(field.get('mssql') or field.get('name') or '')
            if field_name == pk_cols[0]:
                first_pk_field = field
                break

    first_pk_is_identity: bool = bool(first_pk_field.get('identity', False)) if first_pk_field else False

    insert_cols: list[str] = []
    for field in fields_list:
        col_name: str = str(field.get('mssql') or field.get('name') or '')
        is_nullable: bool = bool(field.get('nullable', True))

        # Skip IDENTITY columns
        if field.get('identity'):
            continue

        # Include PK columns if not IDENTITY
        if col_name in pk_cols:
            if not first_pk_is_identity:
                insert_cols.append(col_name)
        # Include non-nullable columns or timestamp columns
        elif not is_nullable or any(kw in col_name.lower() for kw in ['createdt', 'changedt', 'createuser', 'changeuser']):
            insert_cols.append(col_name)

    return insert_cols, first_pk_is_identity


def get_value_for_type(field_type: str, col_name: str, prefix: str = 'CDCTest') -> str:
    """Generate appropriate test value for given SQL field type"""
    field_type = field_type.upper()

    if 'VARCHAR' in field_type or 'NVARCHAR' in field_type or 'CHAR' in field_type:
        return f"'{prefix}_{col_name}'"
    if 'INT' in field_type or 'NUMERIC' in field_type or 'DECIMAL' in field_type:
        return '1'
    if 'BIT' in field_type:
        return '0'
    if 'DATETIME' in field_type or 'DATE' in field_type:
        return 'GETDATE()'
    return 'NULL'


def generate_insert_values(table_def: dict[str, Any], pk_cols: list[str], insert_cols: list[str],
                          first_pk_is_identity: bool, test_id_base: int, num_records: int,
                          prefix: str = 'CDCTest') -> tuple[list[str], int | None, int | None]:
    """
    Generate value sets for INSERT statements.
    Handles both adopus-db-schema format (columns) and generated format (fields).
    
    Returns:
        all_value_sets: List of value tuples as strings
        first_pk_val: First PK value (for non-IDENTITY PKs)
        last_pk_val: Last PK value (for non-IDENTITY PKs)
    """
    all_value_sets: list[str] = []
    first_pk_val: int | None = None
    last_pk_val: int | None = None

    raw_fields = table_def.get('fields') or table_def.get('columns', [])
    fields_list: list[dict[str, Any]] = cast(list[dict[str, Any]], raw_fields if isinstance(raw_fields, list) else [])

    for record_num in range(num_records):
        test_id: int = test_id_base + record_num
        record_vals: list[str] = []

        if first_pk_is_identity:
            # Generate values for non-PK columns only
            for col_name in insert_cols:
                for field in fields_list:
                    field_name: str = str(field.get('mssql') or field.get('name') or '')
                    if field_name == col_name:
                        field_type: str = str(field.get('type', ''))
                        record_vals.append(get_value_for_type(field_type, col_name, prefix))
                        break
        else:
            # Generate values including PK columns
            for col_name in insert_cols:
                if col_name in pk_cols:
                    # PK value
                    pk_idx: int = pk_cols.index(col_name)
                    for field in fields_list:
                        field_name = str(field.get('mssql') or field.get('name') or '')
                        if field_name == col_name:
                            field_type = str(field.get('type', ''))
                            if 'VARCHAR' in field_type.upper() or 'NVARCHAR' in field_type.upper():
                                val: str = f"'{prefix}_{record_num}_{pk_idx}'" if pk_idx > 0 else f"'{prefix}_{record_num}'"
                                record_vals.append(val)
                            else:
                                val_int: int = test_id + pk_idx if pk_idx > 0 else test_id
                                record_vals.append(str(val_int))
                                if pk_idx == 0:
                                    if record_num == 0:
                                        first_pk_val = val_int
                                    if record_num == num_records - 1:
                                        last_pk_val = val_int
                            break
                else:
                    # Non-PK value
                    for field in fields_list:
                        field_name = str(field.get('mssql') or field.get('name') or '')
                        if field_name == col_name:
                            field_type = str(field.get('type', ''))
                            record_vals.append(get_value_for_type(field_type, col_name, prefix))
                            break

        all_value_sets.append(f"({', '.join(record_vals)})")

    return all_value_sets, first_pk_val, last_pk_val


def build_batch_insert_sql(table_name: str, insert_cols: list[str], value_sets: list[str],
                          first_pk_is_identity: bool, pk_value: int | None = None,
                          is_last_batch: bool = False, is_first_batch: bool = False) -> str:
    """
    Build INSERT SQL statement with optional PK return.
    
    For IDENTITY PKs:
        - Last batch: Returns SCOPE_IDENTITY() as pk_val
        - Other batches: No return value
    
    For non-IDENTITY PKs:
        - Last batch: Returns provided pk_value as pk_val
        - Other batches: No return value
    """
    values_clause = ', '.join(value_sets)

    if first_pk_is_identity:
        if is_last_batch:
            return f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES {values_clause}; SELECT CAST(SCOPE_IDENTITY() AS INT) as pk_val;"
        return f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES {values_clause};"
    if is_last_batch and pk_value is not None:
        return f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES {values_clause}; SELECT {pk_value} as pk_val;"
    return f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES {values_clause};"


def discover_cdc_tables() -> list[str]:
    """Discover all tables with CDC definitions from adopus-db-schema/*.yaml"""
    # Find adopus-db-schema directory
    current_dir = Path(__file__).parent
    while current_dir != current_dir.parent:
        schema_dir = current_dir / 'adopus-db-schema'
        if schema_dir.exists():
            break
        current_dir = current_dir.parent
    else:
        # Fallback to absolute path in workspace
        schema_dir = Path('/workspace/adopus-db-schema')

    if not schema_dir.exists():
        return []

    tables: list[str] = []
    for yaml_file in schema_dir.glob('*.yaml'):
        table_name = yaml_file.stem
        tables.append(table_name)

    return sorted(tables)
