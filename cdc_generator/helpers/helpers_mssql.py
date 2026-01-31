"""
MSSQL Helper Functions

Shared utilities for MSSQL operations:
- Database connections
- Table definition loading
- Test data generation
- INSERT statement building
"""

import os
import yaml
from pathlib import Path


def get_mssql_connection(env='nonprod', database=None):
    """
    Create connection to MSSQL database.
    
    Args:
        env: Environment name (local, nonprod, prod)
        database: Optional database name override
        
    Returns:
        Tuple of (connection, database_name)
    """
    import pymssql
    
    env_upper = env.upper()
    
    # Try environment-specific variables first, then fall back to generic
    host = os.getenv(f'MSSQL_{env_upper}_HOST', os.getenv('MSSQL_HOST'))
    port = os.getenv(f'MSSQL_{env_upper}_PORT', os.getenv('MSSQL_PORT', '1433'))
    db_name = database or os.getenv(f'MSSQL_{env_upper}_DATABASE', os.getenv('MSSQL_DATABASE', 'AdOpusTest'))
    user = os.getenv(f'MSSQL_{env_upper}_USER', os.getenv('MSSQL_USER'))
    password = os.getenv(f'MSSQL_{env_upper}_PASSWORD', os.getenv('MSSQL_PASSWORD'))
    
    if not host:
        raise ValueError(f"MSSQL_{env_upper}_HOST or MSSQL_HOST environment variable not set")
    
    conn = pymssql.connect(
        server=host,
        port=int(port),
        database=db_name,
        user=user,
        password=password
    )
    
    return conn, db_name


def load_table_definition(table_name):
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


def get_pk_columns(table_def):
    """Extract primary key columns from table definition"""
    pk_cols = []
    
    # Check for top-level primary_key field (adopus-db-schema format)
    if 'primary_key' in table_def:
        pk = table_def['primary_key']
        if isinstance(pk, list):
            pk_cols = pk
        elif isinstance(pk, str):
            pk_cols = [pk]
    
    # Check for fields array with primaryKey property (generated format)
    if not pk_cols:
        for field in table_def.get('fields', []):
            if field.get('primaryKey'):
                pk_cols.append(field.get('mssql'))
    
    return pk_cols


def get_insert_columns(table_def, pk_cols):
    """
    Build list of columns for INSERT statement.
    Excludes IDENTITY columns, includes required nullable=false columns.
    Handles both adopus-db-schema format (columns) and generated format (fields).
    """
    first_pk_field = None
    fields_list = table_def.get('fields', table_def.get('columns', []))
    
    if pk_cols:
        # Find first PK field - check both 'mssql' (generated) and 'name' (source) formats
        for field in fields_list:
            field_name = field.get('mssql', field.get('name'))
            if field_name == pk_cols[0]:
                first_pk_field = field
                break
    
    first_pk_is_identity = first_pk_field.get('identity', False) if first_pk_field else False
    
    insert_cols = []
    for field in fields_list:
        col_name = field.get('mssql', field.get('name'))  # Support both formats
        is_nullable = field.get('nullable', True)
        
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


def get_value_for_type(field_type, col_name, prefix='CDCTest'):
    """Generate appropriate test value for given SQL field type"""
    field_type = field_type.upper()
    
    if 'VARCHAR' in field_type or 'NVARCHAR' in field_type or 'CHAR' in field_type:
        return f"'{prefix}_{col_name}'"
    elif 'INT' in field_type or 'NUMERIC' in field_type or 'DECIMAL' in field_type:
        return '1'
    elif 'BIT' in field_type:
        return '0'
    elif 'DATETIME' in field_type or 'DATE' in field_type:
        return 'GETDATE()'
    else:
        return 'NULL'


def generate_insert_values(table_def, pk_cols, insert_cols, first_pk_is_identity, test_id_base, num_records, prefix='CDCTest'):
    """
    Generate value sets for INSERT statements.
    Handles both adopus-db-schema format (columns) and generated format (fields).
    
    Returns:
        all_value_sets: List of value tuples as strings
        first_pk_val: First PK value (for non-IDENTITY PKs)
        last_pk_val: Last PK value (for non-IDENTITY PKs)
    """
    all_value_sets = []
    first_pk_val = None
    last_pk_val = None
    
    fields_list = table_def.get('fields', table_def.get('columns', []))
    
    for record_num in range(num_records):
        test_id = test_id_base + record_num
        record_vals = []
        
        if first_pk_is_identity:
            # Generate values for non-PK columns only
            for col_name in insert_cols:
                for field in fields_list:
                    field_name = field.get('mssql', field.get('name'))
                    if field_name == col_name:
                        field_type = field.get('type', '')
                        record_vals.append(get_value_for_type(field_type, col_name, prefix))
                        break
        else:
            # Generate values including PK columns
            for col_name in insert_cols:
                if col_name in pk_cols:
                    # PK value
                    pk_idx = pk_cols.index(col_name)
                    for field in fields_list:
                        field_name = field.get('mssql', field.get('name'))
                        if field_name == col_name:
                            field_type = field.get('type', '')
                            if 'VARCHAR' in field_type.upper() or 'NVARCHAR' in field_type.upper():
                                val = f"'{prefix}_{record_num}_{pk_idx}'" if pk_idx > 0 else f"'{prefix}_{record_num}'"
                                record_vals.append(val)
                            else:
                                val = test_id + pk_idx if pk_idx > 0 else test_id
                                record_vals.append(str(val))
                                if pk_idx == 0:
                                    if record_num == 0:
                                        first_pk_val = val
                                    if record_num == num_records - 1:
                                        last_pk_val = val
                            break
                else:
                    # Non-PK value
                    for field in fields_list:
                        field_name = field.get('mssql', field.get('name'))
                        if field_name == col_name:
                            field_type = field.get('type', '')
                            record_vals.append(get_value_for_type(field_type, col_name, prefix))
                            break
        
        all_value_sets.append(f"({', '.join(record_vals)})")
    
    return all_value_sets, first_pk_val, last_pk_val


def build_batch_insert_sql(table_name, insert_cols, value_sets, first_pk_is_identity, pk_value=None, is_last_batch=False, is_first_batch=False):
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
        else:
            return f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES {values_clause};"
    else:
        if is_last_batch and pk_value is not None:
            return f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES {values_clause}; SELECT {pk_value} as pk_val;"
        else:
            return f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES {values_clause};"


def discover_cdc_tables():
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
    
    tables = []
    for yaml_file in schema_dir.glob('*.yaml'):
        table_name = yaml_file.stem
        tables.append(table_name)
    
    return sorted(tables)
