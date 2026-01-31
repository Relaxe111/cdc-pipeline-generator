"""
Helper functions for SQL generation - optimized using sql_insert and sql_raw.

This module provides functions to generate Redpanda Connect output configurations
that efficiently handle CDC operations (INSERT, UPDATE, DELETE) using:
- sql_raw for DELETE operations
- sql_insert with ON CONFLICT for INSERT/UPDATE operations
"""


def bloblang_field(field_name: str) -> str:
    """Format a field name for use in Bloblang expressions.
    
    Field names with non-ASCII characters need to be quoted.
    """
    # Check if field contains non-ASCII characters
    if not field_name.isascii():
        # Use quoted field reference for special characters
        return f'this."{field_name}"'
    return f'this.{field_name}'


def map_pg_type(pg_type: str) -> str:
    """Map PostgreSQL type to proper array type."""
    pg_type_lower = pg_type.lower().strip()
    
    if 'int' in pg_type_lower or 'serial' in pg_type_lower:
        if 'bigint' in pg_type_lower or 'bigserial' in pg_type_lower:
            return 'bigint'
        elif 'smallint' in pg_type_lower or 'smallserial' in pg_type_lower:
            return 'smallint'
        else:
            return 'integer'
    elif 'numeric' in pg_type_lower or 'decimal' in pg_type_lower:
        return 'numeric'
    elif 'float' in pg_type_lower or 'double' in pg_type_lower or 'real' in pg_type_lower:
        return 'double precision'
    elif 'bool' in pg_type_lower:
        return 'boolean'
    elif 'timestamp' in pg_type_lower:
        if 'timestamptz' in pg_type_lower or 'with time zone' in pg_type_lower:
            return 'timestamptz'
        else:
            return 'timestamp'
    elif 'date' in pg_type_lower:
        return 'date'
    elif 'time' in pg_type_lower:
        return 'time'
    elif 'json' in pg_type_lower:
        return 'jsonb' if 'jsonb' in pg_type_lower else 'json'
    elif 'uuid' in pg_type_lower:
        return 'uuid'
    else:
        return 'text'


def normalize_table_name(name: str) -> str:
    """Normalize Norwegian special characters only (keep original casing and structure)
    
    Replaces:
    - å/Å -> a/A
    - ø/Ø -> o/O  
    - æ/Æ -> ae/AE
    """
    replacements = {
        'å': 'a', 'Å': 'A',
        'ø': 'o', 'Ø': 'O',
        'æ': 'ae', 'Æ': 'AE'
    }
    
    result = name
    for norwegian_char, replacement in replacements.items():
        result = result.replace(norwegian_char, replacement)
    
    return result


def build_delete_case(table_name: str, schema: str, postgres_url: str,
                      pk_fields: list[str], mssql_fields: list[str], postgres_fields: list[str]) -> str:
    """
    Generate optimized DELETE case using sql_raw processor.
    
    Handles both single and composite primary keys efficiently.
    
    Args:
        table_name: Source table name (e.g., "Actor")
        schema: Target PostgreSQL schema (e.g., "avansas")
        postgres_url: PostgreSQL connection URL placeholder
        pk_fields: List of primary key field names in PostgreSQL (e.g., ["actno"] or ["soknad_id", "bruker_navn"])
        mssql_fields: List of all MSSQL field names for mapping
        postgres_fields: List of all PostgreSQL field names
    
    Returns:
        YAML configuration string for DELETE case
    """
    table_normalized = normalize_table_name(table_name)
    
    # Build WHERE clause for composite or single primary keys
    if len(pk_fields) == 1:
        # Single primary key
        pk_pg = pk_fields[0]
        # Find corresponding MSSQL field
        try:
            pk_idx = postgres_fields.index(pk_pg)
            pk_mssql = mssql_fields[pk_idx]
        except (ValueError, IndexError):
            # Fallback: assume same name
            pk_mssql = pk_pg
        
        where_clause = f'"{pk_pg}" = $1'
        args_mapping = f'this.{pk_mssql}'
    else:
        # Composite primary key
        where_parts: list[str] = []
        args_list: list[str] = []
        for i, pk_pg in enumerate(pk_fields, start=1):
            try:
                pk_idx = postgres_fields.index(pk_pg)
                pk_mssql = mssql_fields[pk_idx]
            except (ValueError, IndexError):
                pk_mssql = pk_pg
            
            where_parts.append(f'"{pk_pg}" = ${i}')
            args_list.append(bloblang_field(pk_mssql))
        
        where_clause = " AND ".join(where_parts)
        args_mapping = ", ".join(args_list)
    
    return f"""# DELETE for {table_name}
- check: 'this.__routing_table == "{table_name}" && this.__cdc_operation == "DELETE"'
  output:
    sql_raw:
      driver: postgres
      dsn: "{postgres_url}"
      query: 'DELETE FROM {schema}."{table_normalized}" WHERE {where_clause}'
      args_mapping: |
        root = [ {args_mapping} ]
      conn_max_open: 10
"""


def build_upsert_case(table_name: str, schema: str, postgres_url: str,
                      postgres_fields: list[str], mssql_fields: list[str], pk_fields: list[str]) -> str:
    """
    Generate optimized INSERT/UPDATE case using sql_insert with ON CONFLICT.
    
    Handles both single and composite primary keys with proper UPSERT semantics.
    
    Args:
        table_name: Source table name (e.g., "Actor")
        schema: Target PostgreSQL schema (e.g., "avansas")
        postgres_url: PostgreSQL connection URL placeholder
        postgres_fields: List of PostgreSQL column names (e.g., ["actno", "name", "email"])
        mssql_fields: List of MSSQL field names (e.g., ["actno", "Name", "Email"])
        pk_fields: List of primary key field names in PostgreSQL
    
    Returns:
        YAML configuration string for INSERT/UPDATE case
    """
    table_normalized = normalize_table_name(table_name)
    
    # Build columns list (business fields + metadata fields)
    all_columns = list(postgres_fields) + [
        '__sync_timestamp',
        '__source',
        '__source_db',
        '__source_table',
        '__source_ts_ms',
        '__cdc_operation'
    ]
    
    # Build args_mapping (map MSSQL fields to PostgreSQL + add metadata)
    args_list: list[str] = []
    for mssql_field in mssql_fields:
        args_list.append(bloblang_field(mssql_field))
    
    # Add metadata values
    args_list.extend([
        'this.__sync_timestamp',
        '"kafka-cdc"',
        'this.__source_db',
        'this.__source_table',
        'this.__source_ts_ms',
        'this.__cdc_operation'
    ])
    
    # Build columns YAML (indented list with SQL quotes for PostgreSQL case sensitivity)
    # Use single-quoted YAML strings containing double-quoted SQL identifiers
    columns_yaml = "\n".join([f'        - \'"{col}"\'' for col in all_columns])
    
    # Build args_mapping YAML
    args_yaml = ", ".join(args_list)
    
    # Build ON CONFLICT clause with quoted column names for case sensitivity
    if len(pk_fields) == 1:
        conflict_target = f'("{pk_fields[0]}")'
    else:
        quoted_pks = [f'"{pk}"' for pk in pk_fields]
        conflict_target = f"({', '.join(quoted_pks)})"
    
    # Build UPDATE SET clause with quoted column names (all non-PK fields + metadata fields)
    # Don't update the PK fields themselves in the SET clause
    update_fields = [f for f in postgres_fields if f not in pk_fields]
    update_parts = [f'"{f}" = EXCLUDED."{f}"' for f in update_fields]
    update_parts.extend([
        '"__sync_timestamp" = EXCLUDED."__sync_timestamp"',
        '"__source" = EXCLUDED."__source"',
        '"__source_db" = EXCLUDED."__source_db"',
        '"__source_table" = EXCLUDED."__source_table"',
        '"__source_ts_ms" = EXCLUDED."__source_ts_ms"',
        '"__cdc_operation" = EXCLUDED."__cdc_operation"'
    ])
    update_set = ", ".join(update_parts)
    
    suffix = f"ON CONFLICT {conflict_target} DO UPDATE SET {update_set}"
    
    return f"""# INSERT/UPDATE for {table_name}
- check: 'this.__routing_table == "{table_name}" && (this.__cdc_operation == "INSERT" || this.__cdc_operation == "UPDATE")'
  output:
    sql_insert:
      driver: postgres
      dsn: "{postgres_url}"
      table: '{schema}."{table_normalized}"'
      columns:
{columns_yaml}
      args_mapping: |
        root = [ {args_yaml} ]
      suffix: '{suffix}'
      conn_max_open: 10
"""


# Backward compatibility - keep old function names as aliases
def build_batch_delete_case(table_name: str, schema: str, postgres_url: str,
                            pk_fields: list[str], pk_mssql: str, pk_type: str) -> str:
    """
    Legacy function - redirects to build_delete_case.
    This is kept for backward compatibility with existing code.
    """
    # Convert single pk_mssql to list format for new function
    return build_delete_case(
        table_name, schema, postgres_url,
        pk_fields, [pk_mssql], pk_fields
    )


def build_batch_upsert_case(table_name: str, schema: str, postgres_url: str,
                           postgres_fields_with_meta: list[str], postgres_types_with_meta: list[str],
                           mssql_fields: list[str], pk_fields: list[str], pk_constraint: str) -> str:
    """
    Legacy function - redirects to build_upsert_case.
    This is kept for backward compatibility with existing code.
    """
    # Extract business fields (exclude metadata)
    postgres_fields = postgres_fields_with_meta[:len(mssql_fields)]
    
    return build_upsert_case(
        table_name, schema, postgres_url,
        postgres_fields, mssql_fields, pk_fields
    )


def build_staging_case(table_name: str, schema: str, postgres_url: str,
                       postgres_fields: list[str], mssql_fields: list[str]) -> str:
    """
    Generate staging table INSERT case using sql_insert with batching.
    
    Writes all records (INSERT, UPDATE, DELETE) to staging table for later
    merge processing by stored procedure.
    
    Args:
        table_name: Source table name (e.g., "Actor")
        schema: Target PostgreSQL schema (e.g., "avansas")
        postgres_url: PostgreSQL connection URL placeholder
        postgres_fields: List of PostgreSQL column names
        mssql_fields: List of MSSQL field names
    
    Returns:
        YAML configuration string for staging table INSERT case
    """
    table_normalized = normalize_table_name(table_name)
    stg_table = f"stg_{table_normalized}"
    
    # Build columns list (business fields + all metadata fields including kafka tracking)
    all_columns = list(postgres_fields) + [
        '__sync_timestamp',
        '__source',
        '__source_db',
        '__source_table',
        '__source_ts_ms',
        '__cdc_operation',
        '__kafka_offset',
        '__kafka_partition',
        '__kafka_timestamp'
    ]
    
    # Build args_mapping (map MSSQL fields to PostgreSQL + add metadata)
    args_list: list[str] = []
    for mssql_field in mssql_fields:
        args_list.append(bloblang_field(mssql_field))
    
    # Add metadata values
    args_list.extend([
        'this.__sync_timestamp',
        'this.__source',
        'this.__source_db',
        'this.__source_table',
        'this.__source_ts_ms',
        'this.__cdc_operation',
        'this.__kafka_offset',
        'this.__kafka_partition',
        'this.__kafka_timestamp'
    ])
    
    # Build columns YAML (indented list with SQL quotes for PostgreSQL case sensitivity)
    columns_yaml = "\n".join([f'        - \'"{col}"\'' for col in all_columns])
    
    # Build args_mapping YAML
    args_yaml = ", ".join(args_list)
    
    # Check BOTH schema and table for consolidated routing
    return f"""# Staging INSERT for {schema}.{table_name}
- check: 'this.__routing_schema == "{schema}" && this.__routing_table == "{table_name}"'
  output:
    sql_insert:
      driver: postgres
      dsn: "{postgres_url}"
      table: '{schema}."{stg_table}"'
      columns:
{columns_yaml}
      args_mapping: |
        root = [ {args_yaml} ]
      batching:
        count: 100
        period: 5s
"""
