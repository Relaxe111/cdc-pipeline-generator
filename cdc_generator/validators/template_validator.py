"""Database-aware validation for column templates and transform rules.

Validates that templates and transforms reference actual database columns
and have compatible types.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

from cdc_generator.core.bloblang_refs import read_bloblang_ref
from cdc_generator.core.column_templates import get_template
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.validators.bloblang_parser import (
    extract_column_references,
    extract_root_assignments,
    is_static_expression,
    strip_bloblang_comments,
    uses_environment_variables,
)


@dataclass
class ValidationResult:
    """Result of validating a single template or transform."""

    item_key: str
    is_valid: bool
    errors: list[str]
    warnings: list[str]
    referenced_columns: set[str]
    env_vars: set[str]
    produced_columns: set[str] = field(default_factory=set)


@dataclass
class TableSchema:
    """Database table schema information."""

    table_name: str
    schema_name: str
    columns: dict[str, str]  # column_name -> data_type


# ---------------------------------------------------------------------------
# Source-ref validation helper
# ---------------------------------------------------------------------------


def _validate_value_source_ref(
    value: str,
    item_key: str,
    service: str,
) -> list[str]:
    """Validate source-group references in a template/transform value.

    If *value* is a ``{group.sources.*.key}`` reference, checks that:
    1. The reference parses correctly
    2. The group exists in source-groups.yaml
    3. The key is defined for all sources in the group

    Args:
        value: Template value (Bloblang expression or source-ref).
        item_key: Template/rule key (for error messages).
        service: Service name (unused here, kept for context).

    Returns:
        List of error strings (empty = valid).
    """
    _ = service  # kept for future use

    from cdc_generator.core.source_ref_resolver import (
        SourceRefError,
        is_source_ref,
        parse_source_ref,
        validate_all_sources_have_key,
    )

    if not is_source_ref(value):
        return []

    ref = parse_source_ref(value)
    if ref is None:
        return [
            f"'{item_key}' has invalid source-ref format: {value}. "
            + "Expected: {group.sources.*.key}"
        ]

    try:
        ref_errors = validate_all_sources_have_key(ref)
    except SourceRefError as exc:
        return [f"'{item_key}' source-ref validation failed: {exc.message}"]

    return [
        f"'{item_key}' source-ref {value}: {err}"
        for err in ref_errors
    ]


def _validate_bloblang_syntax(
    value: str,
    item_key: str,
) -> list[str]:
    """Validate Bloblang syntax via ``rpk connect lint``.

    Gracefully degrades when rpk is not installed — returns an empty list
    instead of errors so that environments without rpk still work.

    When the expression contains ``${ENV_VAR}`` references, temporary
    dummy values are set so that ``rpk connect lint`` can parse the
    expression without failing on missing environment variables.

    Args:
        value: Bloblang expression to validate.
        item_key: Template/rule key (for error messages).

    Returns:
        List of error strings (empty = valid or rpk unavailable).
    """
    import os

    from cdc_generator.validators.manage_service.bloblang_validator import (
        check_rpk_available,
        validate_bloblang_expression,
    )

    if not check_rpk_available():
        return []

    # Set dummy env vars so rpk doesn't fail on missing ${VAR} references
    normalized_value = strip_bloblang_comments(value)
    env_vars = uses_environment_variables(normalized_value)
    saved_env: dict[str, str | None] = {}
    for var in env_vars:
        saved_env[var] = os.environ.get(var)
        if var not in os.environ:
            os.environ[var] = "__LINT_PLACEHOLDER__"

    try:
        is_valid, error_msg = validate_bloblang_expression(
            normalized_value,
            item_key,
        )
    finally:
        # Restore original env state
        for var, original in saved_env.items():
            if original is None:
                os.environ.pop(var, None)
            else:
                os.environ[var] = original

    if is_valid:
        return []

    return [
        f"'{item_key}' has invalid Bloblang syntax: {error_msg}"
    ]


def get_source_table_schema(
    service: str,
    table_key: str,
    env: str = "nonprod",
    source_table_key: str | None = None,
) -> TableSchema | None:
    """Get schema for source table from saved schema YAML files.

    For sink tables that reference a source table via ``from``, pass
    the ``source_table_key`` so the lookup targets the *source* schema
    directory (e.g. ``public/customers.yaml``) rather than the
    non-existent sink schema directory (e.g.
    ``directory_replica/customers.yaml``).

    Args:
        service: Service name.
        table_key: Table key (e.g., "dbo.users" or "public.users").
            Used for display purposes when *source_table_key* is given.
        env: Environment (unused, kept for API compatibility).
        source_table_key: Optional override for the schema lookup.
            When a sink table has a ``from`` field (e.g.
            ``public.customers``), pass it here so the schema is
            resolved from the *source* table definition.

    Returns:
        TableSchema if successful, None otherwise.
    """
    from cdc_generator.core.structure_replicator import load_source_schema

    _ = env  # kept for API compatibility

    # Use source_table_key for schema lookup when provided (sink → source)
    lookup_key = source_table_key or table_key

    # Parse lookup key to extract schema.table
    if "." in lookup_key:
        schema_name, table_name = lookup_key.split(".", 1)
    else:
        schema_name = "dbo"  # Default for MSSQL
        table_name = lookup_key

    try:
        schema_data = load_source_schema(service, schema_name, table_name)
        if not schema_data:
            if source_table_key:
                print_warning(
                    f"No schema file found for source table '{source_table_key}'"
                    + f" (referenced by sink table '{table_key}')"
                )
            else:
                print_warning(f"No schema file found for {table_key}")
            return None

        columns_raw = schema_data.get("columns")
        if not columns_raw or not isinstance(columns_raw, list):
            print_warning(f"No columns found for {table_key}")
            return None

        columns = cast(list[dict[str, str]], columns_raw)
        column_map: dict[str, str] = {
            str(col.get("name", "")): str(col.get("type", ""))
            for col in columns
        }

        return TableSchema(
            table_name=table_name,
            schema_name=schema_name,
            columns=column_map,
        )

    except Exception as e:
        print_error(f"Failed to get table schema: {e}")
        return None


def get_sink_table_schema(
    sink_service: str,
    table_key: str,
) -> TableSchema | None:
    """Load schema for a sink table from service-schemas/{sink_service}/.

    For ``target_exists=true`` tables, the sink database already has the
    table.  Its schema is stored under the *target service* directory
    inside ``service-schemas/``.

    Args:
        sink_service: Target service name extracted from the sink key
            (e.g. ``"proxy"`` from sink key ``"sink_asma.proxy"``).
        table_key: Sink table key (e.g. ``"public.directory_user_name"``).

    Returns:
        TableSchema if found, None otherwise.
    """
    from cdc_generator.core.structure_replicator import load_source_schema

    if "." in table_key:
        schema_name, table_name = table_key.split(".", 1)
    else:
        schema_name = "public"
        table_name = table_key

    try:
        schema_data = load_source_schema(
            sink_service, schema_name, table_name,
        )
        if not schema_data:
            return None

        columns_raw = schema_data.get("columns")
        if not columns_raw or not isinstance(columns_raw, list):
            return None

        columns = cast(list[dict[str, str]], columns_raw)
        column_map: dict[str, str] = {
            str(col.get("name", "")): str(col.get("type", ""))
            for col in columns
        }

        return TableSchema(
            table_name=table_name,
            schema_name=schema_name,
            columns=column_map,
        )

    except Exception:
        return None


def validate_sink_column_exists(
    sink_service: str,
    table_key: str,
    column_name: str,
) -> tuple[bool, list[str], list[str]]:
    """Check that *column_name* exists in the sink table's schema.

    Used when ``target_exists=true`` to verify ``--column-name`` maps
    to an actual column on the sink table.

    Args:
        sink_service: Target service from the sink key (e.g. ``"proxy"``).
        table_key: Sink table key (e.g. ``"public.directory_user_name"``).
        column_name: Column name to check.

    Returns:
        Tuple of ``(is_valid, errors, warnings)``.
    """
    errors: list[str] = []
    warnings: list[str] = []

    sink_schema = get_sink_table_schema(sink_service, table_key)
    if sink_schema is None:
        warnings.append(
            f"Cannot verify column '{column_name}' — "
            + f"no schema file found for sink table '{table_key}' "
            + f"in service '{sink_service}'"
        )
        return True, errors, warnings  # Warn but don't fail

    if column_name not in sink_schema.columns:
        available = ", ".join(sorted(sink_schema.columns.keys()))
        errors.append(
            f"Column '{column_name}' not found on sink table '{table_key}'.\n"
            + f"  Available columns: {available}"
        )
        return False, errors, warnings

    return True, errors, warnings


def validate_column_mapping_types(
    source_schema: TableSchema,
    sink_schema: TableSchema,
    columns_mapping: dict[str, str],
    table_key: str,
) -> list[str]:
    """Check source↔sink column type compatibility for explicit mappings.

    For ``target_exists=true`` tables with an explicit ``columns``
    mapping (``sink_col: source_col``), validates that:
    1. Each source column exists in the source schema
    2. Each sink column exists in the sink schema
    3. Types are compatible (warns on mismatches)

    Args:
        source_schema: Source table schema.
        sink_schema: Sink table schema.
        columns_mapping: ``{sink_col: source_col}`` from service YAML.
        table_key: Table key for error messages.

    Returns:
        List of warning strings (type mismatches are warnings, not errors).
    """
    warnings: list[str] = []

    for sink_col, source_col in columns_mapping.items():
        # Check source column exists
        if source_col not in source_schema.columns:
            warnings.append(
                f"Column mapping '{sink_col}: {source_col}' — "
                + f"source column '{source_col}' not found in "
                + f"source table '{source_schema.schema_name}.{source_schema.table_name}'"
            )
            continue

        # Check sink column exists
        if sink_col not in sink_schema.columns:
            warnings.append(
                f"Column mapping '{sink_col}: {source_col}' — "
                + f"sink column '{sink_col}' not found in "
                + f"sink table '{table_key}'"
            )
            continue

        # Compare types
        source_type = source_schema.columns[source_col].lower()
        sink_type = sink_schema.columns[sink_col].lower()
        if not _types_are_compatible(source_type, sink_type):
            warnings.append(
                f"Column mapping '{sink_col}: {source_col}' — "
                + f"type mismatch: source='{source_type}', sink='{sink_type}'"
            )

    return warnings


# Known type compatibility groups — types within the same group are
# considered compatible.
_TYPE_COMPAT_GROUPS: list[set[str]] = [
    {"text", "varchar", "character varying", "nvarchar", "ntext", "char", "nchar"},
    {"int", "integer", "int4", "smallint", "int2", "bigint", "int8"},
    {"float", "double precision", "float8", "real", "float4", "numeric", "decimal"},
    {"bool", "boolean", "bit"},
    {"uuid", "uniqueidentifier"},
    {"timestamp", "timestamp without time zone", "datetime", "datetime2", "smalldatetime"},
    {"timestamptz", "timestamp with time zone", "datetimeoffset"},
    {"date"},
    {"time", "time without time zone"},
    {"json", "jsonb"},
    {"bytea", "varbinary", "binary", "image"},
]


def _types_are_compatible(source_type: str, sink_type: str) -> bool:
    """Check if two column types are compatible.

    Types are compatible if they belong to the same compatibility group
    or if either is a text/varchar type (which can store anything).

    Args:
        source_type: Source column type (lowered).
        sink_type: Sink column type (lowered).

    Returns:
        True if types are compatible.
    """
    if source_type == sink_type:
        return True

    # Strip length/precision suffixes: varchar(100) → varchar
    source_base = source_type.split("(", maxsplit=1)[0].strip()
    sink_base = sink_type.split("(", maxsplit=1)[0].strip()

    if source_base == sink_base:
        return True

    # Text types can store anything
    text_types = {"text", "varchar", "character varying", "nvarchar", "ntext"}
    if sink_base in text_types:
        return True

    # Check compatibility groups
    return any(
        source_base in group and sink_base in group
        for group in _TYPE_COMPAT_GROUPS
    )


def validate_column_template(
    template_key: str,
    table_key: str,
    source_schema: TableSchema,
    service: str = "",
    value_override: str | None = None,
) -> ValidationResult:
    """Validate a column template against database schema.

    Checks:
    - Template exists in column-templates.yaml
    - applies_to pattern matches table_key
    - Source-ref keys (``{group.sources.*.key}``) exist in source-groups.yaml
    - Bloblang column references (``this.col``) exist in source schema

    Args:
        template_key: Template key to validate.
        table_key: Table key (e.g., "dbo.users").
        source_schema: Database table schema.
        service: Service name (for source-ref group validation).
        value_override: Optional value override from ``--value`` flag.
            When set, validates this instead of the template's default value.

    Returns:
        ValidationResult with errors/warnings.
    """
    errors: list[str] = []
    warnings: list[str] = []
    referenced_columns: set[str] = set()
    env_vars: set[str] = set()

    # Check template exists
    template = get_template(template_key)
    if not template:
        errors.append(f"Template '{template_key}' not found in column-templates.yaml")
        return ValidationResult(
            item_key=template_key,
            is_valid=False,
            errors=errors,
            warnings=warnings,
            referenced_columns=referenced_columns,
            env_vars=env_vars,
        )

    # Check applies_to pattern if defined
    if template.applies_to:
        import fnmatch

        matches_pattern = any(
            fnmatch.fnmatch(table_key, pattern) for pattern in template.applies_to
        )
        if not matches_pattern:
            errors.append(
                f"Template '{template_key}' applies_to {template.applies_to} "
                + f"doesn't match table '{table_key}'"
            )

    # Validate source-group reference in template value
    effective_value = value_override if value_override else template.value
    source_ref_errors = _validate_value_source_ref(
        effective_value, template_key, service,
    )
    errors.extend(source_ref_errors)

    # Validate Bloblang syntax via rpk connect lint (skip for source-refs, sql, env)
    from cdc_generator.core.source_ref_resolver import is_source_ref

    if not is_source_ref(effective_value) and template.value_source == "bloblang":
        bloblang_errors = _validate_bloblang_syntax(
            effective_value, template_key,
        )
        errors.extend(bloblang_errors)

    # Extract column references from Bloblang
    referenced_columns = extract_column_references(effective_value)
    env_vars = uses_environment_variables(effective_value)

    # Skip column validation for static expressions and source-ref values
    if is_static_expression(effective_value):
        return ValidationResult(
            item_key=template_key,
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            referenced_columns=referenced_columns,
            env_vars=env_vars,
        )

    # Validate each referenced column exists
    source_label = f"{source_schema.schema_name}.{source_schema.table_name}"
    for col in referenced_columns:
        if col not in source_schema.columns:
            errors.append(
                f"Template '{template_key}' references column '{col}' "
                + f"which does not exist in source table '{source_label}'. "
                + f"Available columns: {', '.join(sorted(source_schema.columns.keys()))}"
            )

    return ValidationResult(
        item_key=template_key,
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        referenced_columns=referenced_columns,
        env_vars=env_vars,
    )


def validate_transform_rule(
    bloblang_ref: str,
    _table_key: str,
    source_schema: TableSchema,
    available_columns: set[str] | None = None,
) -> ValidationResult:
    """Validate a transform Bloblang file against database schema.

    Checks:
    - Rule exists
    - Condition 'when' expressions reference existing columns
    - Value expressions reference existing columns

    Args:
        bloblang_ref: Transform Bloblang file reference.
        table_key: Table key (e.g., "dbo.users").
        source_schema: Database table schema.

    Returns:
        ValidationResult with errors/warnings.
    """
    errors: list[str] = []
    warnings: list[str] = []
    all_referenced_columns: set[str] = set()
    all_env_vars: set[str] = set()

    bloblang = read_bloblang_ref(bloblang_ref)
    if bloblang is None:
        errors.append(
            f"Transform Bloblang file not found: '{bloblang_ref}'"
        )
        return ValidationResult(
            item_key=bloblang_ref,
            is_valid=False,
            errors=errors,
            warnings=warnings,
            referenced_columns=all_referenced_columns,
            env_vars=all_env_vars,
        )

    bloblang_errors = _validate_bloblang_syntax(bloblang, bloblang_ref)
    errors.extend(bloblang_errors)

    referenced_columns = extract_column_references(bloblang)
    all_referenced_columns.update(referenced_columns)
    all_env_vars.update(uses_environment_variables(bloblang))

    runtime_available = (
        set(available_columns)
        if available_columns is not None
        else set(source_schema.columns.keys())
    )

    if not is_static_expression(bloblang):
        table_label = f"{source_schema.schema_name}.{source_schema.table_name}"
        for col in sorted(referenced_columns):
            if col not in runtime_available:
                errors.append(
                    f"Transform '{bloblang_ref}' references column '{col}' "
                    + "which does not exist in available runtime columns for "
                    + f"source table '{table_label}'"
                )

    produced_columns = extract_root_assignments(bloblang)

    return ValidationResult(
        item_key=bloblang_ref,
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        referenced_columns=all_referenced_columns,
        env_vars=all_env_vars,
        produced_columns=produced_columns,
    )


def validate_templates_for_table(
    service: str,
    table_key: str,
    template_keys: list[str],
    env: str = "nonprod",
    source_table_key: str | None = None,
    value_override: str | None = None,
) -> bool:
    """Validate all templates for a table against database schema.

    Args:
        service: Service name.
        table_key: Table key (e.g., "dbo.users").
        template_keys: List of template keys to validate.
        env: Environment to inspect.
        source_table_key: Optional source table key (from the ``from``
            field) for sink tables whose schema lives under a different
            directory than *table_key*.
        value_override: Optional value override from ``--value`` flag.
            Passed through to ``validate_column_template``.

    Returns:
        True if all validations passed.
    """
    if source_table_key:
        print_info(
            f"\nValidating {len(template_keys)} column template(s)"
            + f" for '{table_key}' (source: '{source_table_key}')..."
        )
    else:
        print_info(
            f"\nValidating {len(template_keys)} column template(s) for '{table_key}'..."
        )

    # Get database schema
    schema = get_source_table_schema(
        service, table_key, env, source_table_key=source_table_key,
    )
    if not schema:
        print_error("Failed to inspect database schema")
        return False

    all_valid = True

    for template_key in template_keys:
        result = validate_column_template(
            template_key, table_key, schema,
            service=service, value_override=value_override,
        )

        if result.is_valid:
            status = "✓"
            if result.referenced_columns:
                cols = ", ".join(sorted(result.referenced_columns))
                print_success(f"  {status} {template_key} (uses: {cols})")
            else:
                print_success(f"  {status} {template_key} (static expression)")
        else:
            all_valid = False
            print_error(f"  ✗ {template_key}")
            for error in result.errors:
                print_error(f"      {error}")

        if result.warnings:
            for warning in result.warnings:
                print_warning(f"      {warning}")

        if result.env_vars:
            env_list = ", ".join(sorted(result.env_vars))
            print_info(f"      Environment variables: {env_list}")

    return all_valid


def validate_transforms_for_table(
    service: str,
    table_key: str,
    transform_refs: list[str],
    env: str = "nonprod",
    source_table_key: str | None = None,
) -> bool:
    """Validate all transform rules for a table against database schema.

    Args:
        service: Service name.
        table_key: Table key (e.g., "dbo.users").
        transform_refs: List of transform Bloblang refs to validate.
        env: Environment to inspect.
        source_table_key: Optional source table key (from the ``from``
            field) for sink tables whose schema lives under a different
            directory than *table_key*.

    Returns:
        True if all validations passed.
    """
    if source_table_key:
        print_info(
            f"\nValidating {len(transform_refs)} transform(s)"
            + f" for '{table_key}' (source: '{source_table_key}')..."
        )
    else:
        print_info(
            f"\nValidating {len(transform_refs)} transform(s) for '{table_key}'..."
        )

    # Get database schema
    schema = get_source_table_schema(
        service, table_key, env, source_table_key=source_table_key,
    )
    if not schema:
        print_error("Failed to inspect database schema")
        return False

    all_valid = True
    runtime_available = set(schema.columns.keys())

    for bloblang_ref in transform_refs:
        result = validate_transform_rule(
            bloblang_ref,
            table_key,
            schema,
            available_columns=runtime_available,
        )

        if result.is_valid:
            status = "✓"
            if result.referenced_columns:
                cols = ", ".join(sorted(result.referenced_columns))
                print_success(f"  {status} {bloblang_ref} (uses: {cols})")
            else:
                print_success(f"  {status} {bloblang_ref} (static expression)")
        else:
            all_valid = False
            print_error(f"  ✗ {bloblang_ref}")
            for error in result.errors:
                print_error(f"      {error}")

        if result.warnings:
            for warning in result.warnings:
                print_warning(f"      {warning}")

        if result.env_vars:
            env_list = ", ".join(sorted(result.env_vars))
            print_info(f"      Environment variables: {env_list}")

        if result.is_valid and result.produced_columns:
            runtime_available.update(result.produced_columns)

    return all_valid
