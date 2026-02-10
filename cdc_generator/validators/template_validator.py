"""Database-aware validation for column templates and transform rules.

Validates that templates and transforms reference actual database columns
and have compatible types.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from cdc_generator.core.column_templates import get_template
from cdc_generator.core.transform_rules import get_rule
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.validators.bloblang_parser import (
    extract_column_references,
    is_static_expression,
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


@dataclass
class TableSchema:
    """Database table schema information."""

    table_name: str
    schema_name: str
    columns: dict[str, str]  # column_name -> data_type


def _get_source_table_schema(
    service: str,
    table_key: str,
    env: str = "nonprod",
) -> TableSchema | None:
    """Get schema for source table from saved schema YAML files.

    Args:
        service: Service name.
        table_key: Table key (e.g., "dbo.users" or "public.users").
        env: Environment (unused, kept for API compatibility).

    Returns:
        TableSchema if successful, None otherwise.
    """
    from cdc_generator.core.structure_replicator import load_source_schema

    _ = env  # kept for API compatibility

    # Parse table_key to extract schema.table
    if "." in table_key:
        schema_name, table_name = table_key.split(".", 1)
    else:
        schema_name = "dbo"  # Default for MSSQL
        table_name = table_key

    try:
        schema_data = load_source_schema(service, schema_name, table_name)
        if not schema_data:
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


def validate_column_template(
    template_key: str,
    table_key: str,
    source_schema: TableSchema,
) -> ValidationResult:
    """Validate a column template against database schema.

    Checks:
    - Template exists
    - Bloblang references existing columns
    - applies_to pattern matches table_key

    Args:
        template_key: Template key to validate.
        table_key: Table key (e.g., "dbo.users").
        source_schema: Database table schema.

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

    # Extract column references from Bloblang
    referenced_columns = extract_column_references(template.value)
    env_vars = uses_environment_variables(template.value)

    # Skip column validation for static expressions
    if is_static_expression(template.value):
        return ValidationResult(
            item_key=template_key,
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            referenced_columns=referenced_columns,
            env_vars=env_vars,
        )

    # Validate each referenced column exists
    for col in referenced_columns:
        if col not in source_schema.columns:
            errors.append(
                f"Template '{template_key}' references non-existent column '{col}'. "
                + f"Available: {', '.join(sorted(source_schema.columns.keys()))}"
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
    rule_key: str,
    _table_key: str,
    source_schema: TableSchema,
) -> ValidationResult:
    """Validate a transform rule against database schema.

    Checks:
    - Rule exists
    - Condition 'when' expressions reference existing columns
    - Value expressions reference existing columns

    Args:
        rule_key: Transform rule key.
        table_key: Table key (e.g., "dbo.users").
        source_schema: Database table schema.

    Returns:
        ValidationResult with errors/warnings.
    """
    errors: list[str] = []
    warnings: list[str] = []
    all_referenced_columns: set[str] = set()
    all_env_vars: set[str] = set()

    # Check rule exists
    rule = get_rule(rule_key)
    if not rule:
        errors.append(f"Transform rule '{rule_key}' not found in transform-rules.yaml")
        return ValidationResult(
            item_key=rule_key,
            is_valid=False,
            errors=errors,
            warnings=warnings,
            referenced_columns=all_referenced_columns,
            env_vars=all_env_vars,
        )

    # Validate each condition
    for idx, condition in enumerate(rule.conditions):
        # Check 'when' expression
        when_columns = extract_column_references(condition.when)
        when_env = uses_environment_variables(condition.when)
        all_referenced_columns.update(when_columns)
        all_env_vars.update(when_env)

        for col in when_columns:
            if col not in source_schema.columns:
                errors.append(
                    f"Transform '{rule_key}' condition[{idx}].when "
                    + f"references non-existent column '{col}'"
                )

        # Check 'value' expression if present
        if condition.value:
            value_columns = extract_column_references(condition.value)
            value_env = uses_environment_variables(condition.value)
            all_referenced_columns.update(value_columns)
            all_env_vars.update(value_env)

            for col in value_columns:
                if col not in source_schema.columns:
                    errors.append(
                        f"Transform '{rule_key}' condition[{idx}].value "
                        + f"references non-existent column '{col}'"
                    )

    # Check default_value if present
    if rule.default_value:
        default_columns = extract_column_references(rule.default_value)
        default_env = uses_environment_variables(rule.default_value)
        all_referenced_columns.update(default_columns)
        all_env_vars.update(default_env)

        for col in default_columns:
            if col not in source_schema.columns:
                errors.append(
                    f"Transform '{rule_key}' default_value "
                    + f"references non-existent column '{col}'"
                )

    return ValidationResult(
        item_key=rule_key,
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        referenced_columns=all_referenced_columns,
        env_vars=all_env_vars,
    )


def validate_templates_for_table(
    service: str,
    table_key: str,
    template_keys: list[str],
    env: str = "nonprod",
) -> bool:
    """Validate all templates for a table against database schema.

    Args:
        service: Service name.
        table_key: Table key (e.g., "dbo.users").
        template_keys: List of template keys to validate.
        env: Environment to inspect.

    Returns:
        True if all validations passed.
    """
    print_info(f"\nValidating {len(template_keys)} column template(s) for '{table_key}'...")

    # Get database schema
    schema = _get_source_table_schema(service, table_key, env)
    if not schema:
        print_error("Failed to inspect database schema")
        return False

    all_valid = True

    for template_key in template_keys:
        result = validate_column_template(template_key, table_key, schema)

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
    transform_keys: list[str],
    env: str = "nonprod",
) -> bool:
    """Validate all transform rules for a table against database schema.

    Args:
        service: Service name.
        table_key: Table key (e.g., "dbo.users").
        transform_keys: List of transform rule keys to validate.
        env: Environment to inspect.

    Returns:
        True if all validations passed.
    """
    print_info(f"\nValidating {len(transform_keys)} transform rule(s) for '{table_key}'...")

    # Get database schema
    schema = _get_source_table_schema(service, table_key, env)
    if not schema:
        print_error("Failed to inspect database schema")
        return False

    all_valid = True

    for rule_key in transform_keys:
        result = validate_transform_rule(rule_key, table_key, schema)

        if result.is_valid:
            status = "✓"
            if result.referenced_columns:
                cols = ", ".join(sorted(result.referenced_columns))
                print_success(f"  {status} {rule_key} (uses: {cols})")
            else:
                print_success(f"  {status} {rule_key} (static expression)")
        else:
            all_valid = False
            print_error(f"  ✗ {rule_key}")
            for error in result.errors:
                print_error(f"      {error}")

        if result.warnings:
            for warning in result.warnings:
                print_warning(f"      {warning}")

        if result.env_vars:
            env_list = ", ".join(sorted(result.env_vars))
            print_info(f"      Environment variables: {env_list}")

    return all_valid
