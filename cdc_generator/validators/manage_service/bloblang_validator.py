"""Bloblang syntax validation using rpk connect lint."""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from typing import cast

import yaml

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.service_schema_paths import get_schema_write_root


def check_rpk_available() -> bool:
    """Check if rpk CLI is available."""
    try:
        result = subprocess.run(
            ["rpk", "--version"],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def validate_bloblang_expression(
    expression: str,
    _context: str,
) -> tuple[bool, str | None]:
    """Validate a single Bloblang expression using rpk connect lint.

    Args:
        expression: Bloblang expression to validate
        context: Context description (e.g., "template: source_table")

    Returns:
        tuple: (is_valid, error_message)
    """
    try:
        # Create pipeline with literal block scalar to preserve line numbers
        # The | in YAML preserves newlines exactly
        pipeline_yaml = f"""input:
  stdin: {{}}
pipeline:
  processors:
    - mapping: |
        {expression.replace(chr(10), chr(10) + '        ')}
output:
  drop: {{}}
"""

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".yaml",
            delete=False,
        ) as tmp:
            tmp.write(pipeline_yaml)
            tmp_path = tmp.name

        result = subprocess.run(
            ["rpk", "connect", "lint", tmp_path],
            capture_output=True,
            text=True,
            check=False,
        )

        Path(tmp_path).unlink()

        if result.returncode == 0:
            return True, None

        # The mapping starts at line 6 (after "mapping: |")
        # So we need to subtract 6 from reported line numbers
        line_offset = 6

        error_lines = result.stderr.strip().split("\n")
        import re
        col_indent_offset = 8
        adjusted_errors: list[str] = []
        for error_line in error_lines:
            if not error_line.startswith("Error:"):
                # Adjust line numbers by subtracting offset
                match = re.search(r'\((\d+),(\d+)\)', error_line)
                adjusted = error_line
                if match:
                    orig_line = int(match.group(1))
                    col = int(match.group(2))
                    adjusted_line = orig_line - line_offset
                    if adjusted_line > 0:
                        # Also adjust column for the indentation we added
                        adjusted_col = (
                            col - col_indent_offset
                            if col > col_indent_offset
                            else col
                        )
                        adjusted = re.sub(
                            r'\(\d+,\d+\)',
                            f'({adjusted_line},{adjusted_col})',
                            error_line,
                        )
                adjusted_errors.append(adjusted)

        error_msg = "\n".join(adjusted_errors) if adjusted_errors else result.stderr.strip()
        return False, error_msg

    except Exception as e:
        return False, f"Validation error: {e}"


def validate_column_templates_bloblang(
    service: str,
) -> tuple[int, int]:
    """Validate all Bloblang expressions in column-templates.yaml.

    Args:
        service: Service name

    Returns:
        tuple: (valid_count, error_count)
    """
    templates_file = Path("service-schemas") / "column-templates.yaml"

    if not templates_file.exists():
        print_warning(f"No column-templates.yaml found for service {service}")
        return 0, 0

    try:
        with templates_file.open() as f:
            data = cast(dict[str, object], yaml.safe_load(f))

        templates = cast(dict[str, object], data.get("templates", {}))
        if not templates:
            print_info("No templates found in column-templates.yaml")
            return 0, 0

        template_count = len(templates)
        print_info(
            f"\nValidating Bloblang in column-templates.yaml ({template_count} templates)..."
        )

        valid_count = 0
        error_count = 0

        for key, raw_template in templates.items():
            if not isinstance(raw_template, dict):
                continue

            template = cast(dict[str, object], raw_template)
            raw_value = template.get("value")
            if not raw_value:
                continue

            value = str(raw_value)
            context = f"column_template: {key}"
            is_valid, error_msg = validate_bloblang_expression(value, context)

            if is_valid:
                print_success(f"  ✓ {key}: {value[:50]}...")
                valid_count += 1
            else:
                print_error(f"  ✗ {key}: {value[:50]}...")
                if error_msg:
                    # Show clickable link to the file
                    print_error("    service-schemas/column-templates.yaml")
                    print_error(f"    {error_msg}")
                error_count += 1

        return valid_count, error_count

    except Exception as e:
        print_error(f"Failed to validate column templates: {e}")
        return 0, 1


def validate_transform_rules_bloblang(
    service: str,
) -> tuple[int, int]:
    """Validate all Bloblang expressions in transform-rules.yaml.

    Args:
        service: Service name

    Returns:
        tuple: (valid_count, error_count)
    """
    transform_file = Path("service-schemas") / "transform-rules.yaml"

    if not transform_file.exists():
        print_warning(f"No transform-rules.yaml found for service {service}")
        return 0, 0

    try:
        with transform_file.open() as f:
            data = cast(dict[str, object], yaml.safe_load(f))

        rules = cast(dict[str, object], data.get("rules", {}))
        if not rules:
            print_info("No rules found in transform-rules.yaml")
            return 0, 0

        print_info(f"\nValidating Bloblang in transform-rules.yaml ({len(rules)} rules)...")

        valid_count = 0
        error_count = 0

        for key, raw_rule in rules.items():
            if not isinstance(raw_rule, dict):
                continue

            rule = cast(dict[str, object], raw_rule)
            raw_mapping = rule.get("mapping")
            if not raw_mapping:
                continue

            mapping = str(raw_mapping)
            context = f"transform_rule: {key}"
            is_valid, error_msg = validate_bloblang_expression(mapping, context)

            if is_valid:
                print_success(f"  ✓ {key}: {mapping[:50]}...")
                valid_count += 1
            else:
                print_error(f"  ✗ {key}: {mapping[:50]}...")
                if error_msg:
                    # Show clickable link to the file
                    print_error("    service-schemas/transform-rules.yaml")
                    print_error(f"    {error_msg}")
                error_count += 1

        return valid_count, error_count

    except Exception as e:
        print_error(f"Failed to validate transform rules: {e}")
        return 0, 1


def validate_service_bloblang(service: str) -> bool:
    """Validate all Bloblang expressions for a service.

    Args:
        service: Service name

    Returns:
        bool: True if all validations passed
    """
    if not check_rpk_available():
        print_error("rpk CLI not found. Please install Redpanda Connect CLI:")
        print_error("  https://docs.redpanda.com/current/get-started/rpk-install/")
        return False

    print_info(f"Validating Bloblang for service: {service}")
    print_info("Using: rpk connect lint\n")

    # Validate .blobl files first
    blobl_valid, blobl_errors = validate_bloblang_files()

    # Validate templates and transforms
    template_valid, template_errors = validate_column_templates_bloblang(service)
    transform_valid, transform_errors = validate_transform_rules_bloblang(service)

    total_valid = blobl_valid + template_valid + transform_valid
    total_errors = blobl_errors + template_errors + transform_errors

    print_info("\n" + "=" * 60)
    if total_errors == 0 and total_valid > 0:
        print_success(f"✓ All {total_valid} Bloblang expressions are valid!")
        return True
    if total_errors > 0:
        print_error(
            f"✗ {total_errors} Bloblang expression(s) failed validation "
            + f"({total_valid} valid)"
        )
        return False
    print_warning("No Bloblang expressions found to validate")
    return True


def validate_bloblang_files() -> tuple[int, int]:
    """Validate all .blobl files in services/_schemas/_bloblang/.

    Returns:
        tuple: (valid_count, error_count)
    """
    blobl_dir = get_schema_write_root(get_project_root()) / "_bloblang"

    if not blobl_dir.exists():
        return 0, 0

    blobl_files = list(blobl_dir.rglob("*.blobl"))

    if not blobl_files:
        return 0, 0

    print_info(f"Validating Bloblang files ({len(blobl_files)} files)...")

    valid_count = 0
    error_count = 0

    for blobl_file in blobl_files:
        try:
            content = blobl_file.read_text(encoding="utf-8")
        except Exception as e:
            print_error(f"  ✗ {blobl_file.name}: Failed to read file - {e}")
            error_count += 1
            continue

        # Validate by wrapping in a minimal pipeline (same as templates)
        is_valid, error_msg = validate_bloblang_expression(
            content,
            str(blobl_file.relative_to(blobl_dir)),
        )

        if is_valid:
            print_success(f"  ✓ {blobl_file.relative_to(blobl_dir)}")
            valid_count += 1
        else:
            relative_path = blobl_file.relative_to(blobl_dir)
            print_error(f"  ✗ {relative_path}")
            if error_msg:
                # Parse line/column from rpk error (format: file.yaml(line,col) message)
                import re
                match = re.search(r'\((\d+),(\d+)\)', error_msg)
                if match:
                    line_num = match.group(1)
                    col_num = match.group(2)
                    # Extract just the error message part
                    msg_match = re.search(r'\)\s+(.+)$', error_msg)
                    message = msg_match.group(1) if msg_match else error_msg
                    # Show clickable link with line:column
                    clickable_path = (
                        f"services/_schemas/_bloblang/{relative_path}"
                        + f":{line_num}:{col_num}"
                    )
                    print_error(f"    {clickable_path}")
                    print_error(f"    {message}")
                else:
                    print_error(f"    {error_msg}")
            error_count += 1

    return valid_count, error_count
