"""Tests for PostgreSQL schema name validation.

Covers:
- validate_pg_schema_name() in sink_operations.py
- Integration with add_sink_table (--sink-schema / --target-schema)
- Integration with update_sink_table_schema (--update-schema)
- Integration with _validate_single_sink (existing config validation)
"""

import argparse
import os
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest

from cdc_generator.cli.service_handlers_sink import (
    handle_sink_add_table,
    handle_sink_update_schema,
)
from cdc_generator.validators.manage_service.sink_operations import (
    validate_pg_schema_name,
    validate_sinks,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SERVICE_YAML = (
    "myservice:\n"
    "  source:\n"
    "    tables:\n"
    "      public.users: {}\n"
    "  sinks:\n"
    "    sink_asma.proxy:\n"
    "      tables:\n"
    "        public.users:\n"
    "          target_exists: false\n"
)

SOURCE_TABLE_SCHEMA = (
    "columns:\n"
    "  - name: id\n"
    "    type: uuid\n"
    "    primary_key: true\n"
    "  - name: name\n"
    "    type: text\n"
)


@pytest.fixture()
def project_dir(tmp_path: Path) -> Iterator[Path]:
    """Isolated project with services/ and service-schemas/."""
    services_dir = tmp_path / "services"
    services_dir.mkdir()
    (tmp_path / "source-groups.yaml").write_text(
        "asma:\n  pattern: db-shared\n"
    )
    (tmp_path / "sink-groups.yaml").write_text(
        "sink_asma:\n"
        "  type: postgres\n"
        "  server: sink-pg\n"
    )
    service_schemas_dir = tmp_path / "service-schemas"
    service_schemas_dir.mkdir()

    # Create source table schema
    source_dir = service_schemas_dir / "myservice" / "public"
    source_dir.mkdir(parents=True)
    (source_dir / "users.yaml").write_text(SOURCE_TABLE_SCHEMA)

    # Also create it under proxy target service
    proxy_dir = service_schemas_dir / "proxy" / "public"
    proxy_dir.mkdir(parents=True)
    (proxy_dir / "users.yaml").write_text(SOURCE_TABLE_SCHEMA)

    original_cwd = Path.cwd()
    os.chdir(tmp_path)
    with patch(
        "cdc_generator.validators.manage_service.config.SERVICES_DIR",
        services_dir,
    ), patch(
        "cdc_generator.validators.manage_service.sink_operations.SERVICE_SCHEMAS_DIR",
        service_schemas_dir,
    ):
        try:
            yield tmp_path
        finally:
            os.chdir(original_cwd)


@pytest.fixture()
def service_yaml(project_dir: Path) -> Path:
    """Write the service YAML file."""
    sf = project_dir / "services" / "myservice.yaml"
    sf.write_text(SERVICE_YAML)
    return sf


def _ns(**kwargs: object) -> argparse.Namespace:
    defaults: dict[str, object] = {
        "service": "myservice",
        "sink": "sink_asma.proxy",
        "add_sink": None,
        "remove_sink": None,
        "add_sink_table": None,
        "remove_sink_table": None,
        "update_schema": None,
        "sink_table": None,
        "from_table": None,
        "replicate_structure": False,
        "sink_schema": None,
        "target_exists": None,
        "target": None,
        "target_schema": None,
        "map_column": None,
        "include_sink_columns": None,
        "add_custom_sink_table": None,
        "column": None,
        "modify_custom_table": None,
        "add_column": None,
        "remove_column": None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ═══════════════════════════════════════════════════════════════════════════
# validate_pg_schema_name() — pure unit tests
# ═══════════════════════════════════════════════════════════════════════════


class TestValidatePgSchemaName:
    """Tests for validate_pg_schema_name()."""

    def test_valid_simple_name(self) -> None:
        """Standard schema names are valid."""
        assert validate_pg_schema_name("public") is None

    def test_valid_underscore_name(self) -> None:
        """Underscored names are valid."""
        assert validate_pg_schema_name("directory_clone") is None

    def test_valid_with_digits(self) -> None:
        """Names with digits (not leading) are valid."""
        assert validate_pg_schema_name("schema2") is None

    def test_valid_leading_underscore(self) -> None:
        """Leading underscore is valid."""
        assert validate_pg_schema_name("_private") is None

    def test_valid_with_dollar(self) -> None:
        """Dollar sign is allowed in PostgreSQL identifiers."""
        assert validate_pg_schema_name("pg$temp") is None

    def test_valid_uppercase(self) -> None:
        """Uppercase letters are valid (PostgreSQL folds to lowercase)."""
        assert validate_pg_schema_name("MySchema") is None

    def test_invalid_hyphen(self) -> None:
        """Hyphen is invalid — suggests underscore replacement."""
        result = validate_pg_schema_name("directory-clone")
        assert result is not None
        assert "hyphens" in result
        assert "directory_clone" in result  # Suggestion

    def test_invalid_multiple_hyphens(self) -> None:
        """Multiple hyphens are caught."""
        result = validate_pg_schema_name("my-custom-schema")
        assert result is not None
        assert "my_custom_schema" in result

    def test_invalid_leading_digit(self) -> None:
        """Leading digit is invalid."""
        result = validate_pg_schema_name("123abc")
        assert result is not None
        assert "starts with a digit" in result

    def test_invalid_space(self) -> None:
        """Spaces are invalid."""
        result = validate_pg_schema_name("my schema")
        assert result is not None
        assert "invalid characters" in result

    def test_invalid_special_chars(self) -> None:
        """Special characters like @ are invalid."""
        result = validate_pg_schema_name("my@schema")
        assert result is not None

    def test_empty_string(self) -> None:
        """Empty string is invalid."""
        result = validate_pg_schema_name("")
        assert result is not None
        assert "empty" in result

    def test_too_long(self) -> None:
        """Names exceeding 63 characters are invalid."""
        long_name = "a" * 64
        result = validate_pg_schema_name(long_name)
        assert result is not None
        assert "63" in result

    def test_max_length_ok(self) -> None:
        """Exactly 63 characters is fine."""
        assert validate_pg_schema_name("a" * 63) is None

    def test_single_letter(self) -> None:
        """Single letter is valid."""
        assert validate_pg_schema_name("x") is None

    def test_single_underscore(self) -> None:
        """Single underscore is valid."""
        assert validate_pg_schema_name("_") is None


# ═══════════════════════════════════════════════════════════════════════════
# Integration — add_sink_table rejects invalid --sink-schema
# ═══════════════════════════════════════════════════════════════════════════


class TestAddSinkTableSchemaValidation:
    """Tests that add_sink_table rejects invalid schema names."""

    def test_rejects_hyphenated_sink_schema(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--sink-schema with hyphen is rejected."""
        args = _ns(
            add_sink_table="public.users",
            from_table="public.users",
            sink_schema="directory-clone",
            target_exists="false",
            replicate_structure=True,
        )
        result = handle_sink_add_table(args)
        assert result == 1

    def test_accepts_underscored_sink_schema(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--sink-schema with underscore is accepted."""
        args = _ns(
            add_sink_table="public.users",
            from_table="public.users",
            sink_schema="directory_clone",
            target_exists="false",
            replicate_structure=True,
        )
        result = handle_sink_add_table(args)
        assert result == 0

    def test_rejects_hyphenated_target_schema(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--target-schema with hyphen is rejected."""
        args = _ns(
            add_sink_table="public.users",
            from_table="public.users",
            target_exists="false",
            target_schema="my-bad-schema",
        )
        result = handle_sink_add_table(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# Integration — update_sink_table_schema rejects invalid schema
# ═══════════════════════════════════════════════════════════════════════════


class TestUpdateSchemaValidation:
    """Tests that update_sink_table_schema rejects invalid schema names."""

    def test_rejects_hyphenated_schema(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--update-schema with hyphen is rejected."""
        args = _ns(
            sink_table="public.users",
            update_schema="bad-schema",
        )
        result = handle_sink_update_schema(args)
        assert result == 1

    def test_rejects_digit_leading_schema(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--update-schema starting with digit is rejected."""
        args = _ns(
            sink_table="public.users",
            update_schema="2schema",
        )
        result = handle_sink_update_schema(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# Integration — validate_sinks catches existing bad schemas
# ═══════════════════════════════════════════════════════════════════════════


class TestValidateSinksCatchesBadSchema:
    """Tests that validate_sinks catches hyphenated schemas in existing config."""

    def test_catches_hyphenated_schema_in_config(
        self, project_dir: Path,
    ) -> None:
        """validate_sinks reports error for table with hyphenated schema."""
        (project_dir / "services" / "myservice.yaml").write_text(
            "myservice:\n"
            "  source:\n"
            "    tables:\n"
            "      public.users: {}\n"
            "  sinks:\n"
            "    sink_asma.proxy:\n"
            "      tables:\n"
            "        directory-clone.users:\n"
            "          target_exists: false\n"
        )
        result = validate_sinks("myservice")
        assert result is False

    def test_passes_valid_schema_in_config(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """validate_sinks passes with valid schema names."""
        result = validate_sinks("myservice")
        assert result is True
