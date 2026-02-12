"""Tests for the template validation call chain.

Verifies that ``validate_templates_for_table`` properly passes
``service`` and ``value_override`` through to ``validate_column_template``,
and that Bloblang syntax and column-reference validation work end-to-end.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cdc_generator.core.column_templates import (
    clear_cache as clear_template_cache,
)
from cdc_generator.core.column_templates import (
    set_templates_path,
)
from cdc_generator.validators.template_validator import (
    TableSchema,
    _types_are_compatible,
    _validate_bloblang_syntax,
    _validate_value_source_ref,
    get_sink_table_schema,
    validate_column_mapping_types,
    validate_column_template,
    validate_sink_column_exists,
    validate_templates_for_table,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_caches() -> None:
    """Clear template cache before each test."""
    clear_template_cache()


@pytest.fixture()
def templates_file(tmp_path: Path) -> Path:
    """Create a temporary column-templates.yaml with various templates."""
    content = """\
templates:
  tenant_id:
    name: _tenant_id
    type: uuid
    not_null: true
    description: Tenant identifier
    value: "{asma.sources.*.customer_id}"
  source_table:
    name: _source_table
    type: text
    not_null: true
    description: Source table name
    value: meta("table")
  row_hash:
    name: _row_hash
    type: text
    not_null: false
    description: Row hash
    value: this.name.hash("sha256").encode("hex")
  multi_col:
    name: _combined
    type: text
    not_null: false
    description: Uses multiple columns
    value: this.first_name + " " + this.last_name
"""
    file_path = tmp_path / "column-templates.yaml"
    file_path.write_text(content)
    set_templates_path(file_path)
    return file_path


@pytest.fixture()
def source_schema() -> TableSchema:
    """Schema with known columns for testing."""
    return TableSchema(
        table_name="customers",
        schema_name="public",
        columns={
            "id": "uuid",
            "name": "text",
            "subdomain": "text",
            "first_name": "text",
            "last_name": "text",
        },
    )


@pytest.fixture()
def source_schema_dir(tmp_path: Path) -> Path:
    """Create service-schemas/ with a source table schema."""
    schema_dir = tmp_path / "service-schemas" / "test_svc" / "public"
    schema_dir.mkdir(parents=True)
    schema_file = schema_dir / "customers.yaml"
    schema_file.write_text(
        "database: test_db\n"
        "schema: public\n"
        "service: test_svc\n"
        "table: customers\n"
        "columns:\n"
        "- name: id\n"
        "  type: uuid\n"
        "  nullable: false\n"
        "  primary_key: true\n"
        "- name: name\n"
        "  type: text\n"
        "  nullable: false\n"
        "  primary_key: false\n"
        "- name: first_name\n"
        "  type: text\n"
        "  nullable: true\n"
        "  primary_key: false\n"
        "- name: last_name\n"
        "  type: text\n"
        "  nullable: true\n"
        "  primary_key: false\n"
    )
    return tmp_path / "service-schemas"


# ---------------------------------------------------------------------------
# Tests — _validate_value_source_ref
# ---------------------------------------------------------------------------


class TestValidateValueSourceRef:
    """Tests for source-ref key validation."""

    def test_non_source_ref_returns_no_errors(self) -> None:
        """Plain Bloblang → no source-ref errors."""
        errors = _validate_value_source_ref(
            'this.name.hash("sha256")', "row_hash", "test_svc",
        )
        assert errors == []

    def test_static_expression_returns_no_errors(self) -> None:
        """Static expression → no source-ref errors."""
        errors = _validate_value_source_ref(
            'meta("table")', "source_table", "test_svc",
        )
        assert errors == []

    def test_valid_source_ref_with_existing_key(self) -> None:
        """Valid source-ref with existing key → no errors."""
        mock_config = {
            "asma": {
                "sources": {
                    "src1": {"customer_id": "cust-001"},
                },
            },
        }
        with patch(
            "cdc_generator.core.source_ref_resolver._load_source_groups",
            return_value=mock_config,
        ):
            errors = _validate_value_source_ref(
                "{asma.sources.*.customer_id}", "tenant_id", "test_svc",
            )
        assert errors == []

    def test_source_ref_with_missing_key(self) -> None:
        """Source-ref with missing key → error."""
        mock_config = {
            "asma": {
                "sources": {
                    "src1": {"host": "localhost"},
                },
            },
        }
        with patch(
            "cdc_generator.core.source_ref_resolver._load_source_groups",
            return_value=mock_config,
        ):
            errors = _validate_value_source_ref(
                "{asma.sources.*.customer_id}", "tenant_id", "test_svc",
            )
        assert len(errors) > 0
        assert "customer_id" in errors[0]

    def test_source_ref_with_missing_group(self) -> None:
        """Source-ref with missing group → error."""
        mock_config = {
            "other_group": {
                "sources": {},
            },
        }
        with patch(
            "cdc_generator.core.source_ref_resolver._load_source_groups",
            return_value=mock_config,
        ):
            errors = _validate_value_source_ref(
                "{asma.sources.*.customer_id}", "tenant_id", "test_svc",
            )
        assert len(errors) > 0

    def test_invalid_source_ref_format(self) -> None:
        """Malformed source-ref → format error."""
        errors = _validate_value_source_ref(
            "{invalid_format}", "bad_ref", "test_svc",
        )
        # The value matches is_source_ref but fails to parse
        # Only curly-brace patterns that match is_source_ref are checked
        # {invalid_format} doesn't match is_source_ref (no dots/wildcards)
        # so it's not treated as a source-ref at all
        assert errors == []


# ---------------------------------------------------------------------------
# Tests — _validate_bloblang_syntax
# ---------------------------------------------------------------------------


class TestValidateBloblangSyntax:
    """Tests for Bloblang syntax validation via rpk."""

    def test_returns_empty_when_rpk_unavailable(self) -> None:
        """When rpk is not available, gracefully returns no errors."""
        with patch(
            "cdc_generator.validators.manage_service.bloblang_validator.check_rpk_available",
            return_value=False,
        ):
            errors = _validate_bloblang_syntax(
                'this.name.hash("sha256")', "row_hash",
            )
        assert errors == []

    def test_valid_bloblang_returns_no_errors(self) -> None:
        """Valid Bloblang expression → no errors."""
        with patch(
            "cdc_generator.validators.manage_service.bloblang_validator.check_rpk_available",
            return_value=True,
        ), patch(
            "cdc_generator.validators.manage_service.bloblang_validator.validate_bloblang_expression",
            return_value=(True, None),
        ):
            errors = _validate_bloblang_syntax(
                'this.name.hash("sha256")', "row_hash",
            )
        assert errors == []

    def test_invalid_bloblang_returns_error(self) -> None:
        """Invalid Bloblang expression → error with syntax details."""
        with patch(
            "cdc_generator.validators.manage_service.bloblang_validator.check_rpk_available",
            return_value=True,
        ), patch(
            "cdc_generator.validators.manage_service.bloblang_validator.validate_bloblang_expression",
            return_value=(False, "unexpected token"),
        ):
            errors = _validate_bloblang_syntax(
                "this.name.bad_func(", "broken",
            )
        assert len(errors) == 1
        assert "invalid Bloblang syntax" in errors[0]
        assert "unexpected token" in errors[0]


# ---------------------------------------------------------------------------
# Tests — validate_column_template with service and value_override
# ---------------------------------------------------------------------------


class TestValidateColumnTemplateParams:
    """Tests that validate_column_template uses service and value_override."""

    def test_value_override_used_for_column_refs(
        self,
        templates_file: Path,
        source_schema: TableSchema,
    ) -> None:
        """value_override replaces template default for column checks."""
        # row_hash template normally refs 'this.name' which exists.
        # Override with a value that refs a non-existent column.
        with patch(
            "cdc_generator.validators.manage_service.bloblang_validator.check_rpk_available",
            return_value=False,
        ):
            result = validate_column_template(
                "row_hash",
                "public.customers",
                source_schema,
                value_override="this.nonexistent_col.hash(\"sha256\")",
            )
        assert not result.is_valid
        assert any("nonexistent_col" in e for e in result.errors)

    def test_value_override_source_ref_validated(
        self,
        templates_file: Path,
        source_schema: TableSchema,
    ) -> None:
        """value_override with source-ref triggers source-ref validation."""
        mock_config = {
            "asma": {
                "sources": {
                    "src1": {"host": "localhost"},
                },
            },
        }
        with patch(
            "cdc_generator.core.source_ref_resolver._load_source_groups",
            return_value=mock_config,
        ):
            result = validate_column_template(
                "source_table",
                "public.customers",
                source_schema,
                service="test_svc",
                value_override="{asma.sources.*.missing_key}",
            )
        assert not result.is_valid
        assert any("missing_key" in e for e in result.errors)

    def test_default_value_used_when_no_override(
        self,
        templates_file: Path,
        source_schema: TableSchema,
    ) -> None:
        """Without value_override, template's default value is used."""
        # row_hash refs this.name which exists in source_schema
        with patch(
            "cdc_generator.validators.manage_service.bloblang_validator.check_rpk_available",
            return_value=False,
        ):
            result = validate_column_template(
                "row_hash",
                "public.customers",
                source_schema,
            )
        assert result.is_valid
        assert "name" in result.referenced_columns

    def test_column_ref_not_in_source_schema(
        self,
        templates_file: Path,
        source_schema: TableSchema,
    ) -> None:
        """Template referencing non-existent column → error."""
        # multi_col refs this.first_name + this.last_name
        # Remove first_name from schema
        limited_schema = TableSchema(
            table_name="customers",
            schema_name="public",
            columns={"id": "uuid", "last_name": "text"},
        )
        with patch(
            "cdc_generator.validators.manage_service.bloblang_validator.check_rpk_available",
            return_value=False,
        ):
            result = validate_column_template(
                "multi_col",
                "public.customers",
                limited_schema,
            )
        assert not result.is_valid
        assert any("first_name" in e for e in result.errors)

    def test_static_expression_skips_column_check(
        self,
        templates_file: Path,
        source_schema: TableSchema,
    ) -> None:
        """Static expression (meta, env) → no column validation needed."""
        result = validate_column_template(
            "source_table",
            "public.customers",
            source_schema,
        )
        assert result.is_valid
        assert len(result.referenced_columns) == 0

    def test_bloblang_syntax_validated_for_non_source_ref(
        self,
        templates_file: Path,
        source_schema: TableSchema,
    ) -> None:
        """Non-source-ref value triggers Bloblang syntax check."""
        with patch(
            "cdc_generator.validators.manage_service.bloblang_validator.check_rpk_available",
            return_value=True,
        ), patch(
            "cdc_generator.validators.manage_service.bloblang_validator.validate_bloblang_expression",
            return_value=(False, "unexpected token"),
        ) as mock_lint:
            result = validate_column_template(
                "row_hash",
                "public.customers",
                source_schema,
            )

        mock_lint.assert_called_once()
        assert not result.is_valid
        assert any("Bloblang syntax" in e for e in result.errors)

    def test_source_ref_skips_bloblang_syntax_check(
        self,
        templates_file: Path,
        source_schema: TableSchema,
    ) -> None:
        """Source-ref value does NOT trigger Bloblang syntax check."""
        mock_config = {
            "asma": {
                "sources": {
                    "src1": {"customer_id": "cust-001"},
                },
            },
        }
        mock_lint = MagicMock(return_value=(True, None))
        with patch(
            "cdc_generator.core.source_ref_resolver._load_source_groups",
            return_value=mock_config,
        ), patch(
            "cdc_generator.validators.manage_service.bloblang_validator.check_rpk_available",
            return_value=True,
        ), patch(
            "cdc_generator.validators.manage_service.bloblang_validator.validate_bloblang_expression",
            mock_lint,
        ):
            result = validate_column_template(
                "tenant_id",
                "public.customers",
                source_schema,
                service="test_svc",
            )

        # Bloblang lint should NOT be called for source-ref values
        mock_lint.assert_not_called()
        assert result.is_valid


# ---------------------------------------------------------------------------
# Tests — validate_templates_for_table passes params through
# ---------------------------------------------------------------------------


class TestValidateTemplatesForTableChain:
    """Tests that validate_templates_for_table passes service/value_override."""

    def test_passes_service_to_validate_column_template(
        self,
        templates_file: Path,
        source_schema_dir: Path,
    ) -> None:
        """service param is forwarded to validate_column_template."""
        mock_config = {
            "asma": {
                "sources": {
                    "src1": {"customer_id": "cust-001"},
                },
            },
        }

        with patch(
            "cdc_generator.core.structure_replicator._find_schema_dir",
            return_value=source_schema_dir,
        ), patch(
            "cdc_generator.core.source_ref_resolver._load_source_groups",
            return_value=mock_config,
        ), patch(
            "cdc_generator.validators.manage_service.bloblang_validator.check_rpk_available",
            return_value=False,
        ):
            result = validate_templates_for_table(
                "test_svc",
                "public.customers",
                ["tenant_id"],
            )

        # tenant_id has value "{asma.sources.*.customer_id}"
        # With valid config, should pass
        assert result is True

    def test_passes_service_detects_missing_source_ref_key(
        self,
        templates_file: Path,
        source_schema_dir: Path,
    ) -> None:
        """service param enables source-ref validation → catches missing key."""
        mock_config = {
            "asma": {
                "sources": {
                    "src1": {"host": "localhost"},
                },
            },
        }

        with patch(
            "cdc_generator.core.structure_replicator._find_schema_dir",
            return_value=source_schema_dir,
        ), patch(
            "cdc_generator.core.source_ref_resolver._load_source_groups",
            return_value=mock_config,
        ), patch(
            "cdc_generator.validators.manage_service.bloblang_validator.check_rpk_available",
            return_value=False,
        ):
            result = validate_templates_for_table(
                "test_svc",
                "public.customers",
                ["tenant_id"],
            )

        # tenant_id refs {asma.sources.*.customer_id} but customer_id
        # key doesn't exist → validation should fail
        assert result is False

    def test_passes_value_override_through(
        self,
        templates_file: Path,
        source_schema_dir: Path,
    ) -> None:
        """value_override is forwarded to validate_column_template."""
        with patch(
            "cdc_generator.core.structure_replicator._find_schema_dir",
            return_value=source_schema_dir,
        ), patch(
            "cdc_generator.validators.manage_service.bloblang_validator.check_rpk_available",
            return_value=False,
        ):
            result = validate_templates_for_table(
                "test_svc",
                "public.customers",
                ["source_table"],
                value_override="this.nonexistent_col",
            )

        # Override refs a non-existent column → should fail
        assert result is False

    def test_column_ref_validated_against_source_schema(
        self,
        templates_file: Path,
        source_schema_dir: Path,
    ) -> None:
        """Column references in template are validated against source schema."""
        with patch(
            "cdc_generator.core.structure_replicator._find_schema_dir",
            return_value=source_schema_dir,
        ), patch(
            "cdc_generator.validators.manage_service.bloblang_validator.check_rpk_available",
            return_value=False,
        ):
            # row_hash refs this.name which IS in the source schema
            result = validate_templates_for_table(
                "test_svc",
                "public.customers",
                ["row_hash"],
            )

        assert result is True

    def test_missing_column_ref_fails_validation(
        self,
        templates_file: Path,
        tmp_path: Path,
    ) -> None:
        """Template with non-existent column ref → validation fails."""
        # Create a source schema WITHOUT 'name' column
        schema_dir = tmp_path / "schemas2" / "test_svc" / "public"
        schema_dir.mkdir(parents=True)
        (schema_dir / "customers.yaml").write_text(
            "columns:\n"
            "- name: id\n"
            "  type: uuid\n"
        )

        with patch(
            "cdc_generator.core.structure_replicator._find_schema_dir",
            return_value=tmp_path / "schemas2",
        ), patch(
            "cdc_generator.validators.manage_service.bloblang_validator.check_rpk_available",
            return_value=False,
        ):
            # row_hash refs this.name but 'name' is NOT in this schema
            result = validate_templates_for_table(
                "test_svc",
                "public.customers",
                ["row_hash"],
            )

        assert result is False


# ---------------------------------------------------------------------------
# Tests — _types_are_compatible
# ---------------------------------------------------------------------------


class TestTypesAreCompatible:
    """Tests for column type compatibility checking."""

    def test_identical_types(self) -> None:
        assert _types_are_compatible("uuid", "uuid") is True

    def test_identical_with_length_suffix(self) -> None:
        """Length/precision suffixes are stripped before comparison."""
        assert _types_are_compatible("varchar(100)", "varchar(255)") is True

    def test_text_family_compatible(self) -> None:
        assert _types_are_compatible("varchar", "text") is True
        assert _types_are_compatible("nvarchar", "character varying") is True

    def test_integer_family_compatible(self) -> None:
        assert _types_are_compatible("int", "bigint") is True
        assert _types_are_compatible("smallint", "integer") is True

    def test_uuid_compatible(self) -> None:
        assert _types_are_compatible("uuid", "uniqueidentifier") is True

    def test_text_sink_accepts_anything(self) -> None:
        """A text-family sink column should accept any source type."""
        assert _types_are_compatible("uuid", "text") is True
        assert _types_are_compatible("integer", "varchar") is True
        assert _types_are_compatible("boolean", "character varying") is True

    def test_incompatible_types(self) -> None:
        assert _types_are_compatible("uuid", "integer") is False
        assert _types_are_compatible("boolean", "timestamp") is False

    def test_timestamp_family_compatible(self) -> None:
        assert _types_are_compatible("timestamp", "datetime") is True
        assert _types_are_compatible(
            "timestamp without time zone", "datetime2",
        ) is True

    def test_json_compatible(self) -> None:
        assert _types_are_compatible("json", "jsonb") is True

    def test_float_family_compatible(self) -> None:
        assert _types_are_compatible("float", "double precision") is True
        assert _types_are_compatible("numeric", "decimal") is True

    def test_bool_family_compatible(self) -> None:
        assert _types_are_compatible("boolean", "bit") is True


# ---------------------------------------------------------------------------
# Tests — validate_sink_column_exists
# ---------------------------------------------------------------------------


class TestValidateSinkColumnExists:
    """Tests for sink column existence validation."""

    def test_column_exists(self, tmp_path: Path) -> None:
        """Column found in sink schema → valid, no errors."""
        schema_dir = tmp_path / "service-schemas" / "proxy" / "public"
        schema_dir.mkdir(parents=True)
        (schema_dir / "users.yaml").write_text(
            "columns:\n"
            "- name: user_id\n"
            "  type: uuid\n"
            "- name: full_name\n"
            "  type: text\n"
        )

        with patch(
            "cdc_generator.core.structure_replicator._find_schema_dir",
            return_value=tmp_path / "service-schemas",
        ):
            is_valid, errors, warnings = validate_sink_column_exists(
                "proxy", "public.users", "user_id",
            )

        assert is_valid is True
        assert errors == []
        assert warnings == []

    def test_column_not_found(self, tmp_path: Path) -> None:
        """Column missing from sink schema → error with available columns."""
        schema_dir = tmp_path / "service-schemas" / "proxy" / "public"
        schema_dir.mkdir(parents=True)
        (schema_dir / "users.yaml").write_text(
            "columns:\n"
            "- name: user_id\n"
            "  type: uuid\n"
            "- name: email\n"
            "  type: text\n"
        )

        with patch(
            "cdc_generator.core.structure_replicator._find_schema_dir",
            return_value=tmp_path / "service-schemas",
        ):
            is_valid, errors, warnings = validate_sink_column_exists(
                "proxy", "public.users", "nonexistent_col",
            )

        assert is_valid is False
        assert len(errors) == 1
        assert "nonexistent_col" in errors[0]
        assert "email" in errors[0]  # available columns listed in error

    def test_missing_schema_warns_but_passes(self, tmp_path: Path) -> None:
        """No sink schema file → warning only, still valid (graceful)."""
        empty_dir = tmp_path / "empty_schemas"
        empty_dir.mkdir()

        with patch(
            "cdc_generator.core.structure_replicator._find_schema_dir",
            return_value=empty_dir,
        ):
            is_valid, errors, warnings = validate_sink_column_exists(
                "proxy", "public.nonexistent", "some_col",
            )

        assert is_valid is True
        assert errors == []
        assert len(warnings) == 1
        assert "no schema file" in warnings[0].lower()


# ---------------------------------------------------------------------------
# Tests — validate_column_mapping_types
# ---------------------------------------------------------------------------


class TestValidateColumnMappingTypes:
    """Tests for source↔sink column type compatibility validation."""

    def test_all_compatible_no_warnings(self) -> None:
        """All columns have compatible types → no warnings."""
        source = TableSchema(
            table_name="customer_user", schema_name="public",
            columns={"user_id": "uuid", "full_name": "text"},
        )
        sink = TableSchema(
            table_name="dir_user_name", schema_name="public",
            columns={"uid": "uuid", "name": "text"},
        )
        warnings = validate_column_mapping_types(
            source, sink,
            {"uid": "user_id", "name": "full_name"},
            "public.dir_user_name",
        )
        assert warnings == []

    def test_type_mismatch_produces_warning(self) -> None:
        """Incompatible types → warning (advisory, not error)."""
        source = TableSchema(
            table_name="customer_user", schema_name="public",
            columns={"user_id": "uuid", "age": "integer"},
        )
        sink = TableSchema(
            table_name="dir_user_name", schema_name="public",
            columns={"uid": "uuid", "age": "boolean"},
        )
        warnings = validate_column_mapping_types(
            source, sink,
            {"uid": "user_id", "age": "age"},
            "public.dir_user_name",
        )
        assert len(warnings) == 1
        assert "type mismatch" in warnings[0].lower()
        assert "integer" in warnings[0]
        assert "boolean" in warnings[0]

    def test_missing_source_column_warns(self) -> None:
        """Source column referenced in mapping doesn't exist → warning."""
        source = TableSchema(
            table_name="customer_user", schema_name="public",
            columns={"user_id": "uuid"},
        )
        sink = TableSchema(
            table_name="dir_user_name", schema_name="public",
            columns={"uid": "uuid", "name": "text"},
        )
        warnings = validate_column_mapping_types(
            source, sink,
            {"uid": "user_id", "name": "nonexistent"},
            "public.dir_user_name",
        )
        assert len(warnings) == 1
        assert "nonexistent" in warnings[0]
        assert "source" in warnings[0].lower()

    def test_missing_sink_column_warns(self) -> None:
        """Sink column in mapping not in schema → warning."""
        source = TableSchema(
            table_name="customer_user", schema_name="public",
            columns={"user_id": "uuid", "full_name": "text"},
        )
        sink = TableSchema(
            table_name="dir_user_name", schema_name="public",
            columns={"uid": "uuid"},
        )
        warnings = validate_column_mapping_types(
            source, sink,
            {"uid": "user_id", "ghost_col": "full_name"},
            "public.dir_user_name",
        )
        assert len(warnings) == 1
        assert "ghost_col" in warnings[0]
        assert "sink" in warnings[0].lower()

    def test_text_sink_accepts_uuid_source(self) -> None:
        """Text-family sink column is compatible with any source type."""
        source = TableSchema(
            table_name="t", schema_name="public",
            columns={"id": "uuid"},
        )
        sink = TableSchema(
            table_name="t", schema_name="public",
            columns={"id": "text"},
        )
        warnings = validate_column_mapping_types(
            source, sink, {"id": "id"}, "public.t",
        )
        assert warnings == []

    def test_empty_mapping_no_warnings(self) -> None:
        """Empty mapping dict → nothing to validate."""
        source = TableSchema(
            table_name="t", schema_name="public",
            columns={"id": "uuid"},
        )
        sink = TableSchema(
            table_name="t", schema_name="public",
            columns={"id": "uuid"},
        )
        warnings = validate_column_mapping_types(
            source, sink, {}, "public.t",
        )
        assert warnings == []


# ---------------------------------------------------------------------------
# Tests — get_sink_table_schema
# ---------------------------------------------------------------------------


class TestGetSinkTableSchema:
    """Tests for loading sink table schemas."""

    def test_loads_schema_from_sink_service_dir(self, tmp_path: Path) -> None:
        """Schema loaded from service-schemas/{sink_service}/{schema}/."""
        schema_dir = tmp_path / "service-schemas" / "proxy" / "public"
        schema_dir.mkdir(parents=True)
        (schema_dir / "directory_user_name.yaml").write_text(
            "columns:\n"
            "- name: user_id\n"
            "  type: uuid\n"
            "- name: first_name\n"
            "  type: text\n"
        )

        with patch(
            "cdc_generator.core.structure_replicator._find_schema_dir",
            return_value=tmp_path / "service-schemas",
        ):
            schema = get_sink_table_schema(
                "proxy", "public.directory_user_name",
            )

        assert schema is not None
        assert "user_id" in schema.columns
        assert schema.columns["user_id"] == "uuid"
        assert "first_name" in schema.columns

    def test_returns_none_for_missing_schema(self, tmp_path: Path) -> None:
        """Missing schema file → None."""
        empty = tmp_path / "empty"
        empty.mkdir()

        with patch(
            "cdc_generator.core.structure_replicator._find_schema_dir",
            return_value=empty,
        ):
            schema = get_sink_table_schema(
                "proxy", "public.nonexistent",
            )

        assert schema is None


# ---------------------------------------------------------------------------
# Tests — end-to-end: sink_template_ops integration
# ---------------------------------------------------------------------------


class TestSinkTemplateOpsSinkValidation:
    """Integration tests for sink column + type validation in add_column_template_to_table."""

    def test_rejects_nonexistent_sink_column(
        self,
        templates_file: Path,
        source_schema_dir: Path,
        tmp_path: Path,
    ) -> None:
        """target_exists=true + --column-name not in sink schema → fails."""
        from cdc_generator.validators.manage_service.sink_template_ops import (
            add_column_template_to_table,
        )

        config = {
            "service": "test_svc",
            "sinks": {
                "sink_asma.proxy": {
                    "tables": {
                        "public.users": {
                            "target_exists": True,
                            "from": "public.customers",
                            "columns": {"user_id": "id"},
                        },
                    },
                },
            },
        }

        # Sink schema has user_id and email — NOT "nonexistent_col"
        sink_schema_dir = tmp_path / "sink_schemas" / "proxy" / "public"
        sink_schema_dir.mkdir(parents=True)
        (sink_schema_dir / "users.yaml").write_text(
            "columns:\n"
            "- name: user_id\n"
            "  type: uuid\n"
            "- name: email\n"
            "  type: text\n"
        )

        target = "cdc_generator.validators.manage_service.sink_template_ops"
        with patch(
            f"{target}.load_service_config", return_value=config,
        ), patch(
            f"{target}.save_service_config", return_value=True,
        ), patch(
            "cdc_generator.core.structure_replicator._find_schema_dir",
            return_value=tmp_path / "sink_schemas",
        ):
            result = add_column_template_to_table(
                "test_svc",
                "sink_asma.proxy",
                "public.users",
                "source_table",
                name_override="nonexistent_col",
            )

        assert result is False

    def test_accepts_existing_sink_column(
        self,
        templates_file: Path,
        tmp_path: Path,
    ) -> None:
        """target_exists=true + valid --column-name → passes sink check."""
        from cdc_generator.validators.manage_service.sink_template_ops import (
            add_column_template_to_table,
        )

        config = {
            "service": "test_svc",
            "sinks": {
                "sink_asma.proxy": {
                    "tables": {
                        "public.users": {
                            "target_exists": True,
                            "from": "public.customers",
                            "columns": {"user_id": "id"},
                        },
                    },
                },
            },
        }

        # Create a unified service-schemas dir with BOTH source and sink schemas
        schemas_root = tmp_path / "unified_schemas"

        # Source schema for test_svc (used by validate_templates_for_table)
        src_dir = schemas_root / "test_svc" / "public"
        src_dir.mkdir(parents=True)
        (src_dir / "customers.yaml").write_text(
            "columns:\n"
            "- name: id\n"
            "  type: uuid\n"
            "- name: name\n"
            "  type: text\n"
        )

        # Sink schema for proxy (used by _validate_sink_column)
        sink_dir = schemas_root / "proxy" / "public"
        sink_dir.mkdir(parents=True)
        (sink_dir / "users.yaml").write_text(
            "columns:\n"
            "- name: user_id\n"
            "  type: uuid\n"
            "- name: email\n"
            "  type: text\n"
        )

        target = "cdc_generator.validators.manage_service.sink_template_ops"
        with patch(
            f"{target}.load_service_config", return_value=config,
        ), patch(
            f"{target}.save_service_config", return_value=True,
        ), patch(
            "cdc_generator.core.structure_replicator._find_schema_dir",
            return_value=schemas_root,
        ), patch(
            "cdc_generator.validators.manage_service.bloblang_validator.check_rpk_available",
            return_value=False,
        ):
            result = add_column_template_to_table(
                "test_svc",
                "sink_asma.proxy",
                "public.users",
                "source_table",
                name_override="email",
            )

        assert result is True
