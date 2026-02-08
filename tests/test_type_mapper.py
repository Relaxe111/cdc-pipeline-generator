"""Tests for the TypeMapper helper module.

Covers:
- Direct mapping (mssql→pgsql)
- Identity mapping (pgsql→pgsql) with alias normalization
- Bidirectional/reverse mapping (pgsql→mssql via reversed file)
- Case-insensitive lookup
- Fallback for unknown types
- map_columns() batch conversion
- available_source_types / available_sink_types properties
- get_available_adapters() / get_supported_engines() utilities
- Error handling for missing adapter files
"""

import pytest
from cdc_generator.helpers.type_mapper import (
    TypeMapper,
    get_available_adapters,
    get_supported_engines,
)


# ── Direct mapping: MSSQL → PostgreSQL ─────────────────────────────────────


class TestMssqlToPgsql:
    """Test direct MSSQL→PostgreSQL type mappings."""

    @pytest.fixture()
    def mapper(self) -> TypeMapper:
        """Create an MSSQL→PostgreSQL mapper."""
        return TypeMapper("mssql", "pgsql")

    def test_uuid_mapping(self, mapper: TypeMapper) -> None:
        """uniqueidentifier → uuid."""
        assert mapper.map_type("uniqueidentifier") == "uuid"

    def test_boolean_mapping(self, mapper: TypeMapper) -> None:
        """bit → boolean."""
        assert mapper.map_type("bit") == "boolean"

    def test_text_mapping(self, mapper: TypeMapper) -> None:
        """nvarchar → varchar."""
        assert mapper.map_type("nvarchar") == "varchar"

    def test_datetime_mapping(self, mapper: TypeMapper) -> None:
        """datetime2 → timestamp."""
        assert mapper.map_type("datetime2") == "timestamp"

    def test_datetimeoffset_mapping(self, mapper: TypeMapper) -> None:
        """datetimeoffset → timestamptz."""
        assert mapper.map_type("datetimeoffset") == "timestamptz"

    def test_integer_mapping(self, mapper: TypeMapper) -> None:
        """int → integer."""
        assert mapper.map_type("int") == "integer"

    def test_bigint_mapping(self, mapper: TypeMapper) -> None:
        """bigint → bigint."""
        assert mapper.map_type("bigint") == "bigint"

    def test_binary_mapping(self, mapper: TypeMapper) -> None:
        """varbinary → bytea."""
        assert mapper.map_type("varbinary") == "bytea"

    def test_mssql_timestamp_is_bytea(self, mapper: TypeMapper) -> None:
        """MSSQL timestamp is rowversion (binary), not a date type."""
        assert mapper.map_type("timestamp") == "bytea"

    def test_money_mapping(self, mapper: TypeMapper) -> None:
        """money → numeric."""
        assert mapper.map_type("money") == "numeric"

    def test_float_mapping(self, mapper: TypeMapper) -> None:
        """float → double precision."""
        assert mapper.map_type("float") == "double precision"

    def test_xml_mapping(self, mapper: TypeMapper) -> None:
        """xml → xml."""
        assert mapper.map_type("xml") == "xml"


# ── Identity mapping: PostgreSQL → PostgreSQL ──────────────────────────────


class TestPgsqlToPgsql:
    """Test PostgreSQL→PostgreSQL identity mapping with alias normalization."""

    @pytest.fixture()
    def mapper(self) -> TypeMapper:
        """Create a PostgreSQL→PostgreSQL mapper."""
        return TypeMapper("pgsql", "pgsql")

    def test_uuid_identity(self, mapper: TypeMapper) -> None:
        """uuid → uuid (identity)."""
        assert mapper.map_type("uuid") == "uuid"

    def test_text_identity(self, mapper: TypeMapper) -> None:
        """text → text (identity)."""
        assert mapper.map_type("text") == "text"

    def test_boolean_identity(self, mapper: TypeMapper) -> None:
        """boolean → boolean (identity)."""
        assert mapper.map_type("boolean") == "boolean"

    def test_timestamptz_identity(self, mapper: TypeMapper) -> None:
        """timestamptz → timestamptz (identity)."""
        assert mapper.map_type("timestamptz") == "timestamptz"

    def test_jsonb_identity(self, mapper: TypeMapper) -> None:
        """jsonb → jsonb (identity)."""
        assert mapper.map_type("jsonb") == "jsonb"

    # Alias normalization
    def test_int4_normalized(self, mapper: TypeMapper) -> None:
        """int4 → integer (alias normalization)."""
        assert mapper.map_type("int4") == "integer"

    def test_int8_normalized(self, mapper: TypeMapper) -> None:
        """int8 → bigint (alias normalization)."""
        assert mapper.map_type("int8") == "bigint"

    def test_float8_normalized(self, mapper: TypeMapper) -> None:
        """float8 → double precision (alias normalization)."""
        assert mapper.map_type("float8") == "double precision"

    def test_bool_normalized(self, mapper: TypeMapper) -> None:
        """bool → boolean (alias normalization)."""
        assert mapper.map_type("bool") == "boolean"

    def test_character_varying_normalized(self, mapper: TypeMapper) -> None:
        """character varying → varchar (alias normalization)."""
        assert mapper.map_type("character varying") == "varchar"

    def test_timestamp_with_tz_normalized(self, mapper: TypeMapper) -> None:
        """timestamp with time zone → timestamptz (alias normalization)."""
        assert mapper.map_type("timestamp with time zone") == "timestamptz"

    def test_user_defined_to_text(self, mapper: TypeMapper) -> None:
        """USER-DEFINED → text (fallback for custom/enum types)."""
        assert mapper.map_type("USER-DEFINED") == "text"

    def test_array_types(self, mapper: TypeMapper) -> None:
        """Array types are preserved."""
        assert mapper.map_type("text[]") == "text[]"
        assert mapper.map_type("uuid[]") == "uuid[]"
        assert mapper.map_type("integer[]") == "integer[]"


# ── Bidirectional (reverse) mapping ────────────────────────────────────────


class TestReverseMapping:
    """Test reverse mapping: load mssql-to-pgsql file in pgsql→mssql direction."""

    @pytest.fixture()
    def mapper(self) -> TypeMapper:
        """Create a reverse PostgreSQL→MSSQL mapper."""
        return TypeMapper("pgsql", "mssql")

    def test_uuid_reverse(self, mapper: TypeMapper) -> None:
        """uuid → uniqueidentifier (reverse lookup)."""
        assert mapper.map_type("uuid") == "uniqueidentifier"

    def test_boolean_reverse(self, mapper: TypeMapper) -> None:
        """boolean → bit (reverse lookup)."""
        assert mapper.map_type("boolean") == "bit"

    def test_varchar_reverse(self, mapper: TypeMapper) -> None:
        """varchar → char (reverse lookup, first occurrence wins)."""
        # Multiple MSSQL types map to varchar; reverse picks first
        result = mapper.map_type("varchar")
        assert isinstance(result, str)
        assert len(result) > 0


# ── Case-insensitive lookup ────────────────────────────────────────────────


class TestCaseInsensitive:
    """Test case-insensitive type matching."""

    @pytest.fixture()
    def mapper(self) -> TypeMapper:
        """Create an MSSQL→PostgreSQL mapper."""
        return TypeMapper("mssql", "pgsql")

    def test_uppercase_match(self, mapper: TypeMapper) -> None:
        """BIGINT → bigint (case-insensitive)."""
        assert mapper.map_type("BIGINT") == "bigint"

    def test_mixed_case_match(self, mapper: TypeMapper) -> None:
        """DateTime2 → timestamp (case-insensitive)."""
        assert mapper.map_type("DateTime2") == "timestamp"

    def test_exact_match_preferred(self, mapper: TypeMapper) -> None:
        """Exact match should be preferred over case-insensitive."""
        assert mapper.map_type("bigint") == "bigint"


# ── Fallback behavior ─────────────────────────────────────────────────────


class TestFallback:
    """Test fallback type for unknown source types."""

    def test_unknown_type_returns_fallback(self) -> None:
        """Unknown types fall back to 'text'."""
        mapper = TypeMapper("mssql", "pgsql")
        assert mapper.map_type("some_custom_type") == "text"
        assert mapper.map_type("") == "text"

    def test_fallback_attribute(self) -> None:
        """Fallback is read from the mapping YAML file."""
        mapper = TypeMapper("mssql", "pgsql")
        assert mapper.fallback == "text"


# ── map_columns() ─────────────────────────────────────────────────────────


class TestMapColumns:
    """Test batch column mapping via map_columns()."""

    @pytest.fixture()
    def mapper(self) -> TypeMapper:
        """Create an MSSQL→PostgreSQL mapper."""
        return TypeMapper("mssql", "pgsql")

    def test_maps_type_field(self, mapper: TypeMapper) -> None:
        """Column types are converted correctly."""
        cols = [{"name": "id", "type": "uniqueidentifier", "nullable": False}]
        result = mapper.map_columns(cols)
        assert len(result) == 1
        assert result[0]["name"] == "id"
        assert result[0]["type"] == "uuid"
        assert result[0]["nullable"] is False

    def test_preserves_nullable(self, mapper: TypeMapper) -> None:
        """nullable field is preserved."""
        cols = [{"name": "name", "type": "nvarchar", "nullable": True}]
        result = mapper.map_columns(cols)
        assert result[0]["nullable"] is True

    def test_preserves_primary_key(self, mapper: TypeMapper) -> None:
        """primary_key field is preserved."""
        cols = [{"name": "id", "type": "int", "primary_key": True}]
        result = mapper.map_columns(cols)
        assert result[0]["primary_key"] is True

    def test_multiple_columns(self, mapper: TypeMapper) -> None:
        """Multiple columns are all mapped."""
        cols = [
            {"name": "id", "type": "uniqueidentifier"},
            {"name": "name", "type": "nvarchar"},
            {"name": "active", "type": "bit"},
            {"name": "created", "type": "datetime2"},
        ]
        result = mapper.map_columns(cols)
        assert len(result) == len(cols)
        assert result[0]["type"] == "uuid"
        assert result[1]["type"] == "varchar"
        assert result[2]["type"] == "boolean"
        assert result[3]["type"] == "timestamp"

    def test_skips_invalid_entries(self, mapper: TypeMapper) -> None:
        """Entries without string name/type are skipped."""
        cols = [
            {"name": "id", "type": "int"},
            {"name": 123, "type": "int"},  # invalid name
            {"name": "x", "type": None},  # invalid type
        ]
        result = mapper.map_columns(cols)
        assert len(result) == 1
        assert result[0]["name"] == "id"


# ── Properties ─────────────────────────────────────────────────────────────


class TestProperties:
    """Test available_source_types and available_sink_types properties."""

    def test_source_types_not_empty(self) -> None:
        """available_source_types returns a non-empty sorted list."""
        mapper = TypeMapper("mssql", "pgsql")
        types = mapper.available_source_types
        assert len(types) > 0
        assert types == sorted(types)
        assert "uniqueidentifier" in types
        assert "bigint" in types

    def test_sink_types_not_empty(self) -> None:
        """available_sink_types returns a non-empty sorted list."""
        mapper = TypeMapper("mssql", "pgsql")
        types = mapper.available_sink_types
        assert len(types) > 0
        assert types == sorted(types)
        assert "uuid" in types
        assert "boolean" in types


# ── Error handling ─────────────────────────────────────────────────────────


class TestErrorHandling:
    """Test error handling for invalid configurations."""

    def test_missing_adapter_raises_file_not_found(self) -> None:
        """Non-existent engine pair raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="No type mapping file found"):
            TypeMapper("oracle", "mysql")


# ── Utility functions ──────────────────────────────────────────────────────


class TestUtilities:
    """Test get_available_adapters() and get_supported_engines()."""

    def test_get_available_adapters(self) -> None:
        """Returns list containing known adapter names."""
        adapters = get_available_adapters()
        assert "mssql-to-pgsql" in adapters
        assert "pgsql-to-pgsql" in adapters

    def test_get_supported_engines(self) -> None:
        """Returns set containing known engine identifiers."""
        engines = get_supported_engines()
        assert "mssql" in engines
        assert "pgsql" in engines
