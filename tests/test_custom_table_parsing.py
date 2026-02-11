"""Unit tests for custom sink table column parsing.

Covers _parse_column_spec and _parse_multiple_columns from
service_handlers_sink_custom.py.
"""


from cdc_generator.cli.service_handlers_sink_custom import (
    _parse_column_spec,
    _parse_multiple_columns,
)

# ═══════════════════════════════════════════════════════════════════════════
# _parse_column_spec
# ═══════════════════════════════════════════════════════════════════════════


class TestParseColumnSpec:
    """Tests for single column spec parsing."""

    def test_basic_name_type(self) -> None:
        """Parses name:type correctly."""
        result = _parse_column_spec("id:uuid")
        assert result is not None
        assert result["name"] == "id"
        assert result["type"] == "uuid"

    def test_pk_modifier(self) -> None:
        """Parses :pk modifier."""
        result = _parse_column_spec("id:uuid:pk")
        assert result is not None
        assert result["primary_key"] is True
        assert result["nullable"] is False

    def test_not_null_modifier(self) -> None:
        """Parses :not_null modifier."""
        result = _parse_column_spec("name:text:not_null")
        assert result is not None
        assert result["nullable"] is False

    def test_nullable_modifier(self) -> None:
        """Parses :nullable modifier."""
        result = _parse_column_spec("notes:text:nullable")
        assert result is not None
        assert result["nullable"] is True

    def test_default_now(self) -> None:
        """Parses :default_now modifier."""
        result = _parse_column_spec("created_at:timestamptz:default_now")
        assert result is not None
        assert result["default"] == "now()"

    def test_default_gen_random_uuid(self) -> None:
        """Parses :gen_random_uuid modifier."""
        result = _parse_column_spec("id:uuid:gen_random_uuid")
        assert result is not None
        assert result["default"] == "gen_random_uuid()"

    def test_multiple_modifiers(self) -> None:
        """Multiple modifiers combined."""
        result = _parse_column_spec("created_at:timestamptz:not_null:default_now")
        assert result is not None
        assert result["nullable"] is False
        assert result["default"] == "now()"

    def test_invalid_too_few_parts(self) -> None:
        """Returns None for spec with no type."""
        result = _parse_column_spec("onlyname")
        assert result is None

    def test_empty_name_returns_none(self) -> None:
        """Returns None when column name is empty."""
        result = _parse_column_spec(":uuid")
        assert result is None

    def test_nonstandard_type_still_accepted(self) -> None:
        """Non-standard type accepted with warning."""
        result = _parse_column_spec("data:custom_type")
        assert result is not None
        assert result["type"] == "custom_type"

    def test_unknown_modifier_ignored(self) -> None:
        """Unknown modifier ignored with warning."""
        result = _parse_column_spec("id:uuid:unknown_thing")
        assert result is not None
        assert "unknown_thing" not in result

    def test_type_normalized_lowercase(self) -> None:
        """Type is lowercased."""
        result = _parse_column_spec("id:UUID")
        assert result is not None
        assert result["type"] == "uuid"


# ═══════════════════════════════════════════════════════════════════════════
# _parse_multiple_columns
# ═══════════════════════════════════════════════════════════════════════════


class TestParseMultipleColumns:
    """Tests for multiple column spec parsing."""

    def test_parses_valid_specs(self) -> None:
        """Parses multiple valid specs."""
        result = _parse_multiple_columns([
            "id:uuid:pk", "name:text:not_null", "data:jsonb",
        ])
        assert result is not None
        assert len(result) == 3

    def test_returns_none_on_invalid_spec(self) -> None:
        """Returns None if any spec is invalid."""
        result = _parse_multiple_columns([
            "id:uuid:pk", "badspec",
        ])
        assert result is None

    def test_detects_duplicate_names(self) -> None:
        """Returns None on duplicate column names."""
        result = _parse_multiple_columns([
            "id:uuid:pk", "id:text",
        ])
        assert result is None

    def test_empty_list_returns_none(self) -> None:
        """Returns None for empty column list."""
        result = _parse_multiple_columns([])
        assert result is None

    def test_no_pk_still_accepted(self) -> None:
        """Returns columns even without PK (just warns)."""
        result = _parse_multiple_columns([
            "name:text", "age:integer",
        ])
        assert result is not None
        assert len(result) == 2
