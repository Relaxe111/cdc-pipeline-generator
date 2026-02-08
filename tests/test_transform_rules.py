"""Tests for transform rules loader."""

from pathlib import Path

import pytest  # type: ignore[import-not-found]

from cdc_generator.core.transform_rules import (
    OutputColumn,
    TransformCondition,
    TransformRule,
    _parse_conditions,
    _parse_output_column,
    _parse_single_rule,
    clear_cache,
    get_rule,
    get_rules,
    list_rule_keys,
    set_rules_path,
    validate_rule_reference,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_cache() -> None:  # type: ignore[misc]
    """Clear rule cache before each test."""
    clear_cache()


@pytest.fixture()
def rules_file(tmp_path: Path) -> Path:
    """Create a temporary transform-rules.yaml for testing."""
    content = """\
rules:
  user_class_splitter:
    type: row_multiplier
    description: Split by boolean flags
    output_column:
      name: _user_class
      type: text
      not_null: true
    conditions:
      - when: this.Patient == true
        value: '"Patient"'
      - when: this.Ansatt == true
        value: '"Ansatt"'
    on_no_match: drop

  active_filter:
    type: filter
    description: Keep active rows
    conditions:
      - when: this.is_active == true
    on_no_match: drop

  priority_label:
    type: conditional_column
    description: Map priority to label
    output_column:
      name: _priority_label
      type: text
      not_null: true
    conditions:
      - when: this.priority > 3
        value: '"high"'
      - when: this.priority > 1
        value: '"medium"'
    default_value: '"low"'
    on_no_match: default
"""
    file_path = tmp_path / "transform-rules.yaml"
    file_path.write_text(content)
    set_rules_path(file_path)
    return file_path


# ---------------------------------------------------------------------------
# Unit tests: parsing helpers
# ---------------------------------------------------------------------------


class TestParseOutputColumn:
    """Tests for _parse_output_column."""

    def test_valid(self) -> None:
        raw = {"name": "_col", "type": "text", "not_null": True}
        result = _parse_output_column("test", raw)
        assert result is not None
        assert result.name == "_col"
        assert result.column_type == "text"
        assert result.not_null is True

    def test_missing_name(self) -> None:
        raw = {"type": "text"}
        result = _parse_output_column("test", raw)
        assert result is None

    def test_not_dict(self) -> None:
        result = _parse_output_column("test", "string")
        assert result is None


class TestParseConditions:
    """Tests for _parse_conditions."""

    def test_valid_with_values(self) -> None:
        raw = [
            {"when": "this.x == true", "value": '"yes"'},
            {"when": "this.y == true", "value": '"no"'},
        ]
        result = _parse_conditions("test", raw, require_value=True)
        assert result is not None
        assert len(result) == 2
        assert result[0].when == "this.x == true"
        assert result[0].value == '"yes"'

    def test_valid_without_values(self) -> None:
        """Filter conditions don't require value."""
        raw = [{"when": "this.is_active == true"}]
        result = _parse_conditions("test", raw, require_value=False)
        assert result is not None
        assert len(result) == 1
        assert result[0].value is None

    def test_missing_value_when_required(self) -> None:
        raw = [{"when": "this.x == true"}]  # no value
        result = _parse_conditions("test", raw, require_value=True)
        assert result is None

    def test_missing_when(self) -> None:
        raw = [{"value": '"x"'}]  # no when
        result = _parse_conditions("test", raw, require_value=False)
        assert result is None

    def test_empty_list(self) -> None:
        result = _parse_conditions("test", [], require_value=False)
        assert result is None

    def test_not_list(self) -> None:
        result = _parse_conditions("test", "string", require_value=False)
        assert result is None


class TestParseSingleRule:
    """Tests for _parse_single_rule."""

    def test_row_multiplier(self) -> None:
        raw = {
            "type": "row_multiplier",
            "description": "test",
            "output_column": {"name": "_col", "type": "text"},
            "conditions": [
                {"when": "this.x == true", "value": '"X"'},
            ],
            "on_no_match": "drop",
        }
        result = _parse_single_rule("test", raw)
        assert result is not None
        assert result.rule_type == "row_multiplier"
        assert result.output_column is not None
        assert len(result.conditions) == 1

    def test_filter(self) -> None:
        raw = {
            "type": "filter",
            "description": "test filter",
            "conditions": [{"when": "this.active == true"}],
            "on_no_match": "drop",
        }
        result = _parse_single_rule("test", raw)
        assert result is not None
        assert result.rule_type == "filter"
        assert result.output_column is None

    def test_conditional_column(self) -> None:
        raw = {
            "type": "conditional_column",
            "description": "test",
            "output_column": {"name": "_label", "type": "text"},
            "conditions": [
                {"when": "this.x > 5", "value": '"high"'},
            ],
            "default_value": '"low"',
            "on_no_match": "default",
        }
        result = _parse_single_rule("test", raw)
        assert result is not None
        assert result.rule_type == "conditional_column"
        assert result.default_value == '"low"'
        assert result.on_no_match == "default"

    def test_invalid_type(self) -> None:
        raw = {
            "type": "invalid_type",
            "conditions": [{"when": "x"}],
        }
        result = _parse_single_rule("test", raw)
        assert result is None

    def test_missing_output_for_multiplier(self) -> None:
        raw = {
            "type": "row_multiplier",
            "conditions": [
                {"when": "this.x == true", "value": '"X"'},
            ],
        }
        result = _parse_single_rule("test", raw)
        assert result is None

    def test_not_dict(self) -> None:
        result = _parse_single_rule("test", "string")
        assert result is None

    def test_frozen_dataclass(self) -> None:
        raw = {
            "type": "filter",
            "conditions": [{"when": "this.x == true"}],
        }
        result = _parse_single_rule("test", raw)
        assert result is not None
        with pytest.raises(AttributeError):
            result.key = "changed"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Integration tests: loading from file
# ---------------------------------------------------------------------------


class TestGetRules:
    """Tests for loading rules from YAML file."""

    def test_load_all_rules(self, rules_file: Path) -> None:
        rules = get_rules()
        assert len(rules) == 3
        assert "user_class_splitter" in rules
        assert "active_filter" in rules
        assert "priority_label" in rules

    def test_get_rule_found(self, rules_file: Path) -> None:
        rule = get_rule("user_class_splitter")
        assert rule is not None
        assert rule.rule_type == "row_multiplier"
        assert len(rule.conditions) == 2

    def test_get_rule_not_found(self, rules_file: Path) -> None:
        rule = get_rule("nonexistent")
        assert rule is None

    def test_list_keys(self, rules_file: Path) -> None:
        keys = list_rule_keys()
        assert keys == ["active_filter", "priority_label", "user_class_splitter"]

    def test_validate_valid(self, rules_file: Path) -> None:
        assert validate_rule_reference("active_filter") is None

    def test_validate_invalid(self, rules_file: Path) -> None:
        error = validate_rule_reference("nonexistent")
        assert error is not None
        assert "nonexistent" in error

    def test_caching(self, rules_file: Path) -> None:
        r1 = get_rules()
        r2 = get_rules()
        assert r1 is r2

    def test_file_not_found(self, tmp_path: Path) -> None:
        set_rules_path(tmp_path / "nonexistent.yaml")
        rules = get_rules()
        assert rules == {}

    def test_missing_root_key(self, tmp_path: Path) -> None:
        file_path = tmp_path / "bad.yaml"
        file_path.write_text("other_key: value\n")
        set_rules_path(file_path)
        rules = get_rules()
        assert rules == {}
