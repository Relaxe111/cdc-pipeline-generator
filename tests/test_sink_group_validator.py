"""Unit tests for sink group validation helpers."""

from pathlib import Path
from typing import Any
from unittest.mock import patch

from cdc_generator.validators.sink_group_validator import (
    SinkGroupValidationError,
    validate_all_sink_groups,
    validate_sink_file,
    validate_sink_group_compatibility,
    validate_sink_group_references,
    validate_source_ref_format,
)


def _source_groups() -> dict[str, Any]:
    return {
        "asma": {
            "pattern": "db-shared",
            "servers": {
                "default": {"host": "localhost"},
                "analytics": {"host": "analytics.local"},
            },
            "sources": {
                "directory": {
                    "schemas": ["public"],
                },
            },
        },
    }


def test_validate_source_ref_format_accepts_server_name() -> None:
    assert validate_source_ref_format("default") == "default"


def test_validate_source_ref_format_rejects_path_like_value() -> None:
    try:
        validate_source_ref_format("asma/default")
    except SinkGroupValidationError as exc:
        assert "Invalid source_ref" in str(exc)
    else:
        raise AssertionError("Expected SinkGroupValidationError")


def test_validate_sink_group_references_unknown_source_group() -> None:
    sink_group = {
        "source_group": "ghost",
        "servers": {},
    }

    errors = validate_sink_group_references("sink_ghost", sink_group, _source_groups())

    assert errors
    assert "unknown source group" in errors[0]


def test_validate_sink_group_references_unknown_source_server() -> None:
    sink_group = {
        "source_group": "asma",
        "servers": {
            "default": {
                "source_ref": "missing",
            },
        },
    }

    errors = validate_sink_group_references("sink_asma", sink_group, _source_groups())

    assert errors
    assert "references unknown server" in errors[0]


def test_validate_sink_group_compatibility_warns_for_pattern_mismatch() -> None:
    sink_group = {
        "source_group": "asma",
        "pattern": "db-per-tenant",
        "servers": {},
    }

    warnings = validate_sink_group_compatibility("sink_asma", sink_group, _source_groups())

    assert warnings
    assert "differs from source group pattern" in warnings[0]


def test_validate_all_sink_groups_collects_errors_and_warnings() -> None:
    sink_groups = {
        "sink_asma": {
            "inherits": True,
            "servers": {
                "default": {
                    "source_ref": "default",
                },
            },
            "inherited_sources": ["directory"],
        },
        "sink_analytics": {
            "source_group": "asma",
            "pattern": "db-per-tenant",
            "servers": {
                "default": {
                    "host": "localhost",
                    "port": "5432",
                    "user": "postgres",
                    "password": "secret",
                },
            },
            "sources": {},
        },
    }

    errors, warnings = validate_all_sink_groups(sink_groups, _source_groups())

    assert errors == []
    assert warnings


@patch("cdc_generator.helpers.yaml_loader.load_yaml_file")
@patch("cdc_generator.helpers.helpers_sink_groups.load_sink_groups")
def test_validate_sink_file_success(
    mock_load_sink_groups: Any,
    mock_load_yaml: Any,
    tmp_path: Path,
) -> None:
    sink_file = tmp_path / "sink-groups.yaml"
    source_file = tmp_path / "source-groups.yaml"

    mock_load_sink_groups.return_value = {
        "sink_asma": {
            "inherits": True,
            "servers": {
                "default": {
                    "source_ref": "default",
                },
            },
            "inherited_sources": ["directory"],
        },
    }
    mock_load_yaml.return_value = _source_groups()

    valid, errors, warnings = validate_sink_file(sink_file, source_file)

    assert valid is True
    assert errors == []
    assert warnings == []


@patch("cdc_generator.helpers.helpers_sink_groups.load_sink_groups")
def test_validate_sink_file_handles_load_error(
    mock_load_sink_groups: Any,
    tmp_path: Path,
) -> None:
    sink_file = tmp_path / "sink-groups.yaml"
    source_file = tmp_path / "source-groups.yaml"

    mock_load_sink_groups.side_effect = ValueError("bad yaml")

    valid, errors, warnings = validate_sink_file(sink_file, source_file)

    assert valid is False
    assert errors
    assert warnings == []
