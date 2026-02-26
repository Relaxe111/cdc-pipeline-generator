"""End-to-end flow: type definitions → load/query → completions.

Tests the production code path:
1. Write a type-definitions YAML file on disk
2. Call ``load_type_definitions`` / ``get_all_type_names`` and verify results
3. Verify graceful degradation when files are missing

This validates the integration between:
- Type definition file reading  (``load_type_definitions``)
- Flat type list generation     (``get_all_type_names``)
- Completion system readiness   (correct structure)
"""

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml

from cdc_generator.validators.manage_service_schema.type_definitions import (
    get_all_type_names,
    load_type_definitions,
)

pytestmark = pytest.mark.cli


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PGSQL_DEFINITIONS: dict[str, Any] = {
    "categories": {
        "numeric": {
            "types": ["bigint", "integer", "smallint", "numeric", "decimal", "real", "double precision"],
            "defaults": ["default_0", "default_1"],
        },
        "text": {
            "types": ["text", "varchar", "character varying", "char"],
            "defaults": ["default_empty", "default_null"],
        },
        "date_time": {
            "types": ["date", "timestamp", "timestamptz", "time", "interval"],
            "defaults": ["default_now", "default_current_timestamp"],
        },
        "uuid": {
            "types": ["uuid"],
            "defaults": ["default_gen_random_uuid"],
        },
        "boolean": {
            "types": ["boolean", "bool"],
            "defaults": ["default_false", "default_true"],
        },
        "json": {
            "types": ["json", "jsonb"],
            "defaults": ["default_empty_json"],
        },
    },
    "type_defaults": {
        "uuid": ["default_uuid", "default_gen_random_uuid"],
        "timestamp": ["default_now", "default_current_timestamp"],
        "timestamptz": ["default_now", "default_current_timestamp"],
        "boolean": ["default_false", "default_true"],
    },
}

_MSSQL_DEFINITIONS: dict[str, Any] = {
    "categories": {
        "numeric": {
            "types": ["bigint", "int", "smallint", "tinyint", "decimal", "numeric", "float", "real", "money"],
            "defaults": ["default_0", "default_1"],
        },
        "text": {
            "types": ["varchar", "nvarchar", "char", "nchar", "text", "ntext"],
            "defaults": ["default_empty", "default_null"],
        },
        "date_time": {
            "types": ["date", "datetime", "datetime2", "datetimeoffset", "time"],
            "defaults": ["default_getdate", "default_getutcdate"],
        },
        "uuid": {
            "types": ["uniqueidentifier"],
            "defaults": ["default_newid"],
        },
        "boolean": {
            "types": ["bit"],
            "defaults": ["default_0", "default_1"],
        },
        "binary": {
            "types": ["binary", "varbinary", "image"],
            "defaults": [],
        },
    },
    "type_defaults": {
        "uniqueidentifier": ["default_newid", "default_newsequentialid"],
        "datetime": ["default_getdate", "default_getutcdate"],
        "datetime2": ["default_sysdatetime", "default_sysutcdatetime"],
        "bit": ["default_0", "default_1"],
    },
}


def _write_definitions(root: Path, engine: str, content: dict[str, Any]) -> Path:
    """Write a type definitions YAML file under the standard path."""
    defs_dir = root / "services" / "_schemas" / "_definitions"
    defs_dir.mkdir(parents=True, exist_ok=True)
    defs_file = defs_dir / f"{engine}.yaml"
    defs_file.write_text(yaml.dump(content, default_flow_style=False))
    return defs_file


def _patch_defs_dir(root: Path) -> Any:
    """Patch _get_definitions_dir to point at the isolated project."""
    return patch(
        "cdc_generator.validators.manage_service_schema.type_definitions._get_definitions_dir",
        return_value=root / "services" / "_schemas" / "_definitions",
    )


# ---------------------------------------------------------------------------
# Tests: load_type_definitions (production code)
# ---------------------------------------------------------------------------


class TestLoadTypeDefinitions:
    """Verify load_type_definitions reads and parses YAML correctly."""

    def test_pgsql_definitions_loaded_with_all_categories(
        self, isolated_project: Path,
    ) -> None:
        """PostgreSQL definitions are loaded with correct category keys."""
        _write_definitions(isolated_project, "pgsql", _PGSQL_DEFINITIONS)

        with _patch_defs_dir(isolated_project):
            result = load_type_definitions("pgsql")

        assert result is not None
        assert "numeric" in result
        assert "uuid" in result
        assert "text" in result
        assert "date_time" in result

    def test_mssql_definitions_loaded_with_mssql_types(
        self, isolated_project: Path,
    ) -> None:
        """MSSQL definitions contain MSSQL-specific types."""
        _write_definitions(isolated_project, "mssql", _MSSQL_DEFINITIONS)

        with _patch_defs_dir(isolated_project):
            result = load_type_definitions("mssql")

        assert result is not None
        assert "uniqueidentifier" in result.get("uuid", [])
        assert "nvarchar" in result.get("text", [])
        assert "datetime2" in result.get("date_time", [])
        assert "bit" in result.get("boolean", [])

    def test_missing_definitions_file_returns_none(
        self, isolated_project: Path,
    ) -> None:
        """Missing definitions file → None (graceful degradation)."""
        with _patch_defs_dir(isolated_project):
            result = load_type_definitions("pgsql")

        assert result is None

    def test_malformed_yaml_returns_none(
        self, isolated_project: Path,
    ) -> None:
        """YAML without 'categories' key → None."""
        defs_dir = isolated_project / "services" / "_schemas" / "_definitions"
        defs_dir.mkdir(parents=True, exist_ok=True)
        (defs_dir / "pgsql.yaml").write_text("not_categories: {}\n")

        with _patch_defs_dir(isolated_project):
            result = load_type_definitions("pgsql")

        assert result is None


# ---------------------------------------------------------------------------
# Tests: get_all_type_names (production code)
# ---------------------------------------------------------------------------


class TestGetAllTypeNames:
    """Verify get_all_type_names returns a flat sorted list."""

    def test_pgsql_all_types_includes_postgres_specific(
        self, isolated_project: Path,
    ) -> None:
        """Flat list includes PG-specific types like timestamptz, jsonb, uuid."""
        _write_definitions(isolated_project, "pgsql", _PGSQL_DEFINITIONS)

        with _patch_defs_dir(isolated_project):
            all_types = get_all_type_names("pgsql")

        assert isinstance(all_types, list)
        assert len(all_types) > 0
        assert all_types == sorted(all_types), "Should be sorted"
        assert "timestamptz" in all_types
        assert "jsonb" in all_types
        assert "uuid" in all_types
        assert "text" in all_types

    def test_mssql_all_types_includes_mssql_specific(
        self, isolated_project: Path,
    ) -> None:
        """Flat list includes MSSQL-specific types."""
        _write_definitions(isolated_project, "mssql", _MSSQL_DEFINITIONS)

        with _patch_defs_dir(isolated_project):
            all_types = get_all_type_names("mssql")

        assert "uniqueidentifier" in all_types
        assert "nvarchar" in all_types
        assert "datetime2" in all_types
        assert "datetimeoffset" in all_types

    def test_missing_engine_returns_empty_list(
        self, isolated_project: Path,
    ) -> None:
        """Unknown engine with no definitions file → empty list."""
        with _patch_defs_dir(isolated_project):
            result = get_all_type_names("oracle")

        assert result == []

    def test_types_are_deduplicated(
        self, isolated_project: Path,
    ) -> None:
        """Duplicate types across categories are deduplicated."""
        dupe_defs: dict[str, Any] = {
            "categories": {
                "a": {"types": ["bigint", "text"]},
                "b": {"types": ["text", "uuid"]},
            },
        }
        _write_definitions(isolated_project, "pgsql", dupe_defs)

        with _patch_defs_dir(isolated_project):
            all_types = get_all_type_names("pgsql")

        assert all_types.count("text") == 1
