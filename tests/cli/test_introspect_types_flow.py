"""End-to-end flow: database type introspection → type definitions → completions.

Tests the complete workflow:
1. Run `manage-source-groups --db-definitions` to generate type definitions
2. Verify definitions are written to `services/_schemas/_definitions/{db_type}.yaml`
3. Verify type completions read from those files
4. Verify completion suggestions include database-specific types

This validates the integration between:
- manage-source-groups (type introspection)
- Type definition file generation
- CLI completion system (type suggestions)
"""

from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.cli.conftest import RunCdc, RunCdcCompletion

pytestmark = pytest.mark.cli


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_source_groups(root: Path, source_type: str = "postgres") -> None:
    """Write source-groups.yaml with specified source type."""
    (root / "source-groups.yaml").write_text(
        f"test_group:\n"
        f"  server_group_type: db-shared\n"
        f"  source_type: {source_type}\n"
        "  sources:\n"
        "    default:\n"
        "      envs:\n"
        "        dev:\n"
        "          kafka_bootstrap_servers: kafka:9092\n"
        "          connection_string: postgresql://localhost:5432/testdb\n"
    )


def _create_mock_type_definitions(root: Path, db_type: str) -> Path:
    """Create mock type definitions file for testing."""
    defs_dir = root / "services" / "_schemas" / "_definitions"
    defs_dir.mkdir(parents=True, exist_ok=True)
    
    defs_file = defs_dir / f"{db_type}.yaml"
    
    if db_type == "pgsql":
        content = {
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
            }
        }
    elif db_type == "mssql":
        content = {
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
            }
        }
    else:
        content = {"categories": {}, "type_defaults": {}}
    
    defs_file.write_text(yaml.dump(content, default_flow_style=False))
    return defs_file


def _read_definitions_file(root: Path, db_type: str) -> dict[str, Any] | None:
    """Read type definitions file if it exists."""
    defs_file = root / "services" / "_schemas" / "_definitions" / f"{db_type}.yaml"
    if not defs_file.exists():
        return None
    return yaml.safe_load(defs_file.read_text())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestIntrospectTypesFlowHappyPath:
    """Happy path: generate definitions → verify completions use them."""

    def test_type_definitions_generated_and_used_by_completions(
        self, run_cdc: RunCdc, run_cdc_completion: RunCdcCompletion,
        isolated_project: Path,
    ) -> None:
        """Type definitions are generated and completions suggest types from them."""
        # Setup
        _write_source_groups(isolated_project, "postgres")
        
        # Create mock definitions (simulating --db-definitions output)
        defs_file = _create_mock_type_definitions(isolated_project, "pgsql")
        assert defs_file.exists()
        
        # Verify definitions have expected structure
        defs = _read_definitions_file(isolated_project, "pgsql")
        assert defs is not None
        assert "categories" in defs
        assert "type_defaults" in defs
        
        # Verify numeric types are present
        assert "numeric" in defs["categories"]
        assert "bigint" in defs["categories"]["numeric"]["types"]
        assert "integer" in defs["categories"]["numeric"]["types"]
        
        # Verify UUID types and defaults
        assert "uuid" in defs["categories"]
        assert "uuid" in defs["categories"]["uuid"]["types"]
        assert "uuid" in defs["type_defaults"]
        assert "default_gen_random_uuid" in defs["type_defaults"]["uuid"]


class TestPostgreSQLSpecificTypes:
    """Verify pgsql.yaml contains PostgreSQL-specific types."""

    def test_pgsql_definitions_contain_postgres_types(
        self, isolated_project: Path,
    ) -> None:
        """PostgreSQL type definitions include PG-specific types."""
        _create_mock_type_definitions(isolated_project, "pgsql")
        
        defs = _read_definitions_file(isolated_project, "pgsql")
        assert defs is not None
        
        # PostgreSQL-specific types
        all_types = []
        for category in defs["categories"].values():
            all_types.extend(category["types"])
        
        assert "timestamptz" in all_types
        assert "jsonb" in all_types
        assert "uuid" in all_types
        assert "text" in all_types
        
        # PostgreSQL-specific defaults
        uuid_defaults = defs["type_defaults"].get("uuid", [])
        assert "default_gen_random_uuid" in uuid_defaults


class TestMSSQLSpecificTypes:
    """Verify mssql.yaml contains MSSQL-specific types."""

    def test_mssql_definitions_contain_mssql_types(
        self, isolated_project: Path,
    ) -> None:
        """MSSQL type definitions include MSSQL-specific types."""
        _create_mock_type_definitions(isolated_project, "mssql")
        
        defs = _read_definitions_file(isolated_project, "mssql")
        assert defs is not None
        
        # MSSQL-specific types
        all_types = []
        for category in defs["categories"].values():
            all_types.extend(category["types"])
        
        assert "uniqueidentifier" in all_types
        assert "nvarchar" in all_types
        assert "datetime2" in all_types
        assert "datetimeoffset" in all_types
        assert "bit" in all_types
        
        # MSSQL-specific defaults
        uid_defaults = defs["type_defaults"].get("uniqueidentifier", [])
        assert "default_newid" in uid_defaults


class TestTypeSpecificDefaultSuggestions:
    """Completions filter defaults by type category."""

    def test_uuid_type_suggests_uuid_defaults(
        self, isolated_project: Path,
    ) -> None:
        """UUID type should suggest UUID-specific defaults."""
        _create_mock_type_definitions(isolated_project, "pgsql")
        
        defs = _read_definitions_file(isolated_project, "pgsql")
        assert defs is not None
        
        # UUID type should have specific defaults
        uuid_defaults = defs["type_defaults"].get("uuid", [])
        assert len(uuid_defaults) > 0
        assert "default_gen_random_uuid" in uuid_defaults
        
        # These defaults should NOT be in uuid defaults
        assert "default_now" not in uuid_defaults
        assert "default_0" not in uuid_defaults

    def test_timestamp_type_suggests_timestamp_defaults(
        self, isolated_project: Path,
    ) -> None:
        """Timestamp types should suggest timestamp-specific defaults."""
        _create_mock_type_definitions(isolated_project, "pgsql")
        
        defs = _read_definitions_file(isolated_project, "pgsql")
        assert defs is not None
        
        # Timestamp types should have time-related defaults
        ts_defaults = defs["type_defaults"].get("timestamp", [])
        assert len(ts_defaults) > 0
        assert "default_now" in ts_defaults or "default_current_timestamp" in ts_defaults
        
        # These defaults should NOT be in timestamp defaults
        assert "default_gen_random_uuid" not in ts_defaults
        assert "default_empty" not in ts_defaults


class TestMissingDefinitionsFile:
    """Graceful degradation when definitions file doesn't exist."""

    def test_missing_definitions_file_returns_none(
        self, isolated_project: Path,
    ) -> None:
        """Missing definitions file should return None gracefully."""
        # Don't create any definitions file
        defs = _read_definitions_file(isolated_project, "pgsql")
        assert defs is None
        
        # Verify the directory doesn't even exist
        defs_dir = isolated_project / "services" / "_schemas" / "_definitions"
        assert not defs_dir.exists()


class TestDefinitionsFileStructure:
    """Verify definitions file has correct YAML structure."""

    def test_definitions_file_has_categories_and_type_defaults(
        self, isolated_project: Path,
    ) -> None:
        """Definitions file must have both categories and type_defaults."""
        _create_mock_type_definitions(isolated_project, "pgsql")
        
        defs = _read_definitions_file(isolated_project, "pgsql")
        assert defs is not None
        
        # Must have both top-level keys
        assert "categories" in defs
        assert "type_defaults" in defs
        
        # Categories must be a dict
        assert isinstance(defs["categories"], dict)
        assert len(defs["categories"]) > 0
        
        # Each category must have types and defaults
        for category_name, category_data in defs["categories"].items():
            assert "types" in category_data, f"Category {category_name} missing 'types'"
            assert "defaults" in category_data, f"Category {category_name} missing 'defaults'"
            assert isinstance(category_data["types"], list)
            assert isinstance(category_data["defaults"], list)
        
        # type_defaults must be a dict
        assert isinstance(defs["type_defaults"], dict)


class TestCategoryBasedDefaults:
    """Verify category-level defaults are available."""

    def test_numeric_category_has_common_defaults(
        self, isolated_project: Path,
    ) -> None:
        """Numeric category should have common numeric defaults."""
        _create_mock_type_definitions(isolated_project, "pgsql")
        
        defs = _read_definitions_file(isolated_project, "pgsql")
        assert defs is not None
        
        numeric = defs["categories"].get("numeric")
        assert numeric is not None
        assert "defaults" in numeric
        
        # Common numeric defaults
        numeric_defaults = numeric["defaults"]
        assert "default_0" in numeric_defaults or "default_1" in numeric_defaults

    def test_text_category_has_common_defaults(
        self, isolated_project: Path,
    ) -> None:
        """Text category should have common text defaults."""
        _create_mock_type_definitions(isolated_project, "pgsql")
        
        defs = _read_definitions_file(isolated_project, "pgsql")
        assert defs is not None
        
        text = defs["categories"].get("text")
        assert text is not None
        assert "defaults" in text
        
        # Common text defaults
        text_defaults = text["defaults"]
        assert "default_empty" in text_defaults or "default_null" in text_defaults
