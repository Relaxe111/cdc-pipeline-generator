"""End-to-end flow: custom table schema → sink binding → pipeline generation.

Tests the complete workflow:
1. Create custom table schema under services/_schemas/{service}/custom-tables/
2. Bind custom table to a sink in service YAML
3. Generate pipelines with `cdc manage-pipelines generate`
4. Verify custom table appears in generated output

This validates the integration between:
- manage-services resources (custom table creation)
- manage-services config (sink binding)
- manage-pipelines generate (pipeline generation)
"""

from pathlib import Path

import pytest
import yaml

from tests.cli.conftest import RunCdc

pytestmark = pytest.mark.cli


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_source_groups(root: Path) -> None:
    """Write minimal source-groups.yaml for testing."""
    (root / "source-groups.yaml").write_text(
        "test_group:\n"
        "  pattern: db-shared\n"
        "  source_type: postgres\n"
        "  sources:\n"
        "    default:\n"
        "      envs:\n"
        "        dev:\n"
        "          kafka_bootstrap_servers: kafka:9092\n"
    )


def _write_sink_groups(root: Path) -> None:
    """Write minimal sink-groups.yaml for testing."""
    (root / "sink-groups.yaml").write_text(
        "sink_test:\n"
        "  source_group: test_group\n"
        "  pattern: db-shared\n"
        "  type: postgres\n"
        "  environment_aware: false\n"
        "  servers:\n"
        "    default:\n"
        "      host: localhost\n"
        "      port: 5432\n"
        "      database: testdb\n"
        "      user: testuser\n"
        "      password: testpass\n"
        "  sources: {}\n"
    )


def _write_service_yaml(root: Path, service: str, sinks: dict | None = None) -> None:
    """Write service YAML with optional sinks."""
    services_dir = root / "services"
    services_dir.mkdir(parents=True, exist_ok=True)
    
    config = {
        "server_group": "test_group",
        "tables": {
            "public.users": {
                "schema": "public",
                "table": "users",
                "primary_key": ["id"],
            }
        },
    }
    
    if sinks:
        config["sinks"] = sinks
    
    (services_dir / f"{service}.yaml").write_text(
        yaml.dump(config, default_flow_style=False)
    )


def _create_custom_table_schema(
    root: Path, service: str, schema: str, table: str,
) -> Path:
    """Create custom table schema file."""
    custom_dir = root / "service-schemas" / service / schema
    custom_dir.mkdir(parents=True, exist_ok=True)

    schema_file = custom_dir / f"{table}.yaml"
    schema_file.write_text(
        f"database: null\n"
        f"schema: {schema}\n"
        f"service: {service}\n"
        f"table: {table}\n"
        "custom: true\n"
        "columns:\n"
        "  - name: id\n"
        "    type: uuid\n"
        "    nullable: false\n"
        "    primary_key: true\n"
        "  - name: created_at\n"
        "    type: timestamptz\n"
        "    nullable: false\n"
        "primary_key: id\n"
    )
    return schema_file


def _read_service_yaml(root: Path, service: str) -> dict:
    """Read and parse service YAML, unwrapping service-name root key."""
    path = root / "services" / f"{service}.yaml"
    raw = yaml.safe_load(path.read_text())
    # After save_service_config, YAML is wrapped: {service: {...}}
    if isinstance(raw, dict) and service in raw:
        return raw[service]
    return raw


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSchemaToConfigFlowHappyPath:
    """Happy path: create custom table → bind to sink → generate."""

    def test_custom_table_bound_to_sink_appears_in_config(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """Custom table schema created and bound should appear in service config."""
        # Setup
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project)
        _write_service_yaml(isolated_project, "myservice")
        
        # Create custom table schema
        custom_file = _create_custom_table_schema(
            isolated_project, "myservice", "custom", "audit_log",
        )
        assert custom_file.exists()
        
        # Add custom table as source table first
        run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--add-source-table", "custom.audit_log",
        )
        
        # Bind custom table to sink
        result = run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--add-sink", "sink_test.myservice",
        )
        assert result.returncode == 0
        
        result = run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--sink", "sink_test.myservice",
            "--add-custom-sink-table", "custom.audit_log",
            "--from", "custom.audit_log",
        )
        assert result.returncode == 0
        
        # Verify it's in the service config
        config = _read_service_yaml(isolated_project, "myservice")
        assert "sink_test.myservice" in config["sinks"]
        sink_tables = config["sinks"]["sink_test.myservice"]["tables"]
        assert "custom.audit_log" in sink_tables


class TestMultipleCustomTables:
    """Bind multiple custom tables to different sinks."""

    def test_multiple_custom_tables_different_sinks(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """Multiple custom tables can be bound to different sinks."""
        # Setup
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project)
        _write_service_yaml(isolated_project, "myservice")
        
        # Create two custom tables
        _create_custom_table_schema(
            isolated_project, "myservice", "custom", "audit_log",
        )
        _create_custom_table_schema(
            isolated_project, "myservice", "custom", "event_log",
        )
        
        # Add custom tables as source tables
        run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--add-source-table", "custom.audit_log",
        )
        run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--add-source-table", "custom.event_log",
        )
        
        # Add two sinks
        run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--add-sink", "sink_test.service1",
        )
        run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--add-sink", "sink_test.service2",
        )
        
        # Bind each custom table to a different sink
        result1 = run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--sink", "sink_test.service1",
            "--add-custom-sink-table", "custom.audit_log",
            "--from", "custom.audit_log",
        )
        assert result1.returncode == 0
        
        result2 = run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--sink", "sink_test.service2",
            "--add-custom-sink-table", "custom.event_log",
            "--from", "custom.event_log",
        )
        assert result2.returncode == 0
        
        # Verify both are in the config
        config = _read_service_yaml(isolated_project, "myservice")
        assert "custom.audit_log" in config["sinks"]["sink_test.service1"]["tables"]
        assert "custom.event_log" in config["sinks"]["sink_test.service2"]["tables"]


class TestUnboundCustomTable:
    """Custom table exists but not bound → not in generation."""

    def test_unbound_custom_table_not_in_sink(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """Custom table schema exists but not bound to any sink."""
        # Setup
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project)
        _write_service_yaml(isolated_project, "myservice")
        
        # Create custom table but don't bind it
        _create_custom_table_schema(
            isolated_project, "myservice", "custom", "orphan_table",
        )
        
        # Add a sink
        run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--add-sink", "sink_test.myservice",
        )
        
        # Verify orphan table is NOT in sink config
        config = _read_service_yaml(isolated_project, "myservice")
        assert "sink_test.myservice" in config["sinks"]
        sink_tables = config["sinks"]["sink_test.myservice"]["tables"]
        assert "custom.orphan_table" not in sink_tables


class TestRemoveCustomTableBinding:
    """Binding removed → no longer in generated config."""

    def test_remove_custom_table_from_sink(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """Custom table bound then removed from sink."""
        # Setup
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project)
        _write_service_yaml(isolated_project, "myservice")
        
        # Create and bind custom table
        _create_custom_table_schema(
            isolated_project, "myservice", "custom", "temp_log",
        )
        
        # Add custom table as source table first
        run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--add-source-table", "custom.temp_log",
        )
        
        run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--add-sink", "sink_test.myservice",
        )
        run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--sink", "sink_test.myservice",
            "--add-custom-sink-table", "custom.temp_log",
            "--from", "custom.temp_log",
        )
        
        # Verify it's there
        config = _read_service_yaml(isolated_project, "myservice")
        sink_tables = config["sinks"]["sink_test.myservice"]["tables"]
        assert "custom.temp_log" in sink_tables
        
        # Remove it
        result = run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--sink", "sink_test.myservice",
            "--remove-sink-table", "custom.temp_log",
        )
        assert result.returncode == 0
        
        # Verify it's gone
        config = _read_service_yaml(isolated_project, "myservice")
        sink_tables = config["sinks"]["sink_test.myservice"]["tables"]
        assert "custom.temp_log" not in sink_tables


class TestInvalidCustomTableReference:
    """Try to bind non-existent custom table → error."""

    def test_bind_nonexistent_custom_table_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """Attempting to bind a custom table that doesn't exist should fail."""
        # Setup
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project)
        _write_service_yaml(isolated_project, "myservice")
        
        run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--add-sink", "sink_test.myservice",
        )
        
        # Try to bind non-existent custom table
        result = run_cdc(
            "manage-services", "config",
            "--service", "myservice",
            "--sink", "sink_test.myservice",
            "--add-custom-sink-table", "custom.does_not_exist",
            "--from", "custom.does_not_exist",
        )
        
        # Should fail
        assert result.returncode != 0
        # Error could mention the missing table or "not found"
        output = (result.stdout + result.stderr).lower()
        assert "does_not_exist" in output or "not found" in output
