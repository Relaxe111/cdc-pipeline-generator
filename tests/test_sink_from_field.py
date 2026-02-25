"""Tests for 'from' field feature in sink table configuration."""

from pathlib import Path
from typing import Any, cast

import pytest  # type: ignore[import-not-found]
import yaml

from cdc_generator.validators.manage_service.sink_operations import (
    TableConfigOptions,
    _build_table_config,
    _validate_table_add,
    add_sink_table,
)

# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------


def _create_test_service_config() -> dict[str, object]:
    """Create a minimal service config for testing."""
    return {
        "service": "test_service",
        "source": {
            "server_group": "test_group",
            "tables": {
                "public.customer_user": {
                    "primary_key": ["id"],
                },
                "logs.audit_queue": {
                    "primary_key": ["id"],
                },
                "dbo.Actor": {
                    "primary_key": ["actno"],
                },
            },
        },
        "sinks": {
            "sink_test.target": {
                "tables": {},
            },
        },
    }


# ---------------------------------------------------------------------------
# Unit tests for TableConfigOptions and _build_table_config
# ---------------------------------------------------------------------------


def test_build_table_config_minimal() -> None:
    """Test building minimal table config (only target_exists)."""
    opts = TableConfigOptions(target_exists=False)
    result = _build_table_config(opts)

    assert result == {"target_exists": False}


def test_build_table_config_with_from() -> None:
    """Test building table config with 'from' field."""
    opts = TableConfigOptions(
        target_exists=False,
        from_table="public.customer_user",
    )
    result = _build_table_config(opts)

    assert result == {
        "target_exists": False,
        "from": "public.customer_user",
    }


def test_build_table_config_with_replicate_structure() -> None:
    """Test building table config with replicate_structure flag."""
    opts = TableConfigOptions(
        target_exists=False,
        replicate_structure=True,
    )
    result = _build_table_config(opts)

    assert result == {
        "target_exists": False,
        "replicate_structure": True,
    }


def test_build_table_config_full_replicate() -> None:
    """Test building full replicate_structure config with from field."""
    opts = TableConfigOptions(
        target_exists=False,
        from_table="logs.audit_queue",
        replicate_structure=True,
        target_schema="other_schema",
    )
    result = _build_table_config(opts)

    assert result == {
        "target_exists": False,
        "from": "logs.audit_queue",
        "replicate_structure": True,
        "target_schema": "other_schema",
    }


def test_build_table_config_with_target_existing() -> None:
    """Test building config for mapping to existing table."""
    opts = TableConfigOptions(
        target_exists=True,
        target="public.chat_attachments",
        columns={"id": "attachment_id", "name": "file_name"},
    )
    result = _build_table_config(opts)

    assert result == {
        "target_exists": True,
        "target": "public.chat_attachments",
        "columns": {"id": "attachment_id", "name": "file_name"},
    }


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------


def test_validate_from_field_valid(monkeypatch: Any) -> None:
    """Test validation accepts valid 'from' field."""
    config = _create_test_service_config()
    table_opts: dict[str, Any] = {
        "target_exists": False,
        "from": "public.customer_user",
    }

    # Mock schema validation to always pass
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )

    tables, error = _validate_table_add(
        config,
        "sink_test.target",
        "other_schema.users",
        table_opts,
    )

    assert error is None
    assert tables is not None


def test_validate_from_field_invalid() -> None:
    """Test validation rejects invalid 'from' field."""
    config = _create_test_service_config()
    table_opts: dict[str, Any] = {
        "target_exists": False,
        "from": "nonexistent.table",
    }

    tables, error = _validate_table_add(
        config,
        "sink_test.target",
        "other_schema.users",
        table_opts,
    )

    assert error is not None
    assert "nonexistent.table" in error
    assert "not found" in error.lower()
    assert tables is None


def test_validate_from_field_lists_available_tables() -> None:
    """Test validation error includes available source tables."""
    config = _create_test_service_config()
    table_opts: dict[str, Any] = {
        "target_exists": False,
        "from": "invalid.table",
    }

    _tables, error = _validate_table_add(
        config,
        "sink_test.target",
        "other_schema.users",
        table_opts,
    )

    assert error is not None
    assert "public.customer_user" in error
    assert "logs.audit_queue" in error
    assert "dbo.Actor" in error


def test_validate_from_field_omitted() -> None:
    """Test validation fails when required 'from' field is omitted."""
    config = _create_test_service_config()
    table_opts: dict[str, Any] = {
        "target_exists": False,
    }

    _tables, error = _validate_table_add(
        config,
        "sink_test.target",
        "public.customer_user",
        table_opts,
    )

    assert _tables is None
    assert error is not None
    assert "required parameter 'from'" in error


# ---------------------------------------------------------------------------
# Integration tests (with temporary service files)
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_service_dir(tmp_path: Path) -> Path:
    """Create temporary services/ directory."""
    services_dir = tmp_path / "services"
    services_dir.mkdir()
    return services_dir


@pytest.fixture
def temp_service_file(temp_service_dir: Path) -> Path:
    """Create temporary service YAML file."""
    service_path = temp_service_dir / "test_service.yaml"
    config = _create_test_service_config()

    with service_path.open("w", encoding="utf-8") as f:
        yaml.dump(config, f)

    return service_path


def test_add_sink_table_with_from_writes_yaml(
    temp_service_file: Path,
    monkeypatch: Any,
) -> None:
    """Test that add_sink_table writes 'from' field to YAML."""
    # Mock get_project_root to use temp directory
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.get_project_root",
        lambda: temp_service_file.parent.parent,
    )

    # Mock load_service_config to use temp file
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: yaml.safe_load(temp_service_file.read_text()),
    )

    # Mock save_service_config to write to temp file
    def mock_save(service: str, config: dict[str, object]) -> bool:
        with temp_service_file.open("w", encoding="utf-8") as f:
            yaml.dump(config, f)
        return True

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        mock_save,
    )

    # Mock schema validation to always pass
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )

    # Add sink table with 'from' field
    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="other_schema.manage_audits",
        table_opts={
            "target_exists": False,
            "from": "logs.audit_queue",
            "replicate_structure": True,
        },
    )

    assert result is True

    # Verify YAML contains 'from' field
    with temp_service_file.open(encoding="utf-8") as f:
        updated_config = yaml.safe_load(f)

    sinks = cast(dict[str, object], updated_config["sinks"])
    sink_target = cast(dict[str, object], sinks["sink_test.target"])
    tables = cast(dict[str, object], sink_target["tables"])
    table_config = cast(dict[str, object], tables["other_schema.manage_audits"])

    assert table_config["from"] == "logs.audit_queue"
    assert table_config["replicate_structure"] is True
    assert table_config["target_exists"] is False


def test_add_sink_table_with_column_template_writes_yaml(
    temp_service_file: Path,
    monkeypatch: Any,
) -> None:
    """Test that add_sink_table writes column_templates to YAML."""
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.get_project_root",
        lambda: temp_service_file.parent.parent,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: yaml.safe_load(temp_service_file.read_text()),
    )

    def mock_save(_service: str, config: dict[str, object]) -> bool:
        with temp_service_file.open("w", encoding="utf-8") as f:
            yaml.dump(config, f)
        return True

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        mock_save,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )

    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="adopus.Actor",
        table_opts={
            "target_exists": False,
            "from": "dbo.Actor",
            "replicate_structure": True,
            "column_template": "customer_id",
            "column_template_name": "customer_id",
            "column_template_value": "{adopus.sources.*.customer_id}",
        },
    )

    assert result is True

    with temp_service_file.open(encoding="utf-8") as f:
        updated_config = yaml.safe_load(f)

    sinks = cast(dict[str, object], updated_config["sinks"])
    sink_target = cast(dict[str, object], sinks["sink_test.target"])
    tables = cast(dict[str, object], sink_target["tables"])
    table_config = cast(dict[str, object], tables["adopus.Actor"])

    assert table_config["from"] == "dbo.Actor"
    assert table_config["replicate_structure"] is True
    assert table_config["target_exists"] is False
    column_templates = cast(list[dict[str, str]], table_config["column_templates"])
    assert len(column_templates) == 1
    assert column_templates[0] == {
        "template": "customer_id",
        "name": "customer_id",
        "value": "{adopus.sources.*.customer_id}",
    }


def test_add_sink_table_custom_reference_does_not_duplicate_templates(
    temp_service_file: Path,
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    """Custom reference file must not duplicate column_templates metadata."""
    service_schemas_dir = tmp_path / "service-schemas"
    source_schema_dir = service_schemas_dir / "test_service" / "dbo"
    source_schema_dir.mkdir(parents=True)
    (source_schema_dir / "Actor.yaml").write_text("columns: []\n", encoding="utf-8")

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.get_project_root",
        lambda: tmp_path,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.SERVICE_SCHEMAS_DIR",
        service_schemas_dir,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.get_service_schema_read_dirs",
        lambda _service, _root: [service_schemas_dir / "test_service"],
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: yaml.safe_load(temp_service_file.read_text()),
    )

    def mock_save(_service: str, config: dict[str, object]) -> bool:
        with temp_service_file.open("w", encoding="utf-8") as f:
            yaml.dump(config, f)
        return True

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        mock_save,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )

    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="adopus.Actor",
        table_opts={
            "target_exists": False,
            "from": "dbo.Actor",
            "replicate_structure": True,
            "sink_schema": "adopus",
            "column_template": "customer_id",
            "column_template_name": "customer_id",
            "column_template_value": "{test_service.sources.*.customer_id}",
        },
    )

    assert result is True

    ref_file = service_schemas_dir / "target" / "custom-tables" / "adopus.Actor.yaml"
    assert ref_file.exists()

    reference_data = cast(dict[str, object], yaml.safe_load(ref_file.read_text()))
    assert "source_reference" in reference_data
    assert "sink_target" in reference_data
    assert "column_templates" not in reference_data


def test_add_sink_table_without_from_fails(
    temp_service_file: Path,
    monkeypatch: Any,
) -> None:
    """Test that add_sink_table fails when --from is not provided."""
    # Mock get_project_root to use temp directory
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.get_project_root",
        lambda: temp_service_file.parent.parent,
    )

    # Mock load_service_config to use temp file
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: yaml.safe_load(temp_service_file.read_text()),
    )

    # Mock save_service_config to write to temp file
    def mock_save(service: str, config: dict[str, object]) -> bool:
        with temp_service_file.open("w", encoding="utf-8") as f:
            yaml.dump(config, f)
        return True

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        mock_save,
    )

    # Mock schema validation to always pass
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )

    # Add sink table without 'from' field
    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="public.customer_user",
        table_opts={
            "target_exists": False,
        },
    )

    assert result is False


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_from_field_with_dots_in_schema() -> None:
    """Test 'from' field handles schema names with dots."""
    opts = TableConfigOptions(
        target_exists=False,
        from_table="schema.with.dots.table_name",
    )
    result = _build_table_config(opts)

    assert result["from"] == "schema.with.dots.table_name"


def test_replicate_structure_false_omitted() -> None:
    """Test that replicate_structure=False is omitted from output."""
    opts = TableConfigOptions(
        target_exists=False,
        replicate_structure=False,
    )
    result = _build_table_config(opts)

    assert "replicate_structure" not in result


# ---------------------------------------------------------------------------
# Add-time compatibility checks (target_exists=true)
# ---------------------------------------------------------------------------


def test_add_sink_table_allows_implicit_identity_when_types_match(
    monkeypatch: Any,
) -> None:
    """Same-name compatible columns should not require --map-column."""
    config = _create_test_service_config()

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: config,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        lambda _service, _config: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._load_table_columns",
        lambda _service, _table: [
            {"name": "id", "type": "uuid", "nullable": False},
            {"name": "name", "type": "text", "nullable": True},
        ],
    )

    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="public.customer_user",
        table_opts={
            "target_exists": True,
            "from": "public.customer_user",
        },
    )

    assert result is True


def test_add_sink_table_fails_on_incompatible_identity_columns(
    monkeypatch: Any,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Incompatible same-name columns should fail with mapping guidance."""
    config = _create_test_service_config()

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: config,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        lambda _service, _config: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._load_table_columns",
        lambda _service, _table: [
            {"name": "id", "type": "uuid", "nullable": False},
        ]
        if _service == "test_service"
        else [
            {"name": "id", "type": "text", "nullable": False},
        ],
    )

    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="public.customer_user",
        table_opts={
            "target_exists": True,
            "from": "public.customer_user",
        },
    )

    assert result is False
    output = capsys.readouterr().out
    assert "compatibility check failed" in output.lower()
    assert "--map-column" in output


def test_add_sink_table_accepts_explicit_mapping_for_required_sink_columns(
    monkeypatch: Any,
) -> None:
    """Explicit mapping should satisfy required sink columns."""
    config = _create_test_service_config()

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: config,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        lambda _service, _config: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._load_table_columns",
        lambda _service, _table: [
            {"name": "user_id", "type": "uuid", "nullable": False},
        ]
        if _service == "test_service"
        else [
            {"name": "id", "type": "uuid", "nullable": False},
        ],
    )

    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="public.customer_user",
        table_opts={
            "target_exists": True,
            "columns": {"user_id": "id"},
            "from": "public.customer_user",
        },
    )

    assert result is True


def test_add_sink_table_replicate_structure_skips_compatibility_check(
    monkeypatch: Any,
) -> None:
    """replicate_structure should preserve old behavior and bypass checks."""
    config = _create_test_service_config()

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: config,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        lambda _service, _config: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )

    def _unexpected_schema_load(*_args: object) -> object:
        raise AssertionError("column loader should not be called")

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._load_table_columns",
        _unexpected_schema_load,
    )

    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="public.customer_user",
        table_opts={
            "target_exists": True,
            "replicate_structure": True,
            "from": "public.customer_user",
        },
    )

    assert result is True


def test_add_sink_table_accepts_required_columns_with_defaults(
    monkeypatch: Any,
) -> None:
    """Non-null sink columns with defaults should not require source mapping."""
    config = _create_test_service_config()

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: config,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        lambda _service, _config: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )

    def _load_cols(_service: str, _table: str) -> list[dict[str, object]]:
        if _service == "test_service":
            return [
                {"name": "name", "type": "text", "nullable": False},
            ]
        return [
            {
                "name": "id",
                "type": "uuid",
                "nullable": False,
                "default_value": "gen_random_uuid()",
            },
            {
                "name": "region",
                "type": "text",
                "nullable": False,
                "default": "'global'::text",
            },
            {"name": "name", "type": "text", "nullable": False},
        ]

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._load_table_columns",
        _load_cols,
    )

    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="public.customer_user",
        table_opts={
            "target_exists": True,
            "from": "public.customer_user",
        },
    )

    assert result is True


def test_add_sink_table_accepts_required_column_from_column_template(
    monkeypatch: Any,
) -> None:
    """A required sink column can be covered by add-time column template output."""
    config = _create_test_service_config()

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: config,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        lambda _service, _config: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._load_table_columns",
        lambda _service, _table: [
            {"name": "actno", "type": "integer", "nullable": False},
        ]
        if _service == "test_service"
        else [
            {"name": "customer_id", "type": "uuid", "nullable": False},
        ],
    )

    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="public.customer_user",
        table_opts={
            "target_exists": True,
            "from": "public.customer_user",
            "column_template": "customer_id",
            "column_template_name": "customer_id",
        },
    )

    assert result is True


def test_add_sink_table_accepts_required_column_from_source_transform(
    monkeypatch: Any,
) -> None:
    """A required sink column can be covered by source transform rule output."""
    config = _create_test_service_config()
    source_tables = cast(
        dict[str, object],
        cast(dict[str, object], config["source"])["tables"],
    )
    source_tables["public.customer_user"] = {
        "transforms": [{"rule": "user_class_splitter"}],
    }

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: config,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        lambda _service, _config: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._load_table_columns",
        lambda _service, _table: [
            {"name": "id", "type": "uuid", "nullable": False},
        ]
        if _service == "test_service"
        else [
            {"name": "user_class", "type": "text", "nullable": False},
        ],
    )

    class _Output:
        name = "user_class"

    class _Rule:
        output_column = _Output()

    monkeypatch.setattr(
        "cdc_generator.core.transform_rules.get_rule",
        lambda _key: _Rule(),
    )

    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="public.customer_user",
        table_opts={
            "target_exists": True,
            "from": "public.customer_user",
        },
    )

    assert result is True


def test_add_sink_table_accepts_required_column_from_add_transform_option(
    monkeypatch: Any,
) -> None:
    """Add-time --add-transform output should satisfy required sink columns."""
    config = _create_test_service_config()

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: config,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        lambda _service, _config: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._load_table_columns",
        lambda _service, _table: [
            {"name": "id", "type": "uuid", "nullable": False},
        ]
        if _service == "test_service"
        else [
            {"name": "user_class", "type": "text", "nullable": False},
        ],
    )
    monkeypatch.setattr(
        "cdc_generator.core.bloblang_refs.read_bloblang_ref",
        lambda _ref: 'root = this.merge({"user_class":"Patient"})',
    )

    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="public.customer_user",
        table_opts={
            "target_exists": True,
            "from": "public.customer_user",
            "add_transform": "file://services/_bloblang/examples/user_class_splitter.blobl",
        },
    )

    assert result is True


def test_add_sink_table_rejects_required_column_from_commented_transform_output(
    monkeypatch: Any,
) -> None:
    """Commented merge/root outputs must not satisfy required sink columns."""
    config = _create_test_service_config()

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: config,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        lambda _service, _config: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._load_table_columns",
        lambda _service, _table: [
            {"name": "id", "type": "uuid", "nullable": False},
        ]
        if _service == "test_service"
        else [
            {"name": "user_class", "type": "text", "nullable": False},
        ],
    )
    monkeypatch.setattr(
        "cdc_generator.core.bloblang_refs.read_bloblang_ref",
        lambda _ref: '\n'.join([
            'let results = []',
            '# root.user_class = "Patient"',
            '# $results.append(this.merge({"user_class":"Patient"}))',
            'root = this',
        ]),
    )

    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="public.customer_user",
        table_opts={
            "target_exists": True,
            "from": "public.customer_user",
            "add_transform": "file://services/_bloblang/examples/user_class_splitter.blobl",
        },
    )

    assert result is False


def test_add_sink_table_accepts_required_column_via_accept_column(
    monkeypatch: Any,
) -> None:
    """accepted_columns should allow explicit bypass for required unmapped sink columns."""
    config = _create_test_service_config()

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: config,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        lambda _service, _config: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._load_table_columns",
        lambda _service, _table: [
            {"name": "id", "type": "uuid", "nullable": False},
        ]
        if _service == "test_service"
        else [
            {"name": "user_id", "type": "uuid", "nullable": False},
        ],
    )

    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="public.customer_user",
        table_opts={
            "target_exists": True,
            "from": "public.customer_user",
            "accepted_columns": ["user_id"],
        },
    )

    assert result is True


def test_add_sink_table_rejects_invalid_accept_column_name(
    monkeypatch: Any,
) -> None:
    """accepted_columns should fail fast on unknown sink column names."""
    config = _create_test_service_config()

    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.load_service_config",
        lambda _service: config,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations.save_service_config",
        lambda _service, _config: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._validate_table_in_schemas",
        lambda _sink_key, _table_key: True,
    )
    monkeypatch.setattr(
        "cdc_generator.validators.manage_service.sink_operations._load_table_columns",
        lambda _service, _table: [
            {"name": "id", "type": "uuid", "nullable": False},
        ]
        if _service == "test_service"
        else [
            {"name": "user_id", "type": "uuid", "nullable": False},
        ],
    )

    result = add_sink_table(
        service="test_service",
        sink_key="sink_test.target",
        table_key="public.customer_user",
        table_opts={
            "target_exists": True,
            "from": "public.customer_user",
            "accepted_columns": ["missing_col"],
        },
    )

    assert result is False
