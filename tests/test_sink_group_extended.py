"""Additional unit tests for ``manage-sink-groups`` gaps."""

from argparse import Namespace
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from cdc_generator.cli.sink_group import (
    SinkGroupArgumentParser,
    _auto_scaffold_sink_groups,
    _check_readiness_and_warnings,
    _check_structure_and_resolution,
    _create_inherited_sink_group_from_source,
    _create_standalone_sink,
    _fetch_databases,
    _run_inspection,
    _validate_inspect_args,
    get_sink_file_path,
    get_source_group_file_path,
    handle_add_new_sink_group,
    handle_create,
    handle_info_command,
    handle_inspect_command,
    handle_introspect_types_command,
    handle_list,
)


def _ns(**kwargs: Any) -> Namespace:
    defaults: dict[str, Any] = {
        "source_group": None,
        "add_new_sink_group": None,
        "for_source_group": None,
        "type": "postgres",
        "pattern": "db-shared",
        "environment_aware": True,
        "database_exclude_patterns": None,
        "schema_exclude_patterns": None,
        "sink_group": None,
        "server": None,
        "include_pattern": None,
        "info": None,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


def test_parser_error_for_missing_flag_value() -> None:
    parser = SinkGroupArgumentParser(prog="cdc manage-sink-groups")

    with pytest.raises(SystemExit) as exc:
        parser.error("argument --add-server: expected one argument")

    assert exc.value.code == 1


def test_get_sink_file_path_uses_project_root() -> None:
    with patch("cdc_generator.cli.sink_group.get_project_root") as mock_root:
        mock_root.return_value = Path("/tmp/project")

        path = get_sink_file_path()

    assert path == Path("/tmp/project/sink-groups.yaml")


def test_get_source_group_file_path_returns_path_when_present(tmp_path: Path) -> None:
    source_file = tmp_path / "source-groups.yaml"
    source_file.write_text("asma: {}")

    with patch("cdc_generator.cli.sink_group.get_project_root") as mock_root:
        mock_root.return_value = tmp_path
        result = get_source_group_file_path()

    assert result == source_file


def test_get_source_group_file_path_exits_when_missing(tmp_path: Path) -> None:
    with patch("cdc_generator.cli.sink_group.get_project_root") as mock_root:
        mock_root.return_value = tmp_path
        with pytest.raises(SystemExit) as exc:
            get_source_group_file_path()

    assert exc.value.code == 1


@patch("cdc_generator.cli.sink_group.save_sink_groups")
@patch("cdc_generator.cli.sink_group.create_inherited_sink_group")
def test_auto_scaffold_creates_only_db_shared(
    mock_create: Mock,
    mock_save: Mock,
    tmp_path: Path,
) -> None:
    sink_groups: dict[str, Any] = {}
    source_groups = {
        "asma": {"pattern": "db-shared", "sources": {"directory": {}}},
        "legacy": {"pattern": "db-per-tenant", "sources": {}},
    }
    mock_create.return_value = {
        "inherits": True,
        "servers": {"default": {"source_ref": "default"}},
        "inherited_sources": ["directory"],
    }

    result = _auto_scaffold_sink_groups(
        sink_groups,
        source_groups,
        tmp_path / "sink-groups.yaml",
    )

    assert result == 0
    assert "sink_asma" in sink_groups
    assert "sink_legacy" not in sink_groups
    assert mock_save.called


@patch("cdc_generator.cli.sink_group.save_sink_groups")
def test_auto_scaffold_no_new_groups_returns_zero(
    mock_save: Mock,
    tmp_path: Path,
) -> None:
    sink_groups: dict[str, Any] = {"sink_asma": {"servers": {}}}
    source_groups = {
        "legacy": {"pattern": "db-per-tenant", "sources": {}},
        "asma": {"pattern": "db-shared", "sources": {}},
    }

    result = _auto_scaffold_sink_groups(
        sink_groups,
        source_groups,
        tmp_path / "sink-groups.yaml",
    )

    assert result == 0
    assert not mock_save.called


@patch("cdc_generator.cli.sink_group.save_sink_groups")
@patch("cdc_generator.cli.sink_group.create_inherited_sink_group")
def test_create_inherited_sink_group_success(
    mock_create: Mock,
    mock_save: Mock,
    tmp_path: Path,
) -> None:
    args = _ns(source_group="asma")
    sink_groups: dict[str, Any] = {}
    source_groups = {
        "asma": {"pattern": "db-shared", "sources": {"directory": {}}},
    }
    mock_create.return_value = {
        "inherits": True,
        "servers": {"default": {"source_ref": "default"}},
        "inherited_sources": ["directory"],
    }

    result = _create_inherited_sink_group_from_source(
        args,
        sink_groups,
        source_groups,
        tmp_path / "sink-groups.yaml",
        tmp_path / "source-groups.yaml",
    )

    assert result == 0
    assert "sink_asma" in sink_groups
    assert mock_save.called


def test_create_inherited_sink_group_rejects_non_shared_pattern(tmp_path: Path) -> None:
    args = _ns(source_group="legacy")
    sink_groups: dict[str, Any] = {}
    source_groups = {
        "legacy": {"pattern": "db-per-tenant", "sources": {"directory": {}}},
    }

    result = _create_inherited_sink_group_from_source(
        args,
        sink_groups,
        source_groups,
        tmp_path / "sink-groups.yaml",
        tmp_path / "source-groups.yaml",
    )

    assert result == 1


def test_create_standalone_sink_uses_first_source_group_when_missing(tmp_path: Path) -> None:
    args = _ns(add_new_sink_group="analytics", for_source_group=None)
    sink_groups: dict[str, Any] = {}
    source_groups = {"asma": {"pattern": "db-shared", "sources": {}}}

    result = _create_standalone_sink(
        args,
        sink_groups,
        source_groups,
        tmp_path / "sink-groups.yaml",
        tmp_path / "source-groups.yaml",
    )

    assert result == 0
    assert "sink_analytics" in sink_groups
    assert sink_groups["sink_analytics"]["source_group"] == "asma"


def test_create_standalone_sink_rejects_unknown_source_group(tmp_path: Path) -> None:
    args = _ns(add_new_sink_group="analytics", for_source_group="ghost")
    sink_groups: dict[str, Any] = {}
    source_groups = {"asma": {"pattern": "db-shared", "sources": {}}}

    result = _create_standalone_sink(
        args,
        sink_groups,
        source_groups,
        tmp_path / "sink-groups.yaml",
        tmp_path / "source-groups.yaml",
    )

    assert result == 1


@patch("cdc_generator.cli.sink_group._auto_scaffold_sink_groups")
@patch("cdc_generator.cli.sink_group.load_yaml_file")
@patch("cdc_generator.cli.sink_group.load_sink_groups")
@patch("cdc_generator.cli.sink_group.get_source_group_file_path")
@patch("cdc_generator.cli.sink_group.get_sink_file_path")
def test_handle_create_uses_auto_scaffold_when_no_source_group(
    mock_sink_path: Mock,
    mock_source_path: Mock,
    mock_load_sink: Mock,
    mock_load_yaml: Mock,
    mock_auto: Mock,
) -> None:
    mock_sink_path.return_value = Path("/tmp/sink-groups.yaml")
    mock_source_path.return_value = Path("/tmp/source-groups.yaml")
    mock_load_sink.return_value = {}
    mock_load_yaml.return_value = {"asma": {"pattern": "db-shared", "sources": {}}}
    mock_auto.return_value = 0

    result = handle_create(_ns(source_group=None))

    assert result == 0
    assert mock_auto.called


@patch("cdc_generator.cli.sink_group._create_standalone_sink")
@patch("cdc_generator.cli.sink_group.load_yaml_file")
@patch("cdc_generator.cli.sink_group.load_sink_groups")
@patch("cdc_generator.cli.sink_group.get_source_group_file_path")
@patch("cdc_generator.cli.sink_group.get_sink_file_path")
def test_handle_add_new_sink_group_handles_missing_sink_file(
    mock_sink_path: Mock,
    mock_source_path: Mock,
    mock_load_sink: Mock,
    mock_load_yaml: Mock,
    mock_create: Mock,
) -> None:
    mock_sink_path.return_value = Path("/tmp/sink-groups.yaml")
    mock_source_path.return_value = Path("/tmp/source-groups.yaml")
    mock_load_sink.side_effect = FileNotFoundError
    mock_load_yaml.return_value = {"asma": {"pattern": "db-shared", "sources": {}}}
    mock_create.return_value = 0

    result = handle_add_new_sink_group(
        _ns(add_new_sink_group="analytics", for_source_group="asma"),
    )

    assert result == 0
    assert mock_create.called


@patch("cdc_generator.cli.sink_group.load_sink_groups")
@patch("cdc_generator.cli.sink_group.get_sink_file_path")
def test_handle_list_when_empty_reports_no_groups(
    mock_sink_path: Mock,
    mock_load: Mock,
) -> None:
    mock_sink_path.return_value = Path("/tmp/sink-groups.yaml")
    mock_load.return_value = {}

    assert handle_list(_ns()) == 0


@patch("cdc_generator.cli.sink_group.load_sink_groups")
@patch("cdc_generator.cli.sink_group.get_sink_file_path")
def test_validate_inspect_args_rejects_inherited_group(
    mock_sink_path: Mock,
    mock_load: Mock,
) -> None:
    mock_sink_path.return_value = Path("/tmp/sink-groups.yaml")
    mock_load.return_value = {
        "sink_asma": {
            "inherits": True,
            "servers": {"default": {"source_ref": "default"}},
            "inherited_sources": ["directory"],
        },
    }

    result = _validate_inspect_args(_ns(sink_group="sink_asma"))

    assert result == 1


def test_validate_inspect_args_requires_sink_group() -> None:
    result = _validate_inspect_args(_ns(sink_group=None))

    assert result == 1


@patch("cdc_generator.cli.sink_group.load_sink_groups")
@patch("cdc_generator.cli.sink_group.get_sink_file_path")
def test_validate_inspect_args_success_returns_tuple(
    mock_sink_path: Mock,
    mock_load: Mock,
) -> None:
    mock_sink_path.return_value = Path("/tmp/sink-groups.yaml")
    mock_load.return_value = {
        "sink_analytics": {
            "source_group": "asma",
            "type": "postgres",
            "servers": {"default": {"host": "localhost", "port": "5432"}},
            "sources": {},
        },
    }

    result = _validate_inspect_args(_ns(sink_group="sink_analytics"))

    assert isinstance(result, tuple)


@patch("cdc_generator.cli.sink_group._fetch_databases")
def test_run_inspection_handles_import_error(mock_fetch: Mock) -> None:
    mock_fetch.side_effect = ImportError("psycopg")
    resolved = {
        "type": "postgres",
        "servers": {"default": {"host": "localhost", "port": "5432"}},
    }

    result = _run_inspection(resolved, "sink_analytics", _ns(server="default"))

    assert result == 1


@patch("cdc_generator.cli.sink_group._fetch_databases")
def test_run_inspection_success(mock_fetch: Mock) -> None:
    mock_fetch.return_value = [
        {
            "name": "analytics",
            "service": "directory",
            "environment": "nonprod",
            "schemas": ["public"],
            "table_count": 10,
        },
    ]
    resolved = {
        "type": "postgres",
        "servers": {"default": {"host": "localhost", "port": "5432"}},
    }

    result = _run_inspection(resolved, "sink_analytics", _ns(server="default"))

    assert result == 0


def test_fetch_databases_rejects_unsupported_type() -> None:
    with pytest.raises(ValueError):
        _fetch_databases(
            "http_client",
            {"host": "localhost"},
            {"type": "http_client", "servers": {}},
            _ns(),
            "default",
        )


@patch("cdc_generator.cli.sink_group._run_inspection")
@patch("cdc_generator.cli.sink_group.resolve_sink_group")
@patch("cdc_generator.cli.sink_group.load_yaml_file")
@patch("cdc_generator.cli.sink_group.get_source_group_file_path")
@patch("cdc_generator.cli.sink_group._validate_inspect_args")
def test_handle_inspect_command_success(
    mock_validate: Mock,
    mock_source_path: Mock,
    mock_load_yaml: Mock,
    mock_resolve: Mock,
    mock_run: Mock,
) -> None:
    sink_group = {
        "source_group": "asma",
        "type": "postgres",
        "servers": {"default": {"host": "localhost", "port": "5432"}},
        "sources": {},
    }
    resolved = {
        "type": "postgres",
        "servers": {"default": {"host": "localhost", "port": "5432"}},
    }
    mock_validate.return_value = ({"sink_analytics": sink_group}, sink_group, "sink_analytics")
    mock_source_path.return_value = Path("/tmp/source-groups.yaml")
    mock_load_yaml.return_value = {"asma": {"servers": {}, "sources": {}}}
    mock_resolve.return_value = resolved
    mock_run.return_value = 0

    result = handle_inspect_command(_ns(sink_group="sink_analytics", server="default"))

    assert result == 0
    assert mock_run.called


@patch("cdc_generator.cli.sink_group._validate_inspect_args")
def test_handle_inspect_command_propagates_validation_error(
    mock_validate: Mock,
) -> None:
    mock_validate.return_value = 1

    result = handle_inspect_command(_ns(sink_group="sink_analytics"))

    assert result == 1


@patch("cdc_generator.validators.manage_server_group.type_introspector.introspect_types")
@patch("cdc_generator.cli.sink_group.resolve_sink_group")
@patch("cdc_generator.cli.sink_group.load_yaml_file")
@patch("cdc_generator.cli.sink_group.get_source_group_file_path")
@patch("cdc_generator.cli.sink_group._validate_inspect_args")
def test_handle_introspect_types_uses_username_fallback(
    mock_validate: Mock,
    mock_source_path: Mock,
    mock_load_yaml: Mock,
    mock_resolve: Mock,
    mock_introspect: Mock,
) -> None:
    sink_group = {
        "source_group": "asma",
        "type": "postgres",
        "servers": {"default": {"host": "localhost", "port": "5432", "username": "pg"}},
        "sources": {},
    }
    mock_validate.return_value = ({"sink_analytics": sink_group}, sink_group, "sink_analytics")
    mock_source_path.return_value = Path("/tmp/source-groups.yaml")
    mock_load_yaml.return_value = {"asma": {"servers": {}, "sources": {}}}
    mock_resolve.return_value = sink_group
    mock_introspect.return_value = True

    result = handle_introspect_types_command(
        _ns(sink_group="sink_analytics", server="default"),
    )

    assert result == 0
    assert mock_introspect.called


@patch("cdc_generator.cli.sink_group.resolve_sink_group")
@patch("cdc_generator.cli.sink_group.load_yaml_file")
@patch("cdc_generator.cli.sink_group.get_source_group_file_path")
@patch("cdc_generator.cli.sink_group._validate_inspect_args")
def test_handle_introspect_types_requires_servers(
    mock_validate: Mock,
    mock_source_path: Mock,
    mock_load_yaml: Mock,
    mock_resolve: Mock,
) -> None:
    sink_group = {
        "source_group": "asma",
        "type": "postgres",
        "servers": {"default": {"host": "localhost", "port": "5432"}},
        "sources": {},
    }
    mock_validate.return_value = ({"sink_analytics": sink_group}, sink_group, "sink_analytics")
    mock_source_path.return_value = Path("/tmp/source-groups.yaml")
    mock_load_yaml.return_value = {"asma": {"servers": {}, "sources": {}}}
    mock_resolve.return_value = {
        "type": "postgres",
        "servers": {},
    }

    result = handle_introspect_types_command(
        _ns(sink_group="sink_analytics", server="default"),
    )

    assert result == 1


@patch("cdc_generator.cli.sink_group.resolve_sink_group")
@patch("cdc_generator.cli.sink_group.load_yaml_file")
@patch("cdc_generator.cli.sink_group.load_sink_groups")
@patch("cdc_generator.cli.sink_group.get_source_group_file_path")
@patch("cdc_generator.cli.sink_group.get_sink_file_path")
def test_handle_info_command_success(
    mock_sink_path: Mock,
    mock_source_path: Mock,
    mock_load_sink: Mock,
    mock_load_yaml: Mock,
    mock_resolve: Mock,
) -> None:
    mock_sink_path.return_value = Path("/tmp/sink-groups.yaml")
    mock_source_path.return_value = Path("/tmp/source-groups.yaml")
    mock_load_sink.return_value = {
        "sink_analytics": {
            "source_group": "asma",
            "type": "postgres",
            "servers": {"default": {"host": "localhost", "port": "5432"}},
            "sources": {},
        },
    }
    mock_load_yaml.return_value = {"asma": {"servers": {}, "sources": {}}}
    mock_resolve.return_value = {
        "source_group": "asma",
        "pattern": "db-shared",
        "type": "postgres",
        "kafka_topology": "default",
        "environment_aware": True,
        "description": "analytics sink",
        "servers": {"default": {"host": "localhost", "port": "5432"}},
        "sources": {},
    }

    result = handle_info_command(_ns(info="sink_analytics"))

    assert result == 0


@patch("cdc_generator.cli.sink_group.get_sink_group_warnings")
@patch("cdc_generator.cli.sink_group.is_sink_group_ready")
def test_check_readiness_and_warnings_reports_inheritance(
    mock_ready: Mock,
    mock_warnings: Mock,
) -> None:
    mock_ready.return_value = False
    mock_warnings.return_value = ["missing source mappings"]

    has_warnings = _check_readiness_and_warnings(
        "sink_asma",
        {"inherits": True, "servers": {"default": {"source_ref": "default"}}},
        {"sink_asma": {"inherits": True}},
        {"asma": {"servers": {}, "sources": {}}},
    )

    assert has_warnings is True


@patch("cdc_generator.cli.sink_group.resolve_sink_group")
@patch("cdc_generator.cli.sink_group.validate_sink_group_structure")
def test_check_structure_and_resolution_handles_resolution_error(
    mock_validate: Mock,
    mock_resolve: Mock,
) -> None:
    mock_validate.return_value = []
    mock_resolve.side_effect = ValueError("bad refs")

    is_valid = _check_structure_and_resolution(
        "sink_analytics",
        {"servers": {"default": {"host": "localhost"}}},
        {"sink_analytics": {"servers": {}}},
        {"asma": {"servers": {}, "sources": {}}},
    )

    assert is_valid is False
