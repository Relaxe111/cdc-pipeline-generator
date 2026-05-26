"""Tests for ``cdc fdw bootstrap`` commands."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, cast

import click.testing
import pytest
from click.shell_completion import ShellComplete

from cdc_generator.cli.fdw_bootstrap import (
    _build_bootstrap_query,
    _list_source_databases,
    _resolve_source_instance_keys,
    main as bootstrap_main,
)
from cdc_generator.cli.completions_bootstrap import _get_cache_file_path
from cdc_generator.cli.commands import _click_cli
from cdc_generator.helpers.fdw_bootstrap import (
    FdwBootstrapRequest,
    build_fdw_bootstrap_plan,
)

# Reuse the fdw_project fixture from test_fdw_bootstrap
from tests.test_fdw_bootstrap import _write_fdw_project  # noqa: E402


@pytest.fixture()
def bootstrap_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create a minimal implementation repo for bootstrap tests."""
    _write_fdw_project(tmp_path)
    monkeypatch.setenv("MSSQL_SOURCE_HOST", "10.90.37.9")
    monkeypatch.setenv("MSSQL_SOURCE_PORT", "49852")
    monkeypatch.setenv("MSSQL_SOURCE_USER", "cdc_pipeline_admin")
    monkeypatch.setenv("MSSQL_SOURCE_PASSWORD", "supersecret")
    monkeypatch.setenv("POSTGRES_SINK_HOST_ASMA_NONPROD", "10.90.37.20")
    monkeypatch.setenv("POSTGRES_SINK_PORT_ASMA_NONPROD", "5432")
    monkeypatch.setenv("POSTGRES_SINK_USER_ASMA_NONPROD", "pg_user_nonprod")
    monkeypatch.setenv("POSTGRES_SINK_PASSWORD_ASMA_NONPROD", "pg_password_nonprod")
    monkeypatch.setenv("POSTGRES_SINK_HOST_ASMA_PROD", "10.90.37.30")
    monkeypatch.setenv("POSTGRES_SINK_PORT_ASMA_PROD", "5432")
    monkeypatch.setenv("POSTGRES_SINK_USER_ASMA_PROD", "pg_user_prod")
    monkeypatch.setenv("POSTGRES_SINK_PASSWORD_ASMA_PROD", "pg_password_prod")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _source_instance_key_for_database(
    source_database: str,
    *,
    target_sink_env: str = "dev",
) -> str:
    """Resolve the expected source_instance_key for one source database."""
    plan = build_fdw_bootstrap_plan(
        "adopus",
        source_env=None,
        request=FdwBootstrapRequest(target_sink_env=target_sink_env),
    )
    source_plan = next(candidate for candidate in plan.source_plans if candidate.source_database == source_database)
    return f"{source_plan.source_env}_{source_plan.customer_key}"


# ---------------------------------------------------------------------------
# _list_source_databases
# ---------------------------------------------------------------------------


def test_list_source_databases_returns_sorted_names(
    bootstrap_project: Path,
) -> None:
    """_list_source_databases should return source database names from source-groups.yaml."""
    del bootstrap_project

    from cdc_generator.helpers.service_config import get_project_root

    project_root = get_project_root()
    plan = build_fdw_bootstrap_plan(
        "adopus",
        source_env=None,
        request=FdwBootstrapRequest(),
    )
    databases = _list_source_databases(project_root, plan.server_group_name)
    assert databases == ["FretexDev", "Test"]


# ---------------------------------------------------------------------------
# _resolve_source_instance_keys
# ---------------------------------------------------------------------------


def test_resolve_source_instance_keys_maps_correctly(
    bootstrap_project: Path,
) -> None:
    """Source DB names should map to source_instance_key via source_env + customer_id."""
    del bootstrap_project

    plan = build_fdw_bootstrap_plan(
        "adopus",
        source_env=None,
        request=FdwBootstrapRequest(target_sink_env="dev"),
    )
    keys = _resolve_source_instance_keys(plan.source_plans, ["Test"])
    expected_key = _source_instance_key_for_database("AdOpusTest")
    assert keys == [expected_key]


def test_resolve_source_instance_keys_empty_input(
    bootstrap_project: Path,
) -> None:
    """Empty source list should return empty keys."""
    del bootstrap_project

    plan = build_fdw_bootstrap_plan(
        "adopus",
        source_env=None,
        request=FdwBootstrapRequest(),
    )
    keys = _resolve_source_instance_keys(plan.source_plans, [])
    assert keys == []


def test_resolve_source_instance_keys_unknown_source(
    bootstrap_project: Path,
) -> None:
    """Unknown source database names should yield no keys."""
    del bootstrap_project

    plan = build_fdw_bootstrap_plan(
        "adopus",
        source_env=None,
        request=FdwBootstrapRequest(),
    )
    keys = _resolve_source_instance_keys(plan.source_plans, ["NonExistent"])
    assert keys == []


# ---------------------------------------------------------------------------
# _build_bootstrap_query
# ---------------------------------------------------------------------------


def test_build_bootstrap_query_all_sources_all_tables() -> None:
    """NULL source and NULL tables should produce a clean bootstrap call."""
    query = _build_bootstrap_query([], None, enable_after=True)
    assert "bootstrap_native_cdc_tables(NULL, NULL, true)" in query


def test_build_bootstrap_query_single_source_single_table() -> None:
    """Single source key and single table should produce correct SQL."""
    query = _build_bootstrap_query(
        ["nonprod_avansas"],
        ["Actor"],
        enable_after=True,
    )
    assert "bootstrap_native_cdc_tables('nonprod_avansas', ARRAY['Actor'], true)" in query


def test_build_bootstrap_query_multiple_tables() -> None:
    """Multiple tables should be passed as ARRAY."""
    query = _build_bootstrap_query(
        ["nonprod_avansas"],
        ["Actor", "Soknad"],
        enable_after=True,
    )
    assert "ARRAY['Actor', 'Soknad']" in query


def test_build_bootstrap_query_no_enable_after() -> None:
    """--no-enable-after should produce p_enable_after = false."""
    query = _build_bootstrap_query([], None, enable_after=False)
    assert "NULL, NULL, false)" in query


# ---------------------------------------------------------------------------
# CLI: bootstrap status
# ---------------------------------------------------------------------------


def test_bootstrap_status_dry_run(
    bootstrap_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """bootstrap status should query the DB via psql."""
    del bootstrap_project

    monkeypatch.setattr(
        "cdc_generator.helpers.fdw_sink_target.shutil.which",
        lambda _value: "/usr/bin/psql",
    )

    captured: dict[str, Any] = {}

    def fake_run(
        command: list[str],
        *,
        check: bool,
        env: dict[str, str],
        capture_output: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        return subprocess.CompletedProcess(command, 0, stdout="ok\n")

    monkeypatch.setattr("cdc_generator.cli.fdw_bootstrap.subprocess.run", fake_run)

    result = bootstrap_main(
        [
            "status",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
        ]
    )

    assert result == 0
    cmd = cast(list[str], captured.get("command", []))
    assert "/usr/bin/psql" in cmd
    assert "directory_dev" in cmd
    assert "native_cdc_bootstrap_state" in " ".join(cmd)


def test_bootstrap_status_filters_requested_source(
    bootstrap_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """status --source should translate the source DB name to a source key filter."""
    del bootstrap_project
    test_source_key = _source_instance_key_for_database("AdOpusTest")

    monkeypatch.setattr(
        "cdc_generator.helpers.fdw_sink_target.shutil.which",
        lambda _value: "/usr/bin/psql",
    )

    captured: dict[str, Any] = {}

    def fake_run(
        command: list[str],
        *,
        check: bool,
        env: dict[str, str],
        capture_output: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr("cdc_generator.cli.fdw_bootstrap.subprocess.run", fake_run)

    result = bootstrap_main(
        [
            "status",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--source",
            "AdOpusTest",
        ]
    )

    assert result == 0
    cmd = cast(list[str], captured.get("command", []))
    assert test_source_key in " ".join(cmd)


def test_bootstrap_status_json_output(
    bootstrap_project: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """status --json should emit machine-readable bootstrap state rows."""
    del bootstrap_project
    test_source_key = _source_instance_key_for_database("AdOpusTest")

    monkeypatch.setattr(
        "cdc_generator.helpers.fdw_sink_target.shutil.which",
        lambda _value: "/usr/bin/psql",
    )

    def fake_run(
        command: list[str],
        *,
        check: bool,
        env: dict[str, str],
        capture_output: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        del command, check, env, capture_output, text
        stdout = f"{test_source_key}\tActor\tpending\t\t\t12500\t\n"
        return subprocess.CompletedProcess(["psql"], 0, stdout=stdout)

    monkeypatch.setattr("cdc_generator.cli.fdw_bootstrap.subprocess.run", fake_run)

    result = bootstrap_main(
        [
            "status",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--json",
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["source_database"] == "AdOpusTest"
    assert payload[0]["logical_table_name"] == "Actor"
    assert payload[0]["bootstrap_status"] == "pending"


# ---------------------------------------------------------------------------
# CLI: bootstrap run --dry-run
# ---------------------------------------------------------------------------


def test_bootstrap_run_dry_run_prints_sql(
    bootstrap_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """bootstrap run --dry-run should print the SQL without executing."""
    del bootstrap_project

    monkeypatch.setattr(
        "cdc_generator.helpers.fdw_sink_target.shutil.which",
        lambda _value: "/usr/bin/psql",
    )

    result = bootstrap_main(
        [
            "run",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--source",
            "Test",
            "--dry-run",
        ]
    )

    # Dry run should succeed without actually connecting to a DB
    assert result == 0


def test_bootstrap_run_requires_source_or_all_sources(
    bootstrap_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """bootstrap run should require --source or --all-sources for db-per-tenant repos."""
    del bootstrap_project

    monkeypatch.setattr(
        "cdc_generator.helpers.fdw_sink_target.shutil.which",
        lambda _value: "/usr/bin/psql",
    )

    result = bootstrap_main(
        [
            "run",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--dry-run",
        ]
    )

    assert result == 1


def test_bootstrap_run_all_sources(
    bootstrap_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """bootstrap run --all-sources should build query for all sources."""
    del bootstrap_project
    test_source_key = _source_instance_key_for_database("AdOpusTest")

    monkeypatch.setattr(
        "cdc_generator.helpers.fdw_sink_target.shutil.which",
        lambda _value: "/usr/bin/psql",
    )

    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        check: bool,
        env: dict[str, str],
        capture_output: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        del check, env, capture_output, text
        commands.append(command)
        query = " ".join(command)
        if "native_cdc_bootstrap_state" in query and len(commands) == 1:
            stdout = f"{test_source_key}\tActor\tpending\t\t\t0\t\n"
            return subprocess.CompletedProcess(command, 0, stdout=stdout)
        if "native_cdc_bootstrap_state" in query:
            stdout = f"{test_source_key}\tActor\tcompleted\t2026-05-26 10:00:00\t\t12500\t\n"
            return subprocess.CompletedProcess(command, 0, stdout=stdout)
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr("cdc_generator.cli.fdw_bootstrap.subprocess.run", fake_run)

    result = bootstrap_main(
        [
            "run",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--all-sources",
        ]
    )

    assert result == 0
    bootstrap_commands = [command for command in commands if "bootstrap_native_cdc_tables" in " ".join(command)]
    assert bootstrap_commands


# ---------------------------------------------------------------------------
# CLI: bootstrap retry
# ---------------------------------------------------------------------------


def test_bootstrap_retry_dry_run(
    bootstrap_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """bootstrap retry --dry-run should print the failed-tables query."""
    del bootstrap_project

    monkeypatch.setattr(
        "cdc_generator.helpers.fdw_sink_target.shutil.which",
        lambda _value: "/usr/bin/psql",
    )

    result = bootstrap_main(
        [
            "retry",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--all-sources",
            "--dry-run",
        ]
    )

    assert result == 0


def test_bootstrap_retry_no_failed_tables(
    bootstrap_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """bootstrap retry with no failed tables should exit cleanly."""
    del bootstrap_project

    monkeypatch.setattr(
        "cdc_generator.helpers.fdw_sink_target.shutil.which",
        lambda _value: "/usr/bin/psql",
    )

    def fake_run(
        command: list[str],
        *,
        check: bool,
        env: dict[str, str],
        capture_output: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr("cdc_generator.cli.fdw_bootstrap.subprocess.run", fake_run)

    result = bootstrap_main(
        [
            "retry",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--source",
            "Test",
        ]
    )

    assert result == 0


def test_bootstrap_run_failed_queries_failed_rows(
    bootstrap_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run --failed should select failed rows before executing bootstrap."""
    del bootstrap_project
    test_source_key = _source_instance_key_for_database("AdOpusTest")

    monkeypatch.setattr(
        "cdc_generator.helpers.fdw_sink_target.shutil.which",
        lambda _value: "/usr/bin/psql",
    )

    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        check: bool,
        env: dict[str, str],
        capture_output: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        del check, env, capture_output, text
        commands.append(command)
        query = " ".join(command)
        if "native_cdc_bootstrap_state" in query and "bootstrap_status = 'failed'" in query:
            stdout = f"{test_source_key}\tActor\tfailed\t\t2026-05-26 09:45:00\t10\tboom\n"
            return subprocess.CompletedProcess(command, 0, stdout=stdout)
        if "native_cdc_bootstrap_state" in query:
            stdout = f"{test_source_key}\tActor\tcompleted\t2026-05-26 10:00:00\t\t12500\t\n"
            return subprocess.CompletedProcess(command, 0, stdout=stdout)
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr("cdc_generator.cli.fdw_bootstrap.subprocess.run", fake_run)

    result = bootstrap_main(
        [
            "run",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--source",
            "AdOpusTest",
            "--failed",
        ]
    )

    assert result == 0
    assert any("bootstrap_status = 'failed'" in " ".join(command) for command in commands)


def test_bootstrap_run_json_output_marks_disabled_rows(
    bootstrap_project: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """run --json should emit classified result rows and disabled notes."""
    del bootstrap_project
    test_source_key = _source_instance_key_for_database("AdOpusTest")

    monkeypatch.setattr(
        "cdc_generator.helpers.fdw_sink_target.shutil.which",
        lambda _value: "/usr/bin/psql",
    )

    call_index = 0

    def fake_run(
        command: list[str],
        *,
        check: bool,
        env: dict[str, str],
        capture_output: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal call_index
        del check, env, capture_output, text
        query = " ".join(command)
        call_index += 1
        if "native_cdc_bootstrap_state" in query and call_index == 1:
            stdout = f"{test_source_key}\tActor\tpending\t\t\t0\t\n"
            return subprocess.CompletedProcess(command, 0, stdout=stdout)
        if "native_cdc_bootstrap_state" in query:
            stdout = f"{test_source_key}\tActor\tcompleted\t2026-05-26 10:00:00\t\t12500\t\n"
            return subprocess.CompletedProcess(command, 0, stdout=stdout)
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr("cdc_generator.cli.fdw_bootstrap.subprocess.run", fake_run)

    result = bootstrap_main(
        [
            "run",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--source",
            "AdOpusTest",
            "--no-enable-after",
            "--json",
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["result"] == "bootstrapped"
    assert payload[0]["note"] == "disabled"


def test_bootstrap_run_multiple_sources_executes_per_source(
    bootstrap_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit multi-source runs must execute one bootstrap query per source key."""
    source_groups_path = bootstrap_project / "source-groups.yaml"
    source_groups_text = source_groups_path.read_text(encoding="utf-8")
    source_groups_path.write_text(
        source_groups_text.replace("target_sink_env: stage", "target_sink_env: dev"),
        encoding="utf-8",
    )
    test_source_key = _source_instance_key_for_database("AdOpusTest")
    fretex_source_key = _source_instance_key_for_database("AdOpusFretexDev")

    monkeypatch.setattr(
        "cdc_generator.helpers.fdw_sink_target.shutil.which",
        lambda _value: "/usr/bin/psql",
    )

    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        check: bool,
        env: dict[str, str],
        capture_output: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        del check, env, capture_output, text
        commands.append(command)
        query = " ".join(command)
        if "native_cdc_bootstrap_state" in query and len(commands) == 1:
            stdout = f"{fretex_source_key}\tActor\tpending\t\t\t0\t\n{test_source_key}\tActor\tpending\t\t\t0\t\n"
            return subprocess.CompletedProcess(command, 0, stdout=stdout)
        if "native_cdc_bootstrap_state" in query:
            stdout = (
                f"{fretex_source_key}\tActor\tcompleted\t2026-05-26 10:00:00\t\t100\t\n"
                f"{test_source_key}\tActor\tcompleted\t2026-05-26 10:00:00\t\t200\t\n"
            )
            return subprocess.CompletedProcess(command, 0, stdout=stdout)
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr("cdc_generator.cli.fdw_bootstrap.subprocess.run", fake_run)

    result = bootstrap_main(
        [
            "run",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--source",
            "AdOpusTest",
            "--source",
            "AdOpusFretexDev",
        ]
    )

    assert result == 0
    bootstrap_commands = [command for command in commands if "bootstrap_native_cdc_tables" in " ".join(command)]
    assert len(bootstrap_commands) == 2
    bootstrap_sql = [" ".join(command) for command in bootstrap_commands]
    assert any(test_source_key in sql for sql in bootstrap_sql)
    assert any(fretex_source_key in sql for sql in bootstrap_sql)


# ---------------------------------------------------------------------------
# CLI: default subcommand (status)
# ---------------------------------------------------------------------------


def test_bootstrap_status_explicit_subcommand(
    bootstrap_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """cdc fdw bootstrap status should query the DB."""
    del bootstrap_project

    monkeypatch.setattr(
        "cdc_generator.helpers.fdw_sink_target.shutil.which",
        lambda _value: "/usr/bin/psql",
    )

    captured: dict[str, Any] = {}

    def fake_run(
        command: list[str],
        *,
        check: bool,
        env: dict[str, str],
        capture_output: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        return subprocess.CompletedProcess(command, 0, stdout="ok\n")

    monkeypatch.setattr("cdc_generator.cli.fdw_bootstrap.subprocess.run", fake_run)

    # Default subcommand is status — no explicit subcommand needed
    result = bootstrap_main(
        [
            "status",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
        ]
    )

    assert result == 0
    cmd = cast(list[str], captured.get("command", []))
    assert "/usr/bin/psql" in cmd
    assert "directory_dev" in cmd
    assert "native_cdc_bootstrap_state" in " ".join(cmd)


# ---------------------------------------------------------------------------
# CLI: --no-enable-after produces correct query
# ---------------------------------------------------------------------------


def test_bootstrap_run_no_enable_after_query(
    bootstrap_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """bootstrap run --no-enable-after should pass false to the function."""
    del bootstrap_project
    test_source_key = _source_instance_key_for_database("AdOpusTest")

    monkeypatch.setattr(
        "cdc_generator.helpers.fdw_sink_target.shutil.which",
        lambda _value: "/usr/bin/psql",
    )

    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        check: bool,
        env: dict[str, str],
        capture_output: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        del check, env, capture_output, text
        commands.append(command)
        query = " ".join(command)
        if "native_cdc_bootstrap_state" in query and len(commands) == 1:
            stdout = f"{test_source_key}\tActor\tpending\t\t\t0\t\n"
            return subprocess.CompletedProcess(command, 0, stdout=stdout)
        if "native_cdc_bootstrap_state" in query:
            stdout = f"{test_source_key}\tActor\tcompleted\t2026-05-26 10:00:00\t\t12500\t\n"
            return subprocess.CompletedProcess(command, 0, stdout=stdout)
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr("cdc_generator.cli.fdw_bootstrap.subprocess.run", fake_run)

    result = bootstrap_main(
        [
            "run",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--source",
            "Test",
            "--no-enable-after",
        ]
    )

    assert result == 0
    bootstrap_commands = [" ".join(command) for command in commands if "bootstrap_native_cdc_tables" in " ".join(command)]
    assert bootstrap_commands
    assert any("false" in command for command in bootstrap_commands)


# ---------------------------------------------------------------------------
# Click help + completion wiring
# ---------------------------------------------------------------------------


def test_bootstrap_click_help_lists_subcommands(
    bootstrap_project: Path,
) -> None:
    """cdc fdw bootstrap --help should be served by the typed Click group."""
    del bootstrap_project

    runner = click.testing.CliRunner()
    result = runner.invoke(_click_cli, ["fdw", "bootstrap", "--help"])

    assert result.exit_code == 0
    assert "status" in result.output
    assert "run" in result.output
    assert "retry" in result.output


def test_bootstrap_status_help_includes_source_and_json(
    bootstrap_project: Path,
) -> None:
    """Typed status help should expose the planned source/json options."""
    del bootstrap_project

    runner = click.testing.CliRunner()
    result = runner.invoke(_click_cli, ["fdw", "bootstrap", "status", "--help"])

    assert result.exit_code == 0
    assert "--source" in result.output
    assert "--json" in result.output


def test_bootstrap_run_source_completion_uses_db_names(
    bootstrap_project: Path,
) -> None:
    """Shell completion should suggest source database names for bootstrap run."""
    del bootstrap_project

    completion = ShellComplete(_click_cli, {}, "cdc", "_CDC_COMPLETE")
    suggestions = completion.get_completions(
        [
            "fdw",
            "bootstrap",
            "run",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--source",
        ],
        "",
    )

    values = [suggestion.value for suggestion in suggestions]
    assert "AdOpusTest" in values


def test_bootstrap_run_table_completion_queries_sink_state(
    bootstrap_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Shell completion should query pending/failed table names through psql."""
    del bootstrap_project
    test_source_key = _source_instance_key_for_database("AdOpusTest")

    monkeypatch.setattr(
        "cdc_generator.helpers.fdw_sink_target.shutil.which",
        lambda _value: "/usr/bin/psql",
    )

    cache_path = _get_cache_file_path(
        "adopus",
        "dev",
        [test_source_key],
        False,
    )
    if cache_path.exists():
        cache_path.unlink()

    captured: dict[str, Any] = {}

    def fake_run(
        command: list[str],
        *,
        check: bool,
        env: dict[str, str],
        capture_output: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        return subprocess.CompletedProcess(command, 0, stdout="Actor\nSoknad\n")

    monkeypatch.setattr("cdc_generator.cli.completions_bootstrap.subprocess.run", fake_run)

    completion = ShellComplete(_click_cli, {}, "cdc", "_CDC_COMPLETE")
    suggestions = completion.get_completions(
        [
            "fdw",
            "bootstrap",
            "run",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--source",
            "AdOpusTest",
            "--table",
        ],
        "",
    )

    values = [suggestion.value for suggestion in suggestions]
    assert values == ["Actor", "Soknad"]
    command = cast(list[str], captured.get("command", []))
    assert "native_cdc_bootstrap_state" in " ".join(command)
