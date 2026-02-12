"""Unit tests for ``cdc setup-local`` command internals."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from cdc_generator.cli import setup_local


def test_get_project_root_finds_compose_in_parent(tmp_path: Path) -> None:
    project = tmp_path / "project"
    nested = project / "a" / "b"
    nested.mkdir(parents=True)
    (project / "docker-compose.yml").write_text("services:\n  dev: {}\n")

    with patch("pathlib.Path.cwd", return_value=nested):
        result = setup_local.get_project_root()

    assert result == project


def test_get_project_root_raises_when_missing(tmp_path: Path) -> None:
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)

    with patch("pathlib.Path.cwd", return_value=nested):
        with pytest.raises(FileNotFoundError):
            setup_local.get_project_root()


@patch("cdc_generator.cli.setup_local.subprocess.run")
def test_run_docker_compose_returns_subprocess_code(mock_run: Mock) -> None:
    process = Mock()
    process.returncode = 7
    mock_run.return_value = process

    result = setup_local.run_docker_compose(["ps"])

    assert result == 7
    mock_run.assert_called_once_with(["docker", "compose", "ps"], check=False)


@patch("cdc_generator.cli.setup_local.subprocess.run", side_effect=FileNotFoundError)
def test_run_docker_compose_handles_missing_docker(_mock_run: Mock) -> None:
    result = setup_local.run_docker_compose(["ps"])

    assert result == 1


@patch("cdc_generator.cli.setup_local.get_project_root")
def test_main_returns_1_when_project_root_missing(mock_root: Mock) -> None:
    mock_root.side_effect = FileNotFoundError("missing compose")

    with patch("sys.argv", ["cdc"]):
        result = setup_local.main()

    assert result == 1


@patch("cdc_generator.cli.setup_local.run_docker_compose")
@patch("cdc_generator.cli.setup_local.get_project_root")
def test_main_no_flags_returns_0_without_running_compose(
    mock_root: Mock,
    mock_run: Mock,
) -> None:
    mock_root.return_value = Path("/tmp/project")

    with patch("sys.argv", ["cdc"]):
        result = setup_local.main()

    assert result == 0
    mock_run.assert_not_called()


@patch("cdc_generator.cli.setup_local.run_docker_compose")
@patch("cdc_generator.cli.setup_local.get_project_root")
def test_main_enable_local_sink_builds_expected_compose_args(
    mock_root: Mock,
    mock_run: Mock,
) -> None:
    mock_root.return_value = Path("/tmp/project")
    mock_run.return_value = 0

    with patch("sys.argv", ["cdc", "--enable-local-sink"]):
        result = setup_local.main()

    assert result == 0
    mock_run.assert_called_once_with(["--profile", "local-sink", "up", "-d"])


@patch("cdc_generator.cli.setup_local.run_docker_compose")
@patch("cdc_generator.cli.setup_local.get_project_root")
def test_main_multiple_profiles_preserve_order(
    mock_root: Mock,
    mock_run: Mock,
) -> None:
    mock_root.return_value = Path("/tmp/project")
    mock_run.return_value = 0

    with patch(
        "sys.argv",
        ["cdc", "--enable-local-source", "--enable-streaming"],
    ):
        result = setup_local.main()

    assert result == 0
    mock_run.assert_called_once_with(
        [
            "--profile",
            "local-source",
            "--profile",
            "streaming",
            "up",
            "-d",
        ],
    )


@patch("cdc_generator.cli.setup_local.run_docker_compose")
@patch("cdc_generator.cli.setup_local.get_project_root")
def test_main_full_uses_full_profile(mock_root: Mock, mock_run: Mock) -> None:
    mock_root.return_value = Path("/tmp/project")
    mock_run.return_value = 0

    with patch("sys.argv", ["cdc", "--full"]):
        result = setup_local.main()

    assert result == 0
    mock_run.assert_called_once_with(["--profile", "full", "up", "-d"])


@patch("cdc_generator.cli.setup_local.run_docker_compose")
@patch("cdc_generator.cli.setup_local.get_project_root")
def test_main_failure_from_compose_returns_1(mock_root: Mock, mock_run: Mock) -> None:
    mock_root.return_value = Path("/tmp/project")
    mock_run.return_value = 2

    with patch("sys.argv", ["cdc", "--enable-local-source"]):
        result = setup_local.main()

    assert result == 1


@patch("cdc_generator.cli.setup_local.run_docker_compose")
@patch("cdc_generator.cli.setup_local.get_project_root")
def test_main_down_stops_all_profiles(
    mock_root: Mock,
    mock_run: Mock,
) -> None:
    mock_root.return_value = Path("/tmp/project")
    mock_run.return_value = 0

    with patch("sys.argv", ["cdc", "--down"]):
        result = setup_local.main()

    assert result == 0
    assert mock_run.call_count == 4
    assert mock_run.call_args_list[0].args[0] == ["--profile", "local-sink", "down"]
    assert mock_run.call_args_list[1].args[0] == ["--profile", "local-source", "down"]
    assert mock_run.call_args_list[2].args[0] == ["--profile", "streaming", "down"]
    assert mock_run.call_args_list[3].args[0] == ["--profile", "full", "down"]
