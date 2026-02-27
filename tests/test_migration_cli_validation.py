"""Unit tests for migration CLI sink/env validation helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from cdc_generator.cli.migration_cli_validation import (
    list_manifest_envs,
    resolve_sink_filter,
    validate_env_for_sink,
)


def _write_manifest(
    sink_dir: Path,
    *,
    envs: list[str],
) -> None:
    sink_dir.mkdir(parents=True, exist_ok=True)
    env_rows = "\n".join(f"    {env}: db_{env}" for env in envs)
    (sink_dir / "manifest.yaml").write_text(
        "sink_target:\n"
        "  name: \"sink\"\n"
        "  databases:\n"
        + env_rows
        + "\n",
        encoding="utf-8",
    )


class TestResolveSinkFilter:
    """Sink selection rules for env-bound commands."""

    def test_autoselects_single_sink(self, tmp_path: Path) -> None:
        _write_manifest(tmp_path / "sink_one", envs=["dev", "prod"])

        sink = resolve_sink_filter(migrations_dir=tmp_path, sink_filter=None)

        assert sink == "sink_one"

    def test_requires_sink_when_multiple(self, tmp_path: Path) -> None:
        _write_manifest(tmp_path / "sink_one", envs=["dev"])
        _write_manifest(tmp_path / "sink_two", envs=["dev"])

        with pytest.raises(ValueError, match="--sink is required"):
            resolve_sink_filter(migrations_dir=tmp_path, sink_filter=None)

    def test_validates_requested_sink_exists(self, tmp_path: Path) -> None:
        _write_manifest(tmp_path / "sink_one", envs=["dev"])

        with pytest.raises(ValueError, match="Unknown sink"):
            resolve_sink_filter(migrations_dir=tmp_path, sink_filter="sink_missing")


class TestManifestEnvs:
    """Environment keys are read from manifest databases."""

    def test_lists_envs_for_single_sink(self, tmp_path: Path) -> None:
        _write_manifest(tmp_path / "sink_one", envs=["dev", "stage", "prod"])

        envs = list_manifest_envs(migrations_dir=tmp_path, sink_filter=None)

        assert envs == ["dev", "prod", "stage"]

    def test_lists_union_when_multiple_sinks_without_filter(self, tmp_path: Path) -> None:
        _write_manifest(tmp_path / "sink_one", envs=["dev", "prod"])
        _write_manifest(tmp_path / "sink_two", envs=["stage", "prod-adcuris"])

        envs = list_manifest_envs(migrations_dir=tmp_path, sink_filter=None)

        assert envs == ["dev", "prod", "prod-adcuris", "stage"]

    def test_validates_env_for_selected_sink(self, tmp_path: Path) -> None:
        _write_manifest(tmp_path / "sink_one", envs=["dev", "prod"])

        validate_env_for_sink(migrations_dir=tmp_path, sink_name="sink_one", env="dev")

        with pytest.raises(ValueError, match="not configured"):
            validate_env_for_sink(migrations_dir=tmp_path, sink_name="sink_one", env="stage")
