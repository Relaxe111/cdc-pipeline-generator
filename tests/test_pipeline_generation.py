"""Tests for canonical pipeline generation paths and verify modes."""

from __future__ import annotations

import importlib
import shutil
import sys
from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch

from cdc_generator.cli import pipeline_verify
from cdc_generator.core import pipeline_generator


def _copy_fixture_tree(tmp_path: Path) -> None:
    fixture_root = Path(__file__).parent / "fixtures" / "pipeline_generation"
    for item in fixture_root.iterdir():
        target = tmp_path / item.name
        if item.is_dir():
            shutil.copytree(item, target)
        else:
            shutil.copy2(item, target)


def test_generate_all_writes_canonical_pipeline_paths(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    _copy_fixture_tree(tmp_path)

    monkeypatch.chdir(tmp_path)
    importlib.reload(pipeline_generator)
    monkeypatch.setattr(sys, "argv", ["pipeline_generator.py", "--all"])

    pipeline_generator.main()

    source_pipeline = (
        tmp_path
        / "pipelines"
        / "generated"
        / "sources"
        / "default"
        / "customera"
        / "source-pipeline.yaml"
    )
    sink_pipeline = (
        tmp_path
        / "pipelines"
        / "generated"
        / "sinks"
        / "default"
        / "sink-pipeline.yaml"
    )

    assert source_pipeline.is_file()
    assert sink_pipeline.is_file()


def test_verify_full_passes_on_minimal_fixture(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    _copy_fixture_tree(tmp_path)

    monkeypatch.chdir(tmp_path)
    importlib.reload(pipeline_generator)
    monkeypatch.setattr(sys, "argv", ["pipeline_verify.py", "--full"])

    exit_code = pipeline_verify.main()

    assert exit_code == 0


def test_generate_consolidated_sink_skips_invalid_target_sink_env_route(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    _copy_fixture_tree(tmp_path)

    (tmp_path / "source-groups.yaml").write_text(
        "adopus:\n"
        "  pattern: db-per-tenant\n"
        "  environment_aware: false\n"
        "  type: mssql\n"
        "  servers:\n"
        "    default:\n"
        "      host: localhost\n"
        "      port: 1433\n"
        "      user: sa\n"
        "      password: pass\n"
        "      kafka_bootstrap_servers: localhost:9092\n"
        "  sources:\n"
        "    CustomerA:\n"
        "      schemas:\n"
        "        - dbo\n"
        "      default:\n"
        "        server: default\n"
        "        database: AdOpusCustomerA\n"
        "        target_sink_env: does_not_exist\n"
    )
    (tmp_path / "sink-groups.yaml").write_text(
        "sink_asma:\n"
        "  environment_aware: true\n"
        "  sources:\n"
        "    directory:\n"
        "      schemas:\n"
        "        - public\n"
        "      dev:\n"
        "        server: default\n"
        "        database: directory_dev\n"
    )
    (tmp_path / "services" / "adopus.yaml").write_text(
        "adopus:\n"
        "  source:\n"
        "    tables:\n"
        "      dbo.Actor:\n"
        "        primary_key: actno\n"
        "  sinks:\n"
        "    sink_asma.directory:\n"
        "      tables:\n"
        "        public.actor:\n"
        "          target_exists: false\n"
        "          from: dbo.Actor\n"
    )

    monkeypatch.chdir(tmp_path)
    importlib.reload(pipeline_generator)
    pipeline_generator.generate_consolidated_sink("default", ["customera"])

    sink_pipeline = (
        tmp_path
        / "pipelines"
        / "generated"
        / "sinks"
        / "default"
        / "sink-pipeline.yaml"
    )
    assert sink_pipeline.exists() is False
