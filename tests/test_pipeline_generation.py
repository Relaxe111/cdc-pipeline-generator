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
