"""Unit tests for ``cdc test`` runner argument construction."""

from __future__ import annotations

import sys
from pathlib import Path

from cdc_generator.cli import test_runner


def test_build_pytest_args_all_adds_skip_reasons_by_default() -> None:
    root = Path("/workspace")

    cmd = test_runner._build_pytest_args(["--all"], root)

    assert cmd[:3] == [sys.executable, "-m", "pytest"]
    assert str(root / "tests") in cmd
    assert "-rs" in cmd


def test_build_pytest_args_all_does_not_override_reportchars_short_flag() -> None:
    root = Path("/workspace")

    cmd = test_runner._build_pytest_args(["--all", "-rA"], root)

    assert "-rA" in cmd
    assert "-rs" not in cmd


def test_build_pytest_args_all_does_not_override_reportchars_long_flag() -> None:
    root = Path("/workspace")

    cmd = test_runner._build_pytest_args(
        ["--all", "--reportchars=asxX"],
        root,
    )

    assert "--reportchars=asxX" in cmd
    assert "-rs" not in cmd


def test_build_pytest_args_cli_mode_does_not_auto_add_skip_reasons() -> None:
    root = Path("/workspace")

    cmd = test_runner._build_pytest_args(["--cli"], root)

    assert str(root / "tests/cli") in cmd
    assert "-rs" not in cmd
