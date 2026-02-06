"""Code quality check command - type checking + linting with auto-fix.

Usage:
    cdc check              Run pyright + ruff (safe auto-fix) + black
    cdc check --force      Also apply unsafe ruff fixes
    cdc check --no-fix     Run checks without any auto-fix
    cdc check <path>       Check specific file or directory
"""

import subprocess
import sys
from pathlib import Path
from typing import List


DEFAULT_TARGET = "cdc_generator/"


def run_check(args: List[str]) -> int:
    """Run code quality checks: pyright + ruff + black.

    Args:
        args: CLI arguments (--force, --no-fix, or path)

    Returns:
        Exit code (0 if all pass, 1 if any fail)
    """
    force = "--force" in args
    no_fix = "--no-fix" in args

    # Extract target path (anything that's not a flag)
    target = DEFAULT_TARGET
    for arg in args:
        if not arg.startswith("-"):
            target = arg
            break

    print("=" * 60)
    print("  CDC Code Quality Check")
    print("=" * 60)

    exit_code = 0

    # 1. Pyright (type checking)
    exit_code |= _run_pyright(target)

    # 2. Ruff (linting + auto-fix)
    exit_code |= _run_ruff(target, force=force, fix=not no_fix)

    # 3. Black (formatting)
    exit_code |= _run_black(target, check_only=no_fix)

    # Summary
    print()
    print("=" * 60)
    if exit_code == 0:
        print("  ✅ All checks passed!")
    else:
        print("  ❌ Some checks failed (see above)")
    print("=" * 60)

    return exit_code


def _run_tool(name: str, cmd: List[str]) -> int:
    """Run a tool and return exit code."""
    print(f"\n{'─' * 60}")
    print(f"  🔍 {name}")
    print(f"{'─' * 60}")
    print(f"  $ {' '.join(cmd)}\n")

    try:
        result = subprocess.run(cmd)
        if result.returncode == 0:
            print(f"\n  ✅ {name}: passed")
        else:
            print(f"\n  ❌ {name}: failed (exit {result.returncode})")
        return result.returncode
    except FileNotFoundError:
        print(f"\n  ⚠️  {name}: not installed, skipping")
        return 0


def _run_pyright(target: str) -> int:
    """Run pyright type checker."""
    return _run_tool("Pyright (type checking)", ["pyright", target])


def _run_ruff(target: str, force: bool = False, fix: bool = True) -> int:
    """Run ruff linter with optional auto-fix."""
    cmd = ["ruff", "check"]
    if fix:
        cmd.append("--fix")
        if force:
            cmd.append("--unsafe-fixes")
    cmd.append(target)

    label = "Ruff (lint"
    if fix and force:
        label += " + unsafe auto-fix"
    elif fix:
        label += " + safe auto-fix"
    label += ")"

    return _run_tool(label, cmd)


def _run_black(target: str, check_only: bool = False) -> int:
    """Run black formatter."""
    cmd = ["black"]
    if check_only:
        cmd.append("--check")
    cmd.append(target)

    label = "Black (format check)" if check_only else "Black (format)"
    return _run_tool(label, cmd)
