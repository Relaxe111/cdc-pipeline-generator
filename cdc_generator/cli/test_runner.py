#!/usr/bin/env python3
"""CDC test runner command.

Runs project tests via pytest with category-based filtering.

Usage:
    cdc test              # Run all unit tests (tests/*.py)
    cdc test --cli        # Run end-to-end CLI tests (tests/cli/*.py)
    cdc test --all        # Run all tests (unit + CLI e2e)
    cdc test -v           # Verbose output (passed to pytest)
    cdc test -k scaffold  # Filter by name (passed to pytest)
"""

import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_UNIT_TESTS_DIR = "tests"
_CLI_TESTS_DIR = "tests/cli"
_MARKER_CLI = "cli"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_project_root() -> Path:
    """Find project root by locating pyproject.toml.

    Returns:
        Path to project root.

    Raises:
        SystemExit: If project root cannot be found.
    """
    # In dev container, /workspace is always the generator root
    workspace = Path("/workspace")
    if workspace.is_dir() and (workspace / "pyproject.toml").exists():
        return workspace

    # Walk up from current file
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent

    print("❌ Cannot find project root (no pyproject.toml found)")
    sys.exit(1)


def _build_pytest_args(
    extra_args: list[str],
    project_root: Path,
) -> list[str]:
    """Build pytest command-line arguments.

    Args:
        extra_args: User-provided CLI args after 'cdc test'.
        project_root: Path to project root.

    Returns:
        List of pytest arguments.
    """
    # Separate our flags from pytest pass-through flags
    run_cli = False
    run_all = False
    pytest_passthrough: list[str] = []

    for arg in extra_args:
        if arg == "--cli":
            run_cli = True
        elif arg == "--all":
            run_all = True
        else:
            pytest_passthrough.append(arg)

    # Determine test directories
    if run_all:
        test_dirs = [str(project_root / _UNIT_TESTS_DIR)]
    elif run_cli:
        test_dirs = [str(project_root / _CLI_TESTS_DIR)]
    else:
        # Default: unit tests only (exclude cli/ subdirectory)
        test_dirs = [str(project_root / _UNIT_TESTS_DIR)]
        # Add marker deselection if cli tests exist
        cli_dir = project_root / _CLI_TESTS_DIR
        if cli_dir.is_dir():
            pytest_passthrough.extend(["-m", f"not {_MARKER_CLI}"])

    return [
        sys.executable, "-m", "pytest",
        *test_dirs,
        *pytest_passthrough,
    ]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    """Run CDC tests.

    Returns:
        Exit code from pytest.
    """
    project_root = _find_project_root()
    extra_args = sys.argv[1:]

    cmd = _build_pytest_args(extra_args, project_root)

    result = subprocess.run(cmd, cwd=project_root, check=False)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
