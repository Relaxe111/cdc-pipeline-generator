"""Shared fixtures for end-to-end CLI tests.

Every CLI e2e test gets an isolated temporary directory to work in,
so tests never pollute each other or the real workspace.

Tests invoke the real ``cdc`` command through a **fish** shell subprocess,
exactly as a user would type in the dev container terminal.  This validates
the full chain: fish shell → ``cdc`` entry point → ``commands.py`` dispatch
→ subcommand module → handler.

The ``run_cdc_completion`` fixture additionally lets us assert that fish
autocompletions return the expected suggestions.

**Isolation:** If ``fish`` or the ``cdc`` entry point are not installed
(e.g. running outside the dev container, or in a bare CI environment),
every test that depends on ``run_cdc`` / ``run_cdc_completion`` is
automatically skipped with a clear reason.
"""

import os
import shlex
import shutil
import subprocess
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

# Type aliases for the callable fixtures.
RunCdc = Callable[..., subprocess.CompletedProcess[str]]
RunCdcCompletion = Callable[[str], subprocess.CompletedProcess[str]]

# ---------------------------------------------------------------------------
# Pre-flight checks (evaluated once at import time)
# ---------------------------------------------------------------------------

_FISH_AVAILABLE = shutil.which("fish") is not None
_CDC_AVAILABLE = shutil.which("cdc") is not None

_SKIP_REASON_FISH = "fish shell is not installed (CLI e2e tests require the dev container)"
_SKIP_REASON_CDC = "cdc entry point is not installed (run: pip install -e .)"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def isolated_project(tmp_path: Path) -> Iterator[Path]:
    """Create an isolated empty project directory and cd into it.

    Yields:
        Path to the temporary project root.

    After the test, the working directory is restored.
    """
    original_cwd = Path.cwd()
    os.chdir(tmp_path)
    try:
        yield tmp_path
    finally:
        os.chdir(original_cwd)


@pytest.fixture()
def run_cdc(isolated_project: Path) -> RunCdc:
    """Return a helper that invokes ``cdc <args>`` in a fish shell.

    The command runs through a real fish subprocess — the same way a user
    types commands in the dev container terminal.

    Tests that use this fixture are **automatically skipped** when ``fish``
    or ``cdc`` are not installed.

    Usage in tests::

        def test_scaffold(run_cdc: RunCdc) -> None:
            result = run_cdc("scaffold", "myproj", "--pattern", "db-shared")
            assert result.returncode == 0

    Returns:
        A callable ``(*args) -> CompletedProcess[str]``.
    """
    if not _FISH_AVAILABLE:
        pytest.skip(_SKIP_REASON_FISH)
    if not _CDC_AVAILABLE:
        pytest.skip(_SKIP_REASON_CDC)

    def _run(*args: str) -> subprocess.CompletedProcess[str]:
        quoted = " ".join(shlex.quote(a) for a in args)
        fish_cmd = f"cdc {quoted}"
        return subprocess.run(
            ["fish", "-c", fish_cmd],
            cwd=isolated_project,
            capture_output=True,
            text=True,
            check=False,
        )

    return _run


@pytest.fixture()
def run_cdc_completion() -> RunCdcCompletion:
    """Return a helper that queries fish autocompletions for ``cdc``.

    Uses ``complete -C '<partial>'`` to ask fish what completions it
    would offer for the given input — identical to pressing Tab.

    Tests that use this fixture are **automatically skipped** when ``fish``
    is not installed.

    Usage in tests::

        def test_completions(run_cdc_completion: RunCdcCompletion) -> None:
            result = run_cdc_completion("cdc scaff")
            assert "scaffold" in result.stdout

    Returns:
        A callable ``(partial_cmd) -> CompletedProcess[str]``.
        stdout contains one completion per line (word + tab + description).
    """
    if not _FISH_AVAILABLE:
        pytest.skip(_SKIP_REASON_FISH)

    def _complete(partial_cmd: str) -> subprocess.CompletedProcess[str]:
        fish_cmd = f"complete -C {shlex.quote(partial_cmd)}"
        return subprocess.run(
            ["fish", "-c", fish_cmd],
            capture_output=True,
            text=True,
            check=False,
        )

    return _complete
