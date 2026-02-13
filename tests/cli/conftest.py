"""Shared fixtures for end-to-end CLI tests.

Every CLI e2e test gets an isolated temporary directory to work in,
so tests never pollute each other or the real workspace.

Tests invoke the real ``cdc`` command through a **fish** shell subprocess,
exactly as a user would type in the dev container terminal.  This validates
the full chain: fish shell → ``cdc`` entry point → ``commands.py`` dispatch
→ subcommand module → handler.

The ``run_cdc_completion`` fixture uses Click's ``ShellComplete`` API to
query completions at the Python level — the same logic that drives the
runtime ``_CDC_COMPLETE`` protocol.  Tests don't need fish installed.

**Isolation:** If ``fish`` or the ``cdc`` entry point are not installed
(e.g. running outside the dev container, or in a bare CI environment),
every test that depends on ``run_cdc`` is automatically skipped with a
clear reason.  ``run_cdc_completion`` never requires fish because it
works at the Python/Click level.
"""

import os
import shlex
import shutil
import subprocess
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from click.shell_completion import ShellComplete

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
    """Return a helper that queries Click completions for ``cdc``.

    Uses Click's ``ShellComplete`` API directly — the same logic that
    drives the runtime ``_CDC_COMPLETE`` protocol in the dev container.
    No fish shell required; works in CI and locally.

    The returned callable accepts a partial command string (e.g.
    ``"cdc manage-services config --ser"``) and returns a
    ``CompletedProcess``-like object whose ``.stdout`` contains one
    completion per line (``value\\thelp``), matching the fish format.

    Usage in tests::

        def test_completions(run_cdc_completion: RunCdcCompletion) -> None:
            result = run_cdc_completion("cdc manage-services config --ser")
            assert "--service" in result.stdout

    Returns:
        A callable ``(partial_cmd) -> CompletedProcess[str]``.
    """
    from cdc_generator.cli.commands import _click_cli

    def _complete(partial_cmd: str) -> subprocess.CompletedProcess[str]:
        parts = shlex.split(partial_cmd)
        # Drop the program name ("cdc") — Click handles it via prog_name.
        if parts and parts[0] == "cdc":
            parts = parts[1:]

        # The last token is the incomplete word being typed.
        if parts:
            incomplete = parts[-1]
            args = parts[:-1]
        else:
            incomplete = ""
            args = []

        comp = ShellComplete(_click_cli, {}, "cdc", "_CDC_COMPLETE")
        completions = comp.get_completions(args, incomplete)
        # Format like fish: "value\thelp_text\n"
        lines = [
            f"{c.value}\t{c.help or ''}" if c.help else c.value
            for c in completions
        ]
        stdout = "\n".join(lines) + "\n" if lines else ""
        return subprocess.CompletedProcess(
            args=["click-complete", partial_cmd],
            returncode=0,
            stdout=stdout,
            stderr="",
        )

    return _complete
