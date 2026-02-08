#!/usr/bin/env python3
"""CLI test coverage report.

Scans test files in tests/ and tests/cli/ and produces a summary grouped
by ``cdc`` subcommand, showing how many test scenarios exist for each
command and what percentage of the known command surface is covered.

Commands and their descriptions are **auto-discovered** from
``cdc_generator.cli.commands`` (GENERATOR_COMMANDS / LOCAL_COMMANDS) and
the special-command dispatch, so adding a new ``cdc`` subcommand is
automatically reflected here — no manual list maintenance needed.

Usage (from the dev container):

    python tests/cli/coverage_report.py          # summary
    python tests/cli/coverage_report.py -v       # show every test name
    python tests/cli/coverage_report.py --json   # machine-readable output
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

from cdc_generator.cli.commands import GENERATOR_COMMANDS, LOCAL_COMMANDS

# ---------------------------------------------------------------------------
# Auto-discovered CLI commands
# ---------------------------------------------------------------------------

# Special library commands that are dispatched in _handle_special_commands()
# but don't appear in GENERATOR_COMMANDS or LOCAL_COMMANDS.
_SPECIAL_LIBRARY_COMMANDS: dict[str, str] = {
    "init": "Initialize a new CDC pipeline project (deprecated)",
    "test": "Run project tests",
    "test-coverage": "Show test coverage report by cdc command",
    "help": "Show help message",
}


def _discover_commands() -> list[tuple[str, str, bool]]:
    """Build the known-commands list from authoritative sources.

    Returns a list of ``(command, description, is_library)`` tuples,
    sourced from ``GENERATOR_COMMANDS``, ``LOCAL_COMMANDS`` and the
    small ``_SPECIAL_LIBRARY_COMMANDS`` set.
    """
    commands: list[tuple[str, str, bool]] = []

    # Library commands from GENERATOR_COMMANDS
    for cmd, info in GENERATOR_COMMANDS.items():
        commands.append((cmd, info["description"], True))

    # Special library commands not in GENERATOR_COMMANDS
    for cmd, desc in _SPECIAL_LIBRARY_COMMANDS.items():
        if cmd not in GENERATOR_COMMANDS:
            commands.append((cmd, desc, True))

    # Local/script commands from LOCAL_COMMANDS
    for cmd, info in LOCAL_COMMANDS.items():
        commands.append((cmd, info["description"], False))

    return commands


# Built once at import time
KNOWN_COMMANDS: list[tuple[str, str, bool]] = _discover_commands()


# ---------------------------------------------------------------------------
# Mapping: test name pattern → cdc command (auto-generated)
# ---------------------------------------------------------------------------


def _build_command_patterns() -> list[tuple[str, str]]:
    """Derive test-name → command mapping from discovered commands.

    Converts command names like ``manage-source-groups`` to patterns
    like ``manage_source_group`` (underscored, singular) so that test
    names such as ``test_cdc_manage_source_group_*`` match correctly.

    Longer patterns are sorted first so that ``manage_source_group``
    matches before a bare ``manage``.
    """
    patterns: list[tuple[str, str]] = []
    for cmd, _, _ in KNOWN_COMMANDS:
        # "manage-source-groups" → "manage_source_groups"
        underscored = cmd.replace("-", "_")
        patterns.append((underscored, cmd))

        # Also add singular form: "manage_source_groups" → "manage_source_group"
        if underscored.endswith("s") and underscored != underscored.rstrip("s"):
            singular = underscored.rstrip("s")
            patterns.append((singular, cmd))

    # Longest pattern first → most specific wins
    patterns.sort(key=lambda p: len(p[0]), reverse=True)
    return patterns


COMMAND_PATTERNS: list[tuple[str, str]] = _build_command_patterns()


# ---------------------------------------------------------------------------
# Unit-test module → cdc subsystem mapping (auto-discovered)
# ---------------------------------------------------------------------------


def _discover_unit_module_mapping(tests_root: Path) -> dict[str, str]:
    """Build unit-test module labels from test file names.

    Derives a human-readable label from the filename:
    - ``test_column_templates.py`` → ``core/column_templates``
      (if cdc_generator/core/column_templates.py exists)
    - Falls back to the stem without ``test_`` prefix.
    """
    mapping: dict[str, str] = {}
    generator_root = tests_root.parent / "cdc_generator"

    for py_file in sorted(tests_root.glob("test_*.py")):
        stem = py_file.stem  # e.g. "test_column_templates"
        module_name = stem.removeprefix("test_")  # e.g. "column_templates"

        # Try to find the real module path under cdc_generator/
        label = _find_module_label(generator_root, module_name)
        mapping[stem] = label

    return mapping


def _find_module_label(generator_root: Path, module_name: str) -> str:
    """Resolve a module name to its path under cdc_generator/.

    Searches ``core/``, ``helpers/``, ``cli/``, ``validators/`` subdirs
    (including nested directories).  Returns a path like
    ``core/column_templates`` or falls back to the raw module name.
    """
    search_dirs = ["core", "helpers", "cli", "validators"]
    for subdir in search_dirs:
        # Direct match: cdc_generator/<subdir>/<module>.py
        candidate = generator_root / subdir / f"{module_name}.py"
        if candidate.exists():
            return f"{subdir}/{module_name}"

        # Recursive match: cdc_generator/<subdir>/**/<module>.py
        for match in (generator_root / subdir).rglob(f"{module_name}.py"):
            relative = match.relative_to(generator_root)
            return str(relative.with_suffix(""))

    return module_name


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _collect_tests_from_file(filepath: Path) -> list[str]:
    """Return list of fully-qualified test names from a Python file.

    Format: ``ClassName::method`` for methods inside classes,
    or just ``function_name`` for module-level test functions.
    """
    try:
        tree = ast.parse(filepath.read_text(encoding="utf-8"), filename=str(filepath))
    except SyntaxError:
        return []

    tests: list[str] = []
    _func_types = (ast.FunctionDef, ast.AsyncFunctionDef)
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                if isinstance(item, _func_types) and item.name.startswith("test_"):
                    tests.append(f"{node.name}::{item.name}")
        elif (
            isinstance(node, _func_types)
            and node.name.startswith("test_")
            and not _is_inside_class(tree, node)
        ):
            tests.append(node.name)

    return tests


def _is_inside_class(tree: ast.Module, func_node: ast.AST) -> bool:
    """Check if a function node is nested inside a class."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                if item is func_node:
                    return True
    return False


def _classify_cli_test(test_name: str) -> str:
    """Map a CLI test name to a cdc command."""
    lower = test_name.lower()
    for pattern, command in COMMAND_PATTERNS:
        if pattern in lower:
            return command
    return "unknown"


# ---------------------------------------------------------------------------
# Report building
# ---------------------------------------------------------------------------


@dataclass
class _ReportStats:
    """Aggregated report statistics passed between print methods."""

    total: int
    total_cli: int
    total_unit: int
    lib_commands: list[str] = field(default_factory=lambda: [])
    covered_lib: list[str] = field(default_factory=lambda: [])
    lib_pct: int = 0


class CoverageReport:
    """Build and format the coverage report."""

    def __init__(self, tests_root: Path) -> None:
        self.tests_root = tests_root
        self.cli_dir = tests_root / "cli"
        # command → list of test names
        self.cli_tests: dict[str, list[str]] = {}
        # module → list of test names
        self.unit_tests: dict[str, list[str]] = {}

    def collect(self) -> None:
        """Scan test files and classify all tests."""
        self._collect_cli_tests()
        self._collect_unit_tests()

    def _collect_cli_tests(self) -> None:
        """Collect CLI e2e tests from tests/cli/."""
        if not self.cli_dir.is_dir():
            return

        for py_file in sorted(self.cli_dir.glob("test_*.py")):
            tests = _collect_tests_from_file(py_file)
            for test in tests:
                command = _classify_cli_test(test)
                self.cli_tests.setdefault(command, []).append(
                    f"{py_file.name}::{test}"
                )

    def _collect_unit_tests(self) -> None:
        """Collect unit tests from tests/."""
        module_mapping = _discover_unit_module_mapping(self.tests_root)
        for py_file in sorted(self.tests_root.glob("test_*.py")):
            stem = py_file.stem
            module_label = module_mapping.get(stem, stem)
            tests = _collect_tests_from_file(py_file)
            self.unit_tests[module_label] = tests

    # -----------------------------------------------------------------------
    # Output
    # -----------------------------------------------------------------------

    def _compute_stats(self) -> _ReportStats:
        """Compute aggregate stats for the report."""
        total_cli = sum(len(v) for v in self.cli_tests.values())
        total_unit = sum(len(v) for v in self.unit_tests.values())
        lib_commands = [c for c, _, is_lib in KNOWN_COMMANDS if is_lib]
        covered_lib = [c for c in lib_commands if c in self.cli_tests]
        lib_pct = (
            round(len(covered_lib) / len(lib_commands) * 100)
            if lib_commands
            else 0
        )
        return _ReportStats(
            total=total_cli + total_unit,
            total_cli=total_cli,
            total_unit=total_unit,
            lib_commands=lib_commands,
            covered_lib=covered_lib,
            lib_pct=lib_pct,
        )

    def print_summary(self, *, verbose: bool = False) -> None:
        """Print human-readable coverage report."""
        stats = self._compute_stats()
        self._print_header(stats)
        self._print_cli_section(verbose)
        self._print_local_section(verbose)
        self._print_unit_section(verbose)
        self._print_summary_bar(stats)

    def _print_header(self, s: _ReportStats) -> None:
        """Print report header with totals."""
        print()
        print("=" * 72)
        print("  CDC Pipeline Generator — Test Coverage Report")
        print("=" * 72)
        print(f"\n  Total tests: {s.total}  (CLI e2e: {s.total_cli}, Unit: {s.total_unit})")
        covered = len(s.covered_lib)
        total_lib = len(s.lib_commands)
        print(f"  Library commands covered: {covered}/{total_lib} ({s.lib_pct}%)")
        print()

    def _print_cli_section(self, verbose: bool) -> None:
        """Print CLI e2e tests grouped by library command."""
        print("-" * 72)
        print("  CLI E2E TESTS (by cdc command)")
        print("-" * 72)
        print(f"  {'Command':<30} {'Tests':>6}  {'Status':<10}")
        print(f"  {'─' * 30} {'─' * 6}  {'─' * 10}")

        for cmd, _, is_lib in KNOWN_COMMANDS:
            if not is_lib:
                continue
            tests = self.cli_tests.get(cmd, [])
            count = len(tests)
            status = f"✅ {count} tests" if count > 0 else "❌ none"
            print(f"  cdc {cmd:<25} {count:>6}  {status:<10}")

            if verbose and tests:
                for test in tests:
                    print(f"      • {test}")

    def _print_local_section(self, verbose: bool) -> None:
        """Print local/script command coverage (if any are tested)."""
        local_commands = [c for c, _, is_lib in KNOWN_COMMANDS if not is_lib]
        tested_local = [c for c in local_commands if c in self.cli_tests]

        if not tested_local and not verbose:
            return

        print()
        print(f"  {'Local/Script Commands':<30} {'Tests':>6}  {'Status':<10}")
        print(f"  {'─' * 30} {'─' * 6}  {'─' * 10}")
        for cmd, _, is_lib in KNOWN_COMMANDS:
            if is_lib:
                continue
            tests = self.cli_tests.get(cmd, [])
            count = len(tests)
            if count > 0 or verbose:
                status = f"✅ {count} tests" if count else "⊘ n/a"
                print(f"  cdc {cmd:<25} {count:>6}  {status:<10}")

    def _print_unit_section(self, verbose: bool) -> None:
        """Print unit tests grouped by module."""
        print()
        print("-" * 72)
        print("  UNIT TESTS (by module)")
        print("-" * 72)
        print(f"  {'Module':<40} {'Tests':>6}")
        print(f"  {'─' * 40} {'─' * 6}")

        for module, tests in sorted(self.unit_tests.items()):
            print(f"  {module:<40} {len(tests):>6}")
            if verbose:
                for test in tests:
                    print(f"      • {test}")

    def _print_summary_bar(self, s: _ReportStats) -> None:
        """Print the summary bar chart and final totals."""
        print()
        print("-" * 72)
        print("  COVERAGE SUMMARY")
        print("-" * 72)

        bar_width = 40
        filled = round(s.lib_pct / 100 * bar_width)
        bar = "█" * filled + "░" * (bar_width - filled)
        print(f"\n  Library CLI commands:  [{bar}] {s.lib_pct}%")
        print(f"  Covered: {', '.join(s.covered_lib) or '(none)'}")

        uncovered = [c for c in s.lib_commands if c not in self.cli_tests]
        if uncovered:
            print(f"  Missing:  {', '.join(uncovered)}")

        print(f"\n  Total:  {s.total} tests ({s.total_cli} CLI e2e + {s.total_unit} unit)")
        print()
        print("=" * 72)
        print()

    def to_json(self) -> str:
        """Return JSON representation of the report."""
        lib_commands = [c for c, _, is_lib in KNOWN_COMMANDS if is_lib]
        covered = [c for c in lib_commands if c in self.cli_tests]

        return json.dumps(
            {
                "total_tests": sum(len(v) for v in self.cli_tests.values())
                + sum(len(v) for v in self.unit_tests.values()),
                "cli_e2e_tests": sum(len(v) for v in self.cli_tests.values()),
                "unit_tests": sum(len(v) for v in self.unit_tests.values()),
                "library_commands_total": len(lib_commands),
                "library_commands_covered": len(covered),
                "library_coverage_pct": round(
                    len(covered) / len(lib_commands) * 100
                )
                if lib_commands
                else 0,
                "by_command": {
                    cmd: {
                        "count": len(self.cli_tests.get(cmd, [])),
                        "tests": self.cli_tests.get(cmd, []),
                    }
                    for cmd, _, _ in KNOWN_COMMANDS
                },
                "unit_by_module": {
                    mod: {
                        "count": len(tests),
                        "tests": tests,
                    }
                    for mod, tests in self.unit_tests.items()
                },
            },
            indent=2,
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    """Run the coverage report."""
    parser = argparse.ArgumentParser(
        description="CDC CLI test coverage report",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show every test name",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    args = parser.parse_args()

    # Find the tests/ root relative to this script
    script_dir = Path(__file__).resolve().parent  # tests/cli/
    tests_root = script_dir.parent  # tests/

    report = CoverageReport(tests_root)
    report.collect()

    if args.json:
        print(report.to_json())
    else:
        report.print_summary(verbose=args.verbose)

    return 0


if __name__ == "__main__":
    sys.exit(main())
