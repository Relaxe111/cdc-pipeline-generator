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
from typing import NamedTuple

from cdc_generator.cli.commands import GENERATOR_COMMANDS, LOCAL_COMMANDS
from cdc_generator.helpers.helpers_logging import Colors

# ---------------------------------------------------------------------------
# Auto-discovered CLI commands
# ---------------------------------------------------------------------------

# Special library commands that are dispatched in _handle_special_commands()
# but don't appear in GENERATOR_COMMANDS or LOCAL_COMMANDS.
_SPECIAL_LIBRARY_COMMANDS: dict[str, str] = {
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


# Commands too generic to auto-match (every test name contains "test_")
_SKIP_PATTERN_COMMANDS: set[str] = {"test", "help"}


def _build_command_patterns() -> list[tuple[str, str]]:
    """Derive test-name → command mapping from discovered commands.

    Converts command names like ``manage-source-groups`` to patterns
    like ``manage_source_group`` (underscored, singular) so that test
    names such as ``test_cdc_manage_source_group_*`` match correctly.

    Longer patterns are sorted first so that ``manage_source_group``
    matches before a bare ``manage``.

    Commands in ``_SKIP_PATTERN_COMMANDS`` are excluded because their
    patterns (e.g. ``test``) would match every test function name.
    """
    patterns: list[tuple[str, str]] = []
    for cmd, _, _ in KNOWN_COMMANDS:
        if cmd in _SKIP_PATTERN_COMMANDS:
            continue
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

# CLI test files whose names don't match any command pattern naturally.
# Key = file stem (without .py), Value = cdc command.
_CLI_FILE_OVERRIDES: dict[str, str] = {
    "test_source_table": "manage-service",
}

# Unit-test mapping overrides for module names that don't naturally match
# the command name. Auto-derived patterns are added on top of these.
_UNIT_COMMAND_OVERRIDES: list[tuple[str, str]] = [
    ("setup_local", "setup-local"),
    ("server_group", "manage-source-groups"),
    ("sink_group", "manage-sink-groups"),
    ("service_schema", "manage-service-schema"),
    ("source_handler", "manage-service"),
    ("sink_handler", "manage-service"),
    ("sink_from", "manage-service"),
    ("sink_map", "manage-service"),
    ("inspect_handler", "manage-service"),
    ("list_create_misc", "manage-service"),
    ("template_validation", "manage-service"),
    ("service_parser", "manage-service"),
    ("source_table", "manage-service"),
    ("target_exists", "manage-service"),
    ("custom_table", "manage-service"),
    ("pg_schema", "manage-service"),
    ("dispatch", "manage-service"),
    ("column_template", "manage-column-templates"),
    ("transform_rule", "manage-column-templates"),
    ("structure_replicator", "manage-service"),
]


def _build_unit_command_patterns() -> list[tuple[str, str]]:
    """Build unit-test module-name patterns dynamically.

    Starts with explicit overrides, then auto-derives additional patterns
    from discovered command names to make new top-level commands visible
    without manual updates.
    """
    patterns: list[tuple[str, str]] = list(_UNIT_COMMAND_OVERRIDES)
    seen: set[tuple[str, str]] = set(patterns)

    for cmd, _, _ in KNOWN_COMMANDS:
        underscored = cmd.replace("-", "_")
        candidates = {underscored}

        if underscored.endswith("s") and underscored != underscored.rstrip("s"):
            candidates.add(underscored.rstrip("s"))

        if underscored.startswith("manage_"):
            candidates.add(underscored.removeprefix("manage_"))

        for candidate in candidates:
            pair = (candidate, cmd)
            if pair not in seen:
                patterns.append(pair)
                seen.add(pair)

    patterns.sort(key=lambda p: len(p[0]), reverse=True)
    return patterns


# Unit test file/module pattern → cdc command mapping (longest pattern first).
_UNIT_COMMAND_PATTERNS: list[tuple[str, str]] = _build_unit_command_patterns()


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
# Source-code analysis: estimate target test counts per command
# ---------------------------------------------------------------------------

# Command → list of source modules (relative to cdc_generator/).
# Each command is mapped to the entry module + handler/validator modules
# that make up its implementation.  Only testable modules are listed
# (i.e. not __init__.py, types.py, or template files).
_COMMAND_SOURCE_MODULES: dict[str, list[str]] = {
    "scaffold": [
        "cli/scaffold_command.py",
        "validators/manage_server_group/scaffolding/create.py",
        "validators/manage_server_group/scaffolding/pipeline_templates.py",
        "validators/manage_server_group/scaffolding/templates.py",
        "validators/manage_server_group/scaffolding/update.py",
        "validators/manage_server_group/scaffolding/vscode_settings.py",
    ],
    "generate": [
        "core/pipeline_generator.py",
    ],
    "manage-service": [
        "cli/service.py",
        "cli/service_handlers.py",
        "cli/service_handlers_bloblang.py",
        "cli/service_handlers_create.py",
        "cli/service_handlers_inspect.py",
        "cli/service_handlers_inspect_sink.py",
        "cli/service_handlers_list_source.py",
        "cli/service_handlers_misc.py",
        "cli/service_handlers_sink.py",
        "cli/service_handlers_sink_custom.py",
        "cli/service_handlers_source.py",
        "cli/service_handlers_templates.py",
        "cli/service_handlers_validation.py",
        "validators/manage_service/config.py",
        "validators/manage_service/bloblang_validator.py",
        "validators/manage_service/interactive_mode.py",
        "validators/manage_service/interactive.py",
        "validators/manage_service/mssql_inspector.py",
        "validators/manage_service/postgres_inspector.py",
        "validators/manage_service/schema_saver.py",
        "validators/manage_service/service_creator.py",
        "validators/manage_service/sink_inspector.py",
        "validators/manage_service/sink_operations.py",
        "validators/manage_service/sink_template_ops.py",
        "validators/manage_service/table_operations.py",
        "validators/manage_service/validation.py",
        "core/structure_replicator.py",
    ],
    "manage-source-groups": [
        "cli/server_group.py",
        "validators/manage_server_group/config.py",
        "validators/manage_server_group/display.py",
        "validators/manage_server_group/handlers.py",
        "validators/manage_server_group/stats_calculator.py",
        "validators/manage_server_group/type_introspector.py",
        "validators/manage_server_group/utils.py",
        "validators/manage_server_group/validation.py",
        "validators/manage_server_group/yaml_builder.py",
        "validators/manage_server_group/yaml_io.py",
        "validators/manage_server_group/yaml_writer.py",
    ],
    "manage-sink-groups": [
        "cli/sink_group.py",
        "validators/sink_group_validator.py",
    ],
    "manage-service-schema": [
        "cli/service_schema.py",
        "validators/manage_service_schema/custom_table_ops.py",
        "validators/manage_service_schema/type_definitions.py",
    ],
    "manage-column-templates": [
        "cli/column_templates.py",
        "core/column_template_definitions.py",
        "core/column_template_operations.py",
        "core/column_templates.py",
        "core/transform_rules.py",
    ],
    "setup-local": [
        "cli/setup_local.py",
    ],
}


class _SourceStats(NamedTuple):
    """Analysis results for a single source file."""

    functions: int
    branches: int


def _analyze_source_file(filepath: Path) -> _SourceStats:
    """Count public functions and branch points in a Python file.

    Public functions: module-level and class methods whose names
    don't start with ``_``.

    Branch points: ``if``, ``elif``, ``for``, ``while``, ``try``,
    ``with``, ternary ``IfExp``, and boolean operators ``and``/``or``.
    """
    try:
        tree = ast.parse(
            filepath.read_text(encoding="utf-8"),
            filename=str(filepath),
        )
    except (SyntaxError, FileNotFoundError):
        return _SourceStats(0, 0)

    functions = 0
    branches = 0
    func_types = (ast.FunctionDef, ast.AsyncFunctionDef)

    for node in ast.walk(tree):
        if isinstance(node, func_types):
            name: str = node.name
            if not name.startswith("_"):
                functions += 1
        elif isinstance(
            node,
            (ast.If, ast.For, ast.While, ast.Try, ast.IfExp, ast.BoolOp),
        ):
            branches += 1

    return _SourceStats(functions, branches)


def _resolve_command_source_modules(
    command: str,
    generator_root: Path,
) -> list[str]:
    """Resolve source modules for a command.

    Uses the explicit mapping first, and falls back to the command's
    primary script from ``GENERATOR_COMMANDS`` so new commands get
    an estimated target automatically.
    """
    explicit_modules = _COMMAND_SOURCE_MODULES.get(command)
    if explicit_modules:
        return explicit_modules

    cmd_info = GENERATOR_COMMANDS.get(command)
    if cmd_info is None:
        return []

    script_path = cmd_info.get("script")
    if not script_path:
        return []

    source_file = generator_root / script_path
    if source_file.exists():
        return [script_path]

    return []


def _compute_command_targets(
    generator_root: Path,
) -> dict[str, tuple[int, int, int]]:
    """Compute target test estimates per command.

    Returns a dict of ``command → (functions, branches, target)``.
    The *target* is estimated as ``functions x 2 + branches // 3``:
    each public function typically needs a happy-path test plus an
    error-path test, and roughly one test per 3 branch points covers
    the remaining conditional logic.
    """
    targets: dict[str, tuple[int, int, int]] = {}
    for cmd, _, is_lib in KNOWN_COMMANDS:
        if not is_lib:
            continue

        modules = _resolve_command_source_modules(cmd, generator_root)
        if not modules:
            continue

        total_fns = 0
        total_br = 0
        for mod_path in modules:
            stats = _analyze_source_file(generator_root / mod_path)
            total_fns += stats.functions
            total_br += stats.branches
        target = total_fns * 2 + total_br // 3
        targets[cmd] = (total_fns, total_br, target)
    return targets


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


def _classify_cli_test(test_name: str, filename: str = "") -> str:
    """Map a CLI test name to a cdc command.

    When *filename* is provided the combined
    ``{filename}::{test_name}`` string is searched, which lets the
    classifier pick up the command from the file stem (e.g.
    ``test_manage_service``) even when the individual test function
    name doesn't contain the command pattern.
    """
    combined = f"{filename}::{test_name}".lower()
    for pattern, command in COMMAND_PATTERNS:
        if pattern in combined:
            return command
    return "unknown"


def _classify_unit_module(module_label: str) -> str | None:
    """Map a unit-test module label to a cdc command, or ``None``.

    *module_label* is the human-readable label such as
    ``core/column_templates`` or ``server_group_dispatch``.
    """
    leaf = module_label.rsplit("/", 1)[-1]
    for pattern, command in _UNIT_COMMAND_PATTERNS:
        if pattern in leaf:
            return command
    return None


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
    covered_e2e: list[str] = field(default_factory=lambda: [])
    lib_pct: int = 0
    e2e_pct: int = 0


class CoverageReport:
    """Build and format the coverage report."""

    def __init__(self, tests_root: Path) -> None:
        self.tests_root = tests_root
        self.cli_dir = tests_root / "cli"
        self.generator_root = tests_root.parent / "cdc_generator"
        # command → list of test names
        self.cli_tests: dict[str, list[str]] = {}
        # module → list of test names
        self.unit_tests: dict[str, list[str]] = {}
        # command → list of unit test names (mapped from modules)
        self.unit_by_command: dict[str, list[str]] = {}
        # command → (functions, branches, target)
        self.command_targets: dict[str, tuple[int, int, int]] = {}

    def collect(self) -> None:
        """Scan test files and classify all tests."""
        self._collect_cli_tests()
        self._collect_unit_tests()
        self.command_targets = _compute_command_targets(
            self.generator_root,
        )

    def _collect_cli_tests(self) -> None:
        """Collect CLI e2e tests from tests/cli/."""
        if not self.cli_dir.is_dir():
            return

        for py_file in sorted(self.cli_dir.glob("test_*.py")):
            file_stem = py_file.stem
            file_override = _CLI_FILE_OVERRIDES.get(file_stem)
            tests = _collect_tests_from_file(py_file)
            for test in tests:
                command = file_override or _classify_cli_test(test, file_stem)
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

            # Also map into command buckets for the per-command table
            command = _classify_unit_module(module_label)
            if command:
                for test in tests:
                    self.unit_by_command.setdefault(command, []).append(
                        f"{module_label}::{test}"
                    )

    # -----------------------------------------------------------------------
    # Output
    # -----------------------------------------------------------------------

    def _compute_stats(self) -> _ReportStats:
        """Compute aggregate stats for the report."""
        total_cli = sum(len(v) for v in self.cli_tests.values())
        total_unit = sum(len(v) for v in self.unit_tests.values())
        # Only count commands that have a target or tests (skip meta-commands)
        lib_commands = [
            cmd for cmd, _, is_lib in KNOWN_COMMANDS
            if is_lib and (
                cmd in self.command_targets
                or cmd in self.cli_tests
                or cmd in self.unit_by_command
            )
        ]
        covered_e2e = [cmd for cmd in lib_commands if cmd in self.cli_tests]
        covered_lib = [
            cmd for cmd in lib_commands
            if cmd in self.cli_tests or cmd in self.unit_by_command
        ]
        lib_pct = (
            round(len(covered_lib) / len(lib_commands) * 100)
            if lib_commands
            else 0
        )
        e2e_pct = (
            round(len(covered_e2e) / len(lib_commands) * 100)
            if lib_commands
            else 0
        )
        return _ReportStats(
            total=total_cli + total_unit,
            total_cli=total_cli,
            total_unit=total_unit,
            lib_commands=lib_commands,
            covered_lib=covered_lib,
            covered_e2e=covered_e2e,
            lib_pct=lib_pct,
            e2e_pct=e2e_pct,
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
        c = Colors
        print()
        print(f"{c.DIM}{'=' * 80}{c.RESET}")
        print(f"  {c.BOLD}{c.CYAN}🧪 CDC Pipeline Generator — Test Coverage Report{c.RESET}")
        print(f"{c.DIM}{'=' * 80}{c.RESET}")
        print(
            f"\n  📊 Total tests: {c.BOLD}{s.total}{c.RESET}"
            f"  (CLI e2e: {c.CYAN}{s.total_cli}{c.RESET},"
            f" Unit: {c.CYAN}{s.total_unit}{c.RESET})"
        )
        total_lib = len(s.lib_commands)
        lib_color = c.GREEN if s.lib_pct >= 80 else c.YELLOW if s.lib_pct >= 50 else c.RED
        e2e_color = c.GREEN if s.e2e_pct >= 80 else c.YELLOW if s.e2e_pct >= 50 else c.RED
        print(
            f"  📦 Library commands covered: "
            f"{lib_color}{len(s.covered_lib)}/{total_lib} ({s.lib_pct}%){c.RESET} overall, "
            f"{e2e_color}{len(s.covered_e2e)}/{total_lib} ({s.e2e_pct}%){c.RESET} e2e"
        )
        print()

    def _print_cli_section(self, verbose: bool) -> None:
        """Print tests grouped by library command."""
        c = Colors
        print(f"{c.DIM}{'-' * 80}{c.RESET}")
        print(f"  {c.BOLD}🔧 TESTS BY CDC COMMAND{c.RESET}")
        print(f"{c.DIM}{'-' * 80}{c.RESET}")
        print(
            f"  {c.DIM}{'Command':<31} {'E2E':>5} {'Unit':>6}"
            f" {'Total':>6} {'Target':>7} {'Progress':>9}{c.RESET}"
        )
        print(
            f"  {c.DIM}{'─' * 31} {'─' * 5} {'─' * 6}"
            f" {'─' * 6} {'─' * 7} {'─' * 9}{c.RESET}"
        )

        for cmd, _, is_lib in KNOWN_COMMANDS:
            if not is_lib:
                continue
            e2e = self.cli_tests.get(cmd, [])
            unit = self.unit_by_command.get(cmd, [])
            total = len(e2e) + len(unit)
            _fns, _br, target = self.command_targets.get(
                cmd, (0, 0, 0),
            )

            # Hide meta-commands with no target and no tests
            if target == 0 and total == 0:
                continue

            pct = 0
            if target > 0:
                pct = min(round(total / target * 100), 100)
                pct_color = c.GREEN if pct >= 80 else c.YELLOW if pct >= 50 else c.RED
                progress = f"{pct_color}{pct}%{c.RESET}"
                # Pad manually since ANSI codes break alignment
                progress = f"{' ' * (9 - len(f'{pct}%'))}{progress}"
                row_icon = "✅" if pct >= 80 else "🔶" if pct >= 50 else "❌"
            elif total > 0:
                progress = f"       {c.GREEN}✅{c.RESET}"
                row_icon = "✅"
            else:
                progress = f"       {c.DIM}—{c.RESET}"
                row_icon = "⬜"

            total_color = c.GREEN if total > 0 else c.DIM
            target_str = str(target) if target > 0 else "—"
            cmd_color = c.GREEN if total > 0 else c.RED
            print(
                f"  {row_icon} {cmd_color}cdc {cmd:<24}{c.RESET} {len(e2e):>5}"
                f" {len(unit):>6} {total_color}{total:>6}{c.RESET}"
                f" {target_str:>7} {progress}"
            )

            if verbose and e2e:
                for test in e2e:
                    print(f"      {c.DIM}• {test}{c.RESET}")

    def _print_local_section(self, verbose: bool) -> None:
        """Print local/script command coverage (if any are tested)."""
        c = Colors
        local_commands = [cmd for cmd, _, is_lib in KNOWN_COMMANDS if not is_lib]
        command_col_width = max(
            len("Local/Script Commands"),
            *(len(f"cdc {cmd}") for cmd in local_commands),
        )

        print()
        print(
            f"  {c.DIM}{'Local/Script Commands':<{command_col_width}}"
            f" {'Tests':>6}  {'Status':<10}{c.RESET}"
        )
        print(
            f"  {c.DIM}{'─' * command_col_width}"
            f" {'─' * 6}  {'─' * 10}{c.RESET}"
        )
        for cmd in local_commands:
            tests = self.cli_tests.get(cmd, [])
            count = len(tests)
            if count:
                status = f"{c.GREEN}✅ {count} tests{c.RESET}"
            else:
                status = f"{c.RED}❌ 0 tests{c.RESET}"
            print(
                f"  {'cdc ' + cmd:<{command_col_width}}"
                f" {count:>6}  {status}"
            )

    def _print_unit_section(self, verbose: bool) -> None:
        """Print unit tests grouped by module."""
        c = Colors
        module_col_width = max(
            len("Module"),
            *(len(module) for module in self.unit_tests),
        )
        print()
        print(f"{c.DIM}{'-' * 80}{c.RESET}")
        print(f"  {c.BOLD}🧩 UNIT TESTS (by module){c.RESET}")
        print(f"{c.DIM}{'-' * 80}{c.RESET}")
        print(
            f"  {c.DIM}{'Module':<{module_col_width}} {'Tests':>6}"
            f"  {'Command':<22}{c.RESET}"
        )
        print(
            f"  {c.DIM}{'─' * module_col_width}"
            f" {'─' * 6}  {'─' * 22}{c.RESET}"
        )

        for module, tests in sorted(self.unit_tests.items()):
            cmd = _classify_unit_module(module) or "—"
            cmd_display = f"{c.CYAN}{cmd}{c.RESET}" if cmd != "—" else f"{c.DIM}—{c.RESET}"
            print(f"  {module:<{module_col_width}} {len(tests):>6}  {cmd_display}")
            if verbose:
                for test in tests:
                    print(f"      {c.DIM}• {test}{c.RESET}")

    @staticmethod
    def _color_bar(filled: int, total: int, pct: int) -> str:
        """Build a colored progress bar string."""
        c = Colors
        bar_color = c.GREEN if pct >= 80 else c.YELLOW if pct >= 50 else c.RED
        bar = (
            f"{bar_color}{'█' * filled}{c.RESET}"
            f"{c.DIM}{'░' * (total - filled)}{c.RESET}"
        )
        pct_color = bar_color
        return f"[{bar}] {pct_color}{pct}%{c.RESET}"

    def _print_summary_bar(self, s: _ReportStats) -> None:
        """Print the summary bar chart and final totals."""
        c = Colors
        print()
        print(f"{c.DIM}{'-' * 80}{c.RESET}")
        print(f"  {c.BOLD}📈 COVERAGE SUMMARY{c.RESET}")
        print(f"{c.DIM}{'-' * 80}{c.RESET}")

        bar_width = 40

        # Overall coverage (e2e + unit)
        filled = round(s.lib_pct / 100 * bar_width)
        bar = self._color_bar(filled, bar_width, s.lib_pct)
        print(f"\n  📦 Commands covered:  {bar}")
        print(
            f"     ✅ {c.GREEN}"
            f"{', '.join(s.covered_lib) or '(none)'}{c.RESET}"
        )

        # E2E only coverage
        e2e_filled = round(s.e2e_pct / 100 * bar_width)
        e2e_bar = self._color_bar(e2e_filled, bar_width, s.e2e_pct)
        print(f"  🔗 E2E only:          {e2e_bar}")
        print(
            f"     ✅ {c.GREEN}"
            f"{', '.join(s.covered_e2e) or '(none)'}{c.RESET}"
        )

        # Test progress vs target
        total_target = sum(
            t for _, _, t in self.command_targets.values()
        )
        if total_target > 0:
            target_pct = min(
                round(s.total / total_target * 100), 100,
            )
            target_filled = round(target_pct / 100 * bar_width)
            target_bar = self._color_bar(
                target_filled, bar_width, target_pct,
            )
            pct_color = (
                c.GREEN if target_pct >= 80
                else c.YELLOW if target_pct >= 50
                else c.RED
            )
            print(
                f"  🎯 Test progress:     {target_bar}"
                f" {pct_color}({s.total}/{total_target}){c.RESET}"
            )

        uncovered = [
            cmd for cmd in s.lib_commands if cmd not in s.covered_lib
        ]
        if uncovered:
            print(
                f"     ❌ {c.RED}"
                f"{', '.join(uncovered)}{c.RESET}"
            )

        print(
            f"\n  🏁 Total:  {c.BOLD}{s.total}{c.RESET} tests"
            f" ({c.CYAN}{s.total_cli}{c.RESET} CLI e2e"
            f" + {c.CYAN}{s.total_unit}{c.RESET} unit)"
        )
        print()
        print(f"{c.DIM}{'=' * 80}{c.RESET}")
        print()

    def to_json(self) -> str:
        """Return JSON representation of the report."""
        lib_commands = [c for c, _, is_lib in KNOWN_COMMANDS if is_lib]
        covered_e2e = [c for c in lib_commands if c in self.cli_tests]
        covered_any = [
            c for c in lib_commands
            if c in self.cli_tests or c in self.unit_by_command
        ]

        return json.dumps(
            {
                "total_tests": sum(len(v) for v in self.cli_tests.values())
                + sum(len(v) for v in self.unit_tests.values()),
                "cli_e2e_tests": sum(
                    len(v) for v in self.cli_tests.values()
                ),
                "unit_tests": sum(
                    len(v) for v in self.unit_tests.values()
                ),
                "library_commands_total": len(lib_commands),
                "library_commands_covered": len(covered_any),
                "library_commands_e2e_covered": len(covered_e2e),
                "library_coverage_pct": round(
                    len(covered_any) / len(lib_commands) * 100
                )
                if lib_commands
                else 0,
                "by_command": {
                    cmd: {
                        "e2e_count": len(
                            self.cli_tests.get(cmd, [])
                        ),
                        "unit_count": len(
                            self.unit_by_command.get(cmd, [])
                        ),
                        "total": len(self.cli_tests.get(cmd, []))
                        + len(self.unit_by_command.get(cmd, [])),
                        "target": self.command_targets.get(
                            cmd, (0, 0, 0),
                        )[2],
                        "source_functions": self.command_targets.get(
                            cmd, (0, 0, 0),
                        )[0],
                        "source_branches": self.command_targets.get(
                            cmd, (0, 0, 0),
                        )[1],
                        "e2e_tests": self.cli_tests.get(cmd, []),
                    }
                    for cmd, _, _ in KNOWN_COMMANDS
                },
                "unit_by_module": {
                    mod: {
                        "count": len(tests),
                        "command": _classify_unit_module(mod),
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
