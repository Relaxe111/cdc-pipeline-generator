#!/usr/bin/env python3
"""CDC Pipeline CLI - Main Entry Point.

This CLI runs inside a Docker dev container with all dependencies pre-installed.

Usage:
    cdc <command> [options]

Commands:
    scaffold              Scaffold a new CDC pipeline project with source group configuration
    manage-services      Manage service config/schema commands
    manage-source-groups Manage source groups configuration
    manage-sink-groups   Manage sink groups configuration
    manage-pipelines     Manage pipeline lifecycle commands
    manage-migrations    Manage migration and DB lifecycle commands
    setup-local          Set up local development environment
    generate-usage-stats Generate command usage stats from _docs/_stats user files
    help                 Show this help message
    test                 Run project tests (unit and CLI e2e)
"""

from __future__ import annotations

import contextlib
import subprocess
import sys
from pathlib import Path

import click

from cdc_generator.cli.usage_stats import track_usage

# Minimum number of CLI args (program name + command)
_MIN_ARGS = 2

# Type alias for script paths dictionary
ScriptPaths = dict[str, Path | None]


def _detect_dev_container(cwd: Path) -> tuple[Path, str | None, bool] | None:
    """Check if running inside the dev container and return env tuple, or None."""
    if cwd.parts[:2] == ("/", "workspace"):
        return Path("/workspace"), None, True
    if cwd.parts[:3] == ("/", "implementations", "adopus"):
        return Path("/implementations/adopus"), "adopus", True
    if cwd.parts[:3] == ("/", "implementations", "asma"):
        return Path("/implementations/asma"), "asma", True
    return None


def _impl_name_from_path(path: Path) -> str | None:
    """Extract implementation name from a filesystem path."""
    path_str = str(path)
    if "adopus-cdc-pipeline" in path_str:
        return "adopus"
    if "asma-cdc-pipeline" in path_str:
        return "asma"
    return None


def detect_environment() -> tuple[Path, str | None, bool]:
    """Detect the current working environment.

    Returns:
        tuple: (workspace_root, implementation_name, is_dev_container)
            - workspace_root: Path to workspace root (generator or implementation)
            - implementation_name: 'adopus' or 'asma' or None
            - is_dev_container: True if running inside dev container
    """
    cwd = Path.cwd()

    # Check dev container first
    dev_result = _detect_dev_container(cwd)
    if dev_result is not None:
        return dev_result

    # Check if we're in an implementation directory on host
    if (cwd / "source-groups.yaml").exists():
        return cwd, _impl_name_from_path(cwd), False

    # Try to find implementation root by walking up
    current = cwd
    while current != current.parent:
        if (current / "source-groups.yaml").exists():
            return current, _impl_name_from_path(current), False
        current = current.parent

    # Fallback: assume we're in current directory
    return cwd, None, False


def get_script_paths(workspace_root: Path, is_dev_container: bool) -> ScriptPaths:
    """Get paths to scripts based on environment.

    Args:
        workspace_root: Path to workspace root
        is_dev_container: Whether running in dev container

    Returns:
        Mapping of script types to paths
    """
    if is_dev_container:
        generator_candidate = Path("/workspace") / "cdc_generator"
        if generator_candidate.exists() and (generator_candidate / "cli").exists():
            return {
                "generator": generator_candidate,
                "scripts": workspace_root / "scripts",
                "root": workspace_root,
            }
        return {
            "generator": None,
            "scripts": workspace_root / "scripts",
            "root": workspace_root,
        }

    # On host - look for generator as sibling directory
    generator_candidate = workspace_root.parent / "cdc-pipeline-generator"
    if generator_candidate.exists():
        return {
            "generator": generator_candidate / "cdc_generator",
            "scripts": workspace_root / "scripts",
            "root": workspace_root,
        }

    return {
        "generator": None,
        "scripts": workspace_root / "scripts",
        "root": workspace_root,
    }


# Commands that use generator library
GENERATOR_COMMANDS: dict[str, dict[str, str]] = {
    "scaffold": {
        "module": "cdc_generator.cli.scaffold_command",
        "script": "cli/scaffold_command.py",
        "description": "Scaffold a new CDC pipeline project with source group configuration",
    },
    "manage-source-groups": {
        "module": "cdc_generator.cli.source_group",
        "script": "cli/source_group.py",
        "description": "Manage source groups configuration (source-groups.yaml)",
    },
    "manage-sink-groups": {
        "module": "cdc_generator.cli.sink_group",
        "script": "cli/sink_group.py",
        "description": "Manage sink groups configuration (sink-groups.yaml)",
    },
    "setup-local": {
        "module": "cdc_generator.cli.setup_local",
        "script": "cli/setup_local.py",
        "description": "Set up local development environment with on-demand services",
    },
    "generate-usage-stats": {
        "module": "cdc_generator.cli.usage_stats",
        "script": "cli/usage_stats.py",
        "description": "Generate command usage stats from _docs/_stats user files",
    },
}

# manage-pipelines subcommands
SERVICE_COMMANDS: dict[str, dict[str, str]] = {
    "config": {
        "runner": "generator",
        "module": "cdc_generator.cli.service",
        "script": "cli/service.py",
        "description": "Manage CDC service definitions",
        "usage": "cdc manage-services config [service] [options]",
    },
    "schema": {
        "runner": "generator",
        "module": "cdc_generator.cli.service_schema",
        "script": "cli/service_schema.py",
        "description": (
            "Manage schema resources "
            + "(custom-tables, column-templates, transforms)"
        ),
        "usage": (
            "cdc manage-services schema <custom-tables|column-templates|transforms>"
        ),
    },
}


# manage-pipelines subcommands
PIPELINE_COMMANDS: dict[str, dict[str, str]] = {
    "generate": {
        "runner": "generator",
        "module": "cdc_generator.core.pipeline_generator",
        "script": "core/pipeline_generator.py",
        "description": "Generate Redpanda Connect pipelines",
        "usage": "cdc manage-pipelines generate [customer] [--all] [--force]",
    },
}

# manage-migrations subcommands
MIGRATION_COMMANDS: dict[str, dict[str, str]] = {
    "schema-docs": {
        "runner": "generator",
        "module": "cdc_generator.cli.schema_docs",
        "script": "cli/schema_docs.py",
        "description": "Generate database schema documentation YAML files",
        "usage": "cdc manage-migrations schema-docs [--env <env>]",
    },
}

# Top-level aliases that must defer to canonical command objects.
# This keeps behavior, options, and SmartCommand hierarchies in one place.
COMMAND_ALIASES: dict[str, str] = {
    "ms": "manage-services",
    "msog": "manage-source-groups",
    "msig": "manage-sink-groups",
    "mp": "manage-pipelines",
    "mm": "manage-migrations",
}


def print_help(
    workspace_root: Path,
    implementation_name: str | None,
    is_dev_container: bool,
) -> None:
    """Print help message with all available commands."""
    print(__doc__)

    if is_dev_container:
        if implementation_name:
            print(f"📍 Environment: Dev container - /implementations/{implementation_name}")
        else:
            print("📍 Environment: Dev container - /workspace (generator)")
    else:
        print(f"📍 Environment: Host - {workspace_root}")
        if implementation_name:
            print(f"   Implementation: {implementation_name}")

    print("\n📦 Top-level generator commands:")
    for cmd, info in GENERATOR_COMMANDS.items():
        print(f"  {cmd:20} - {info['description']}")

    print("\n🧩 Service management:")
    for cmd, info in SERVICE_COMMANDS.items():
        desc = info["description"]
        usage = info.get("usage")
        if usage is not None:
            desc += f"\n  {' ' * 20}   Usage: {usage}"
        print(f"  {'manage-services ' + cmd:20} - {desc}")

    print("\n⚡ Aliases:")
    print("  ms                   - alias for manage-services")
    print("  msc                  - alias for manage-services config")
    print("  mss                  - alias for manage-services schema")
    print("  msog                 - alias for manage-source-groups")
    print("  msig                 - alias for manage-sink-groups")
    print("  mp                   - alias for manage-pipelines")
    print("  mm                   - alias for manage-migrations")

    print("\n🔄 Pipeline management:")
    for cmd, info in PIPELINE_COMMANDS.items():
        desc = info["description"]
        usage = info.get("usage")
        if usage is not None:
            desc += f"\n  {' ' * 20}   Usage: {usage}"
        print(f"  {'manage-pipelines ' + cmd:20} - {desc}")

    print("\n🗄️ Migration management:")
    for cmd, info in MIGRATION_COMMANDS.items():
        desc = info["description"]
        usage = info.get("usage")
        if usage is not None:
            desc += f"\n  {' ' * 20}   Usage: {usage}"
        print(f"  {'manage-migrations ' + cmd:20} - {desc}")

    print("\n🧪 Testing:")
    print("  test                 - Run tests (--cli for e2e, --all for everything)")
    print("  test-coverage        - Show test coverage report by cdc command (-v for details)")

    print("\n💡 Tip: Run commands from implementation directory or dev container")


def _run_subprocess(
    cmd: list[str],
    cwd: Path | None = None,
) -> int:
    """Run a subprocess and return exit code, handling errors."""
    result = subprocess.run(cmd, cwd=cwd, check=False)
    return result.returncode


def run_generator_command(
    command: str,
    paths: ScriptPaths,
    extra_args: list[str],
    workspace_root: Path,
) -> int:
    """Run a command from the generator library.

    Args:
        command: Command name
        paths: Dictionary of script paths
        extra_args: Additional command-line arguments
        workspace_root: Current workspace root

    Returns:
        Exit code
    """
    cmd_info = GENERATOR_COMMANDS[command]

    return run_generator_spec(command, cmd_info, paths, extra_args, workspace_root)


def run_generator_spec(
    command_name: str,
    cmd_info: dict[str, str],
    paths: ScriptPaths,
    extra_args: list[str],
    workspace_root: Path,
) -> int:
    """Run a generator-backed command from a command spec."""

    if paths["generator"] is None:
        cmd = ["python3", "-m", cmd_info["module"], *extra_args]
    else:
        script_path = Path(str(paths["generator"])) / cmd_info["script"]
        if not script_path.exists():
            print(f"❌ Error: Generator script not found: {script_path}")
            print("   Make sure cdc-pipeline-generator is properly set up.")
            return 1
        cmd = ["python3", str(script_path), *extra_args]

    try:
        return _run_subprocess(cmd, cwd=workspace_root)
    except Exception as e:
        print(f"❌ Error running {command_name}: {e}")
        return 1


def execute_grouped_command(
    group: str,
    subcommand: str,
    extra_args: list[str],
) -> int:
    """Execute a grouped command (service/pipeline/migration)."""
    workspace_root, _implementation_name, is_dev_container = detect_environment()
    paths = get_script_paths(workspace_root, is_dev_container)

    if group == "manage-services":
        cmd_info = SERVICE_COMMANDS.get(subcommand)
    elif group == "manage-pipelines":
        cmd_info = PIPELINE_COMMANDS.get(subcommand)
    elif group == "manage-migrations":
        cmd_info = MIGRATION_COMMANDS.get(subcommand)
    else:
        cmd_info = None

    if cmd_info is None:
        print(f"❌ Unknown subcommand: {group} {subcommand}")
        print("\nRun 'cdc help' to see available commands.")
        return 1

    command_name = f"{group} {subcommand}"
    runner = cmd_info.get("runner")
    if runner == "generator":
        return run_generator_spec(command_name, cmd_info, paths, extra_args, workspace_root)
    print(
        "❌ This command is not available in canonical generator-only mode: "
        + command_name,
    )
    return 1


def _handle_special_commands(command: str, extra_args: list[str]) -> int | None:
    """Handle commands that don't need environment detection.

    Returns:
        Exit code if handled, None if not a special command.
    """
    if command == "scaffold":
        from cdc_generator.cli.scaffold_command import main as scaffold_main

        sys.argv = [sys.argv[0], *extra_args]
        return scaffold_main()

    if command == "test":
        from cdc_generator.cli.test_runner import main as test_main

        sys.argv = [sys.argv[0], *extra_args]
        return test_main()

    if command == "test-coverage":
        from tests.cli.coverage_report import main as coverage_main

        sys.argv = [sys.argv[0], *extra_args]
        return coverage_main()

    return None


def execute_command(command: str, extra_args: list[str]) -> int:
    """Execute a cdc command using the existing command handlers."""
    # Commands that run without environment detection
    special_result = _handle_special_commands(command, extra_args)
    if special_result is not None:
        return special_result

    # For other commands, detect environment
    workspace_root, _implementation_name, is_dev_container = detect_environment()
    paths = get_script_paths(workspace_root, is_dev_container)

    if command in GENERATOR_COMMANDS:
        return run_generator_command(command, paths, extra_args, workspace_root)

    print(f"❌ Unknown command: {command}")
    print("\nRun 'cdc help' to see available commands.")
    return 1


@click.group(invoke_without_command=True)
@click.pass_context
def _click_cli(ctx: click.Context) -> int:
    """Top-level CDC command group with passthrough command registration."""
    if ctx.invoked_subcommand is not None:
        return 0

    workspace_root, implementation_name, is_dev_container = detect_environment()
    print_help(workspace_root, implementation_name, is_dev_container)
    return 0


def _register_passthrough_command(
    command_name: str,
    description: str,
) -> None:
    """Register a generic passthrough click command (no typed options)."""

    @click.command(
        name=command_name,
        help=description,
        context_settings={
            "allow_extra_args": True,
            "ignore_unknown_options": True,
        },
        add_help_option=False,
    )
    @click.pass_context
    def _cmd(ctx: click.Context) -> int:
        extra_args: list[str] = list(ctx.args)
        return execute_command(command_name, extra_args)

    _click_cli.add_command(_cmd)


def _register_commands() -> None:
    """Register all top-level commands in the click app.

    Commands with typed Click option declarations (from click_commands.py)
    are registered directly. Remaining commands use generic passthrough.
    """
    from cdc_generator.cli.click_commands import (
        CLICK_COMMANDS,
        manage_services_config_cmd,
        manage_services_schema_cmd,
    )

    # Register typed commands (have full Click option declarations)
    for _name, cmd_obj in CLICK_COMMANDS.items():
        _click_cli.add_command(cmd_obj)

    # Register aliases as additional names for the same typed command objects.
    # No wrapper commands: aliases defer to canonical implementations.
    for alias, canonical in COMMAND_ALIASES.items():
        cmd_obj = CLICK_COMMANDS.get(canonical)
        if cmd_obj is not None:
            _click_cli.add_command(cmd_obj, name=alias)

    # Register direct subcommand aliases as top-level shortcuts.
    _click_cli.add_command(manage_services_config_cmd, name="msc")
    _click_cli.add_command(manage_services_schema_cmd, name="mss")

    # Register remaining commands as generic passthrough
    typed_names = set(CLICK_COMMANDS.keys())
    for cmd, info in GENERATOR_COMMANDS.items():
        if cmd not in typed_names:
            _register_passthrough_command(cmd, info["description"])

    @click.command(name="help", help="Show help message")
    def _help_cmd() -> int:
        workspace_root, implementation_name, is_dev_container = detect_environment()
        print_help(workspace_root, implementation_name, is_dev_container)
        return 0

    _click_cli.add_command(_help_cmd)


_register_commands()


def main() -> int:
    """Main CLI entry point."""
    import os

    # Let Click handle shell completion protocol before anything else.
    # When _CDC_COMPLETE is set, Click outputs completion data and exits.
    if os.environ.get("_CDC_COMPLETE"):
        with contextlib.suppress(SystemExit):
            _click_cli.main(
                args=sys.argv[1:],
                prog_name="cdc",
                standalone_mode=True,
            )
        return 0

    if len(sys.argv) >= _MIN_ARGS:
        workspace_root, _implementation_name, _is_dev_container = detect_environment()
        with contextlib.suppress(Exception):
            track_usage(workspace_root, sys.argv[1:])

    if len(sys.argv) < _MIN_ARGS or sys.argv[1] in ["help", "--help", "-h"]:
        workspace_root, implementation_name, is_dev_container = detect_environment()
        print_help(workspace_root, implementation_name, is_dev_container)
        return 0

    try:
        result = _click_cli.main(
            args=sys.argv[1:],
            prog_name="cdc",
            standalone_mode=False,
        )
    except click.Abort:
        print("\n⚠️  Cancelled by user")
        return 130
    except click.ClickException as exc:
        exc.show()
        return exc.exit_code

    return 0 if result is None else int(result)


if __name__ == "__main__":
    sys.exit(main())
