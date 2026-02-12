"""Tests for Click-based shell completions.

Validates that:
1. The cdc.fish eval bootstrap file exists and is correct
2. All subcommands are registered as Click commands
3. Typed commands have proper option declarations
4. Shell completion callbacks are wired correctly
5. Click's completion protocol responds to _CDC_COMPLETE
"""

from __future__ import annotations

import os
from pathlib import Path

import click
import click.testing

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CDC_FISH = Path(__file__).resolve().parent.parent / (
    "cdc_generator/templates/init/cdc.fish"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_click_cli() -> click.Group:
    """Import and return the Click CLI group."""
    from cdc_generator.cli.commands import _click_cli

    return _click_cli


def _get_typed_commands() -> dict[str, click.Command]:
    """Import and return the typed Click commands registry."""
    from cdc_generator.cli.click_commands import CLICK_COMMANDS

    return CLICK_COMMANDS


def _get_command_option_names(cmd: click.Command) -> set[str]:
    """Extract all long option names from a Click command."""
    names: set[str] = set()
    for param in cmd.params:
        if isinstance(param, click.Option):
            for opt in param.opts:
                if opt.startswith("--"):
                    names.add(opt)
    return names


# ---------------------------------------------------------------------------
# Tests: cdc.fish bootstrap file
# ---------------------------------------------------------------------------


class TestCdcFishBootstrap:
    """The cdc.fish file must be a minimal eval bootstrap."""

    def test_file_exists(self) -> None:
        """The cdc.fish template must exist."""
        assert CDC_FISH.exists(), f"Missing: {CDC_FISH}"

    def test_file_is_small(self) -> None:
        """The cdc.fish file should be a small eval bootstrap, not 700+ lines."""
        content = CDC_FISH.read_text(encoding="utf-8")
        lines = content.strip().split("\n")
        _MAX_BOOTSTRAP_LINES = 20
        assert len(lines) <= _MAX_BOOTSTRAP_LINES, (
            f"cdc.fish should be a small eval bootstrap, "
            f"got {len(lines)} lines"
        )

    def test_contains_eval_bootstrap(self) -> None:
        """The cdc.fish file must contain the Click eval bootstrap."""
        content = CDC_FISH.read_text(encoding="utf-8")
        assert "_CDC_COMPLETE=fish_source" in content
        assert "eval" in content


# ---------------------------------------------------------------------------
# Tests: Click command registration
# ---------------------------------------------------------------------------


class TestClickCommandRegistration:
    """All commands must be registered in the Click group."""

    def test_generator_commands_registered(self) -> None:
        """All GENERATOR_COMMANDS must be registered as Click subcommands."""
        from cdc_generator.cli.commands import GENERATOR_COMMANDS

        cli = _get_click_cli()
        registered = set(cli.commands.keys()) if hasattr(cli, "commands") else set()

        for cmd_name in GENERATOR_COMMANDS:
            assert cmd_name in registered, (
                f"GENERATOR_COMMANDS[{cmd_name!r}] not registered in Click"
            )

    def test_local_commands_registered(self) -> None:
        """All LOCAL_COMMANDS must be registered as Click subcommands."""
        from cdc_generator.cli.commands import LOCAL_COMMANDS

        cli = _get_click_cli()
        registered = set(cli.commands.keys()) if hasattr(cli, "commands") else set()

        for cmd_name in LOCAL_COMMANDS:
            assert cmd_name in registered, (
                f"LOCAL_COMMANDS[{cmd_name!r}] not registered in Click"
            )

    def test_special_commands_registered(self) -> None:
        """test, test-coverage, and help must be registered."""
        cli = _get_click_cli()
        registered = set(cli.commands.keys()) if hasattr(cli, "commands") else set()

        for cmd_name in ["test", "test-coverage", "help"]:
            assert cmd_name in registered, (
                f"Special command {cmd_name!r} not registered"
            )


# ---------------------------------------------------------------------------
# Tests: Typed command option declarations
# ---------------------------------------------------------------------------


class TestManageServiceOptions:
    """manage-service must have typed Click option declarations."""

    def test_has_service_option(self) -> None:
        """--service must be declared with shell_complete."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-service"])
        assert "--service" in opts

    def test_has_sink_management_options(self) -> None:
        """Sink management options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-service"])
        for opt in [
            "--add-sink",
            "--remove-sink",
            "--sink",
            "--add-sink-table",
            "--remove-sink-table",
            "--sink-table",
        ]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_source_table_options(self) -> None:
        """Source table management options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-service"])
        for opt in [
            "--add-source-table",
            "--remove-table",
            "--source-table",
            "--list-source-tables",
        ]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_column_template_options(self) -> None:
        """Column template options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-service"])
        for opt in [
            "--add-column-template",
            "--remove-column-template",
            "--list-column-templates",
            "--add-transform",
            "--remove-transform",
            "--list-transforms",
        ]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_custom_table_options(self) -> None:
        """Custom table options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-service"])
        for opt in [
            "--add-custom-sink-table",
            "--modify-custom-table",
            "--remove-column",
        ]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_inspect_options(self) -> None:
        """Inspect/validation options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-service"])
        for opt in [
            "--inspect",
            "--inspect-sink",
            "--validate-config",
            "--validate-bloblang",
        ]:
            assert opt in opts, f"Missing option: {opt}"


class TestManageSourceGroupsOptions:
    """manage-source-groups must have typed Click option declarations."""

    def test_has_core_options(self) -> None:
        """Core options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-source-groups"])
        for opt in ["--update", "--info", "--all"]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_server_management_options(self) -> None:
        """Server management options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-source-groups"])
        for opt in ["--add-server", "--remove-server", "--list-servers"]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_extraction_pattern_options(self) -> None:
        """Extraction pattern options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-source-groups"])
        for opt in [
            "--add-extraction-pattern",
            "--list-extraction-patterns",
            "--remove-extraction-pattern",
        ]:
            assert opt in opts, f"Missing option: {opt}"


class TestManageSinkGroupsOptions:
    """manage-sink-groups must have typed Click option declarations."""

    def test_has_create_options(self) -> None:
        """Create options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-sink-groups"])
        for opt in ["--create", "--source-group", "--add-new-sink-group"]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_server_management_options(self) -> None:
        """Server management options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-sink-groups"])
        for opt in ["--sink-group", "--add-server", "--remove-server"]:
            assert opt in opts, f"Missing option: {opt}"


class TestManageColumnTemplatesOptions:
    """manage-column-templates must have typed Click option declarations."""

    def test_has_crud_options(self) -> None:
        """CRUD options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-column-templates"])
        for opt in ["--list", "--show", "--add", "--edit", "--remove"]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_field_options(self) -> None:
        """Template field options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-column-templates"])
        for opt in ["--name", "--type", "--value", "--not-null"]:
            assert opt in opts, f"Missing option: {opt}"


class TestScaffoldOptions:
    """scaffold must have typed Click option declarations."""

    def test_has_core_options(self) -> None:
        """Core options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["scaffold"])
        for opt in ["--pattern", "--source-type", "--update"]:
            assert opt in opts, f"Missing option: {opt}"

    def test_pattern_has_choices(self) -> None:
        """--pattern must have Choice type with correct values."""
        cmds = _get_typed_commands()
        cmd = cmds["scaffold"]
        for param in cmd.params:
            if isinstance(param, click.Option) and "--pattern" in param.opts:
                assert isinstance(param.type, click.Choice)
                assert "db-per-tenant" in param.type.choices
                assert "db-shared" in param.type.choices
                return
        raise AssertionError("--pattern option not found")


class TestSetupLocalOptions:
    """setup-local must have typed Click option declarations."""

    def test_has_service_options(self) -> None:
        """Service options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["setup-local"])
        for opt in ["--postgres", "--mssql", "--all", "--stop"]:
            assert opt in opts, f"Missing option: {opt}"


# ---------------------------------------------------------------------------
# Tests: Click completion protocol
# ---------------------------------------------------------------------------


class TestClickCompletionProtocol:
    """Click's _CDC_COMPLETE environment variable must work."""

    def test_fish_source_outputs_script(self) -> None:
        """_CDC_COMPLETE=fish_source must output a fish completion script."""
        runner = click.testing.CliRunner()
        cli = _get_click_cli()

        # Click reads os.environ directly, not CliRunner's env param
        os.environ["_CDC_COMPLETE"] = "fish_source"
        try:
            result = runner.invoke(
                cli, [], prog_name="cdc", catch_exceptions=False,
            )
        finally:
            os.environ.pop("_CDC_COMPLETE", None)
        # Click outputs the fish completion function and exits
        assert "complete" in result.output

    def test_fish_source_contains_function(self) -> None:
        """The generated script must define a completion function."""
        runner = click.testing.CliRunner()
        cli = _get_click_cli()

        os.environ["_CDC_COMPLETE"] = "fish_source"
        try:
            result = runner.invoke(
                cli, [], prog_name="cdc", catch_exceptions=False,
            )
        finally:
            os.environ.pop("_CDC_COMPLETE", None)
        # Click generates a function named _cdc_completion
        assert "function" in result.output


# ---------------------------------------------------------------------------
# Tests: Shell complete callbacks are wired
# ---------------------------------------------------------------------------


class TestShellCompleteCallbacksWired:
    """Typed commands must have shell_complete callbacks on dynamic options."""

    def _has_shell_complete(
        self, cmd: click.Command, option_name: str,
    ) -> bool:
        """Check if an option has a shell_complete callback."""
        for param in cmd.params:
            if isinstance(param, click.Option) and option_name in param.opts:
                # Click stores the callback as _custom_shell_complete
                return hasattr(param, "_custom_shell_complete") and (
                    param._custom_shell_complete is not None
                )
        return False

    def test_manage_service_dynamic_options(self) -> None:
        """manage-service dynamic options must have shell_complete."""
        cmds = _get_typed_commands()
        cmd = cmds["manage-service"]
        for opt in [
            "--service",
            "--add-source-table",
            "--remove-table",
            "--sink",
            "--add-sink",
            "--add-sink-table",
            "--remove-sink-table",
            "--add-column-template",
            "--remove-column-template",
            "--add-transform",
            "--remove-transform",
        ]:
            assert self._has_shell_complete(cmd, opt), (
                f"manage-service {opt} missing shell_complete callback"
            )

    def test_manage_source_groups_dynamic_options(self) -> None:
        """manage-source-groups dynamic options must have shell_complete."""
        cmds = _get_typed_commands()
        cmd = cmds["manage-source-groups"]
        for opt in ["--server", "--list-extraction-patterns"]:
            assert self._has_shell_complete(cmd, opt), (
                f"manage-source-groups {opt} missing shell_complete callback"
            )

    def test_manage_sink_groups_dynamic_options(self) -> None:
        """manage-sink-groups dynamic options must have shell_complete."""
        cmds = _get_typed_commands()
        cmd = cmds["manage-sink-groups"]
        for opt in [
            "--sink-group",
            "--source-group",
            "--info",
            "--remove",
            "--remove-server",
        ]:
            assert self._has_shell_complete(cmd, opt), (
                f"manage-sink-groups {opt} missing shell_complete callback"
            )

    def test_manage_column_templates_dynamic_options(self) -> None:
        """manage-column-templates dynamic options must have shell_complete."""
        cmds = _get_typed_commands()
        cmd = cmds["manage-column-templates"]
        for opt in ["--show", "--edit", "--remove", "--type"]:
            assert self._has_shell_complete(cmd, opt), (
                f"manage-column-templates {opt} missing shell_complete callback"
            )


# ═══════════════════════════════════════════════════════════════════════════
# Smart completion (context-aware option filtering)
# ═══════════════════════════════════════════════════════════════════════════


class TestSmartCompletion:
    """SmartCommand filters completions based on active context flags.

    When no context flag is set the user sees only entry-point flags
    (+ always-visible options). Once a context flag is present, only
    its sub-options (+ always-visible) are shown.
    """

    @staticmethod
    def _complete(partial_cmd: str) -> list[str]:
        """Return completion values for *partial_cmd* via Click API."""
        import shlex

        from click.shell_completion import ShellComplete

        cli = _get_click_cli()
        parts = shlex.split(partial_cmd)
        if parts and parts[0] == "cdc":
            parts = parts[1:]
        incomplete = parts.pop() if parts else ""
        comp = ShellComplete(cli, {}, "cdc", "_CDC_COMPLETE")
        return [c.value for c in comp.get_completions(parts, incomplete)]

    # -- manage-service: entry-point filtering --------------------------------

    def test_entry_points_include_service(self) -> None:
        assert "--service" in self._complete("cdc manage-service --")

    def test_entry_points_include_context_flags(self) -> None:
        opts = self._complete("cdc manage-service --")
        assert "--inspect" in opts
        assert "--add-source-table" in opts
        assert "--add-sink-table" in opts

    def test_entry_points_exclude_sub_options(self) -> None:
        opts = self._complete("cdc manage-service --")
        assert "--schema" not in opts  # sub-option of --inspect
        assert "--primary-key" not in opts  # sub-option of --add-source-table
        assert "--map-column" not in opts  # sub-option of --add-sink-table

    # -- manage-service: context-filtered options -----------------------------

    def test_inspect_context_shows_sub_options(self) -> None:
        opts = self._complete("cdc manage-service --inspect --")
        assert "--schema" in opts
        assert "--save" in opts
        assert "--env" in opts

    def test_inspect_context_hides_unrelated(self) -> None:
        opts = self._complete("cdc manage-service --inspect --")
        assert "--primary-key" not in opts
        assert "--map-column" not in opts

    def test_add_source_table_context(self) -> None:
        opts = self._complete(
            "cdc manage-service --add-source-table Actor --"
        )
        assert "--primary-key" in opts
        assert "--schema" in opts
        assert "--map-column" not in opts

    def test_add_sink_table_context(self) -> None:
        opts = self._complete(
            "cdc manage-service --add-sink-table pub.Actor --"
        )
        assert "--sink" in opts
        assert "--target" in opts
        assert "--map-column" in opts
        assert "--primary-key" not in opts

    def test_source_table_context(self) -> None:
        opts = self._complete(
            "cdc manage-service --source-table Actor --"
        )
        assert "--ignore-columns" in opts
        assert "--track-columns" in opts

    def test_modify_custom_table_context(self) -> None:
        opts = self._complete(
            "cdc manage-service --modify-custom-table tbl --"
        )
        assert "--add-column" in opts
        assert "--remove-column" in opts

    # -- multi-context union --------------------------------------------------

    def test_multiple_contexts_union(self) -> None:
        opts = self._complete(
            "cdc manage-service --inspect --add-source-table Actor --"
        )
        # Union of inspect + add_source_table sub-options
        assert "--schema" in opts
        assert "--save" in opts
        assert "--primary-key" in opts

    # -- always-visible options persist in context ----------------------------

    def test_always_visible_in_context(self) -> None:
        opts = self._complete("cdc manage-service --inspect --")
        assert "--service" in opts
        assert "--server" in opts

    # -- positional + smart completion ----------------------------------------

    def test_positional_service_with_context(self) -> None:
        opts = self._complete(
            "cdc manage-service directory --inspect --"
        )
        assert "--schema" in opts
        assert "--primary-key" not in opts

    # -- manage-source-groups -------------------------------------------------

    def test_source_groups_entry_points(self) -> None:
        opts = self._complete("cdc manage-source-groups --")
        assert "--update" in opts
        assert "--add-server" in opts
        assert "--introspect-types" in opts

    def test_source_groups_introspect_context(self) -> None:
        opts = self._complete(
            "cdc manage-source-groups --introspect-types --"
        )
        assert "--server" in opts
        # Should NOT show unrelated sub-options
        assert "--host" not in opts

    def test_source_groups_add_server_context(self) -> None:
        opts = self._complete(
            "cdc manage-source-groups --add-server srv1 --"
        )
        assert "--host" in opts
        assert "--port" in opts
        assert "--user" in opts

    # -- manage-sink-groups ---------------------------------------------------

    def test_sink_groups_entry_points(self) -> None:
        opts = self._complete("cdc manage-sink-groups --")
        assert "--create" in opts
        assert "--inspect" in opts
        assert "--add-server" in opts

    def test_sink_groups_create_context(self) -> None:
        opts = self._complete("cdc manage-sink-groups --create --")
        assert "--source-group" in opts
        assert "--type" in opts
        assert "--pattern" in opts

    def test_sink_groups_inspect_context(self) -> None:
        opts = self._complete("cdc manage-sink-groups --inspect --")
        assert "--server" in opts
        assert "--include-pattern" in opts
        # create sub-options hidden
        assert "--source-group" not in opts

    # -- --sink as qualifier context -----------------------------------------

    def test_sink_qualifier_narrows_to_actions(self) -> None:
        opts = self._complete(
            "cdc manage-service --sink sink_asma.proxy --"
        )
        assert "--add-sink-table" in opts
        assert "--remove-sink-table" in opts
        assert "--sink-table" in opts
        assert "--update-schema" in opts
        # Unrelated entry-points hidden
        assert "--inspect" not in opts
        assert "--add-source-table" not in opts
        assert "--validate-config" not in opts

    def test_sink_qualifier_plus_action(self) -> None:
        opts = self._complete(
            "cdc manage-service --sink asma --add-sink-table pub.A --"
        )
        # Union: sink actions + add_sink_table sub-options
        assert "--target" in opts
        assert "--map-column" in opts
        assert "--sink-table" in opts

    # -- non-smart commands unaffected ----------------------------------------

    def test_non_smart_command_unchanged(self) -> None:
        opts = self._complete("cdc generate --")
        assert "--all" in opts
        assert "--force" in opts
