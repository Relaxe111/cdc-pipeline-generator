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


def _get_manage_services_config_command() -> click.Command:
    """Return the typed command for `manage-services config`."""
    cmds = _get_typed_commands()
    group = cmds["manage-services"]
    assert isinstance(group, click.Group)
    return group.commands["config"]


def _get_manage_services_schema_column_templates_command() -> click.Command:
    """Return the typed command for `manage-services resources column-templates`."""
    cmds = _get_typed_commands()
    root_group = cmds["manage-services"]
    assert isinstance(root_group, click.Group)
    resources_group = root_group.commands["resources"]
    assert isinstance(resources_group, click.Group)
    return resources_group.commands["column-templates"]


def _get_manage_services_resources_inspect_command() -> click.Command:
    """Return the typed command for `manage-services resources inspect`."""
    cmds = _get_typed_commands()
    root_group = cmds["manage-services"]
    assert isinstance(root_group, click.Group)
    resources_group = root_group.commands["resources"]
    assert isinstance(resources_group, click.Group)
    return resources_group.commands["inspect"]


def _get_manage_services_resources_source_overrides_command() -> click.Command:
    """Return the typed command for `manage-services resources source-overrides`."""
    cmds = _get_typed_commands()
    root_group = cmds["manage-services"]
    assert isinstance(root_group, click.Group)
    resources_group = root_group.commands["resources"]
    assert isinstance(resources_group, click.Group)
    return resources_group.commands["source-overrides"]


def _get_manage_services_resources_command() -> click.Command:
    """Return the typed command group for `manage-services resources`."""
    cmds = _get_typed_commands()
    root_group = cmds["manage-services"]
    assert isinstance(root_group, click.Group)
    return root_group.commands["resources"]


def _get_manage_migrations_group() -> click.Group:
    """Return the typed command group for `manage-migrations`."""
    cmds = _get_typed_commands()
    group = cmds["manage-migrations"]
    assert isinstance(group, click.Group)
    return group


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
        max_bootstrap_lines = 20
        assert len(lines) <= max_bootstrap_lines, (
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

    def test_no_local_script_commands_registered(self) -> None:
        """Legacy script-backed top-level commands are not registered."""
        cli = _get_click_cli()
        registered = set(cli.commands.keys()) if hasattr(cli, "commands") else set()

        for cmd_name in ["validate", "reset-local", "nuke-local", "reload-cdc-autocompletions"]:
            assert cmd_name not in registered, (
                f"Legacy local command {cmd_name!r} should not be registered"
            )

    def test_special_commands_registered(self) -> None:
        """test, test-coverage, and help must be registered."""
        cli = _get_click_cli()
        registered = set(cli.commands.keys()) if hasattr(cli, "commands") else set()

        for cmd_name in ["test", "test-coverage", "help"]:
            assert cmd_name in registered, (
                f"Special command {cmd_name!r} not registered"
            )

    def test_management_groups_registered(self) -> None:
        """Grouped management commands must be registered."""
        cli = _get_click_cli()
        registered = set(cli.commands.keys()) if hasattr(cli, "commands") else set()

        for cmd_name in [
            "manage-services", "manage-pipelines", "manage-migrations",
        ]:
            assert cmd_name in registered, (
                f"Management group {cmd_name!r} not registered"
            )

    def test_management_aliases_registered(self) -> None:
        """Short aliases for management commands must be registered."""
        cli = _get_click_cli()
        registered = set(cli.commands.keys()) if hasattr(cli, "commands") else set()

        for alias in ["ms", "msc", "msr", "mss", "msog", "msig", "mp", "mm"]:
            assert alias in registered, (
                f"Management alias {alias!r} not registered"
            )

    def test_management_aliases_share_canonical_objects(self) -> None:
        """Aliases should resolve to the same Click command objects."""
        cli = _get_click_cli()
        assert hasattr(cli, "commands")
        commands = cli.commands

        assert commands["ms"] is commands["manage-services"]
        assert commands["msog"] is commands["manage-source-groups"]
        assert commands["msig"] is commands["manage-sink-groups"]
        assert commands["mp"] is commands["manage-pipelines"]
        assert commands["mm"] is commands["manage-migrations"]

        # Direct subcommand aliases should be distinct command objects.
        assert commands["msc"] is not commands["manage-services"]
        assert commands["msr"] is not commands["manage-services"]
        assert commands["mss"] is not commands["manage-services"]


# ---------------------------------------------------------------------------
# Tests: Typed command option declarations
# ---------------------------------------------------------------------------


class TestManageServicesConfigOptions:
    """manage-services config must have typed Click option declarations."""

    def test_has_service_option(self) -> None:
        """--service must be declared with shell_complete."""
        opts = _get_command_option_names(_get_manage_services_config_command())
        assert "--service" in opts
        assert "--list-services" in opts

    def test_has_sink_management_options(self) -> None:
        """Sink management options must be declared."""
        opts = _get_command_option_names(_get_manage_services_config_command())
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
        opts = _get_command_option_names(_get_manage_services_config_command())
        for opt in [
            "--add-source-table",
            "--remove-table",
            "--source-table",
            "--list-source-tables",
        ]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_column_template_options(self) -> None:
        """Column template options must be declared."""
        opts = _get_command_option_names(_get_manage_services_config_command())
        for opt in [
            "--add-column-template",
            "--remove-column-template",
            "--list-column-templates",
        ]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_custom_table_options(self) -> None:
        """Custom table options must be declared."""
        opts = _get_command_option_names(_get_manage_services_config_command())
        for opt in [
            "--add-custom-sink-table",
            "--modify-custom-table",
            "--remove-column",
        ]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_validation_options(self) -> None:
        """Validation options must be declared on config command."""
        opts = _get_command_option_names(_get_manage_services_config_command())
        for opt in [
            "--sink-all",
            "--validate-config",
            "--validate-bloblang",
        ]:
            assert opt in opts, f"Missing option: {opt}"

        assert "--inspect" not in opts
        assert "--inspect-sink" not in opts
        assert "--sink-inspect" not in opts

    def test_resources_inspect_has_inspect_options(self) -> None:
        """Inspect options must be declared on resources inspect command."""
        opts = _get_command_option_names(
            _get_manage_services_resources_inspect_command(),
        )
        for opt in [
            "--inspect",
            "--inspect-sink",
            "--sink-inspect",
            "--sink-all",
            "--sink-save",
            "--track-table",
            "--schema",
            "--env",
        ]:
            assert opt in opts, f"Missing option: {opt}"

    def test_resources_root_has_track_table_options(self) -> None:
        """Root resources command supports tracked-table management options."""
        opts = _get_command_option_names(
            _get_manage_services_resources_command(),
        )
        for opt in [
            "--source",
            "--service",
            "--sink",
            "--track-table",
            "--list-source-overrides",
            "--set-source-override",
            "--set-source-ovveride",
            "--remove-source-override",
        ]:
            assert opt in opts, f"Missing option: {opt}"

    def test_resources_source_overrides_subcommands_registered(self) -> None:
        """source-overrides command must expose canonical set/remove/list actions."""
        cmd = _get_manage_services_resources_source_overrides_command()
        assert isinstance(cmd, click.Group)
        assert "set" in cmd.commands
        assert "remove" in cmd.commands
        assert "list" in cmd.commands


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

    def test_has_source_custom_key_options(self) -> None:
        """Source custom key options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-source-groups"])
        for opt in [
            "--add-source-custom-key",
            "--custom-key-value",
            "--custom-key-exec-type",
        ]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_table_exclude_options(self) -> None:
        """Table exclude options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-source-groups"])
        for opt in ["--add-to-table-excludes", "--list-table-excludes"]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_table_include_options(self) -> None:
        """Table include options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-source-groups"])
        for opt in ["--add-to-table-includes", "--list-table-includes"]:
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

    def test_has_exclude_pattern_options(self) -> None:
        """Exclude-pattern options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-sink-groups"])
        for opt in [
            "--add-to-ignore-list",
            "--add-to-schema-excludes",
            "--add-to-table-excludes",
            "--list-table-excludes",
        ]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_source_custom_key_options(self) -> None:
        """Sink custom key options must be declared."""
        cmds = _get_typed_commands()
        opts = _get_command_option_names(cmds["manage-sink-groups"])
        for opt in [
            "--add-source-custom-key",
            "--custom-key-value",
            "--custom-key-exec-type",
        ]:
            assert opt in opts, f"Missing option: {opt}"


class TestManageServicesSchemaColumnTemplatesOptions:
    """manage-services resources column-templates options must be declared."""

    def test_has_crud_options(self) -> None:
        """CRUD options must be declared."""
        opts = _get_command_option_names(
            _get_manage_services_schema_column_templates_command(),
        )
        for opt in ["--list", "--show", "--add", "--edit", "--remove"]:
            assert opt in opts, f"Missing option: {opt}"

    def test_has_field_options(self) -> None:
        """Template field options must be declared."""
        opts = _get_command_option_names(
            _get_manage_services_schema_column_templates_command(),
        )
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


class TestManagePipelinesOptions:
    """manage-pipelines subcommands must expose expected options."""

    def test_generate_has_core_options(self) -> None:
        cmds = _get_typed_commands()
        group = cmds["manage-pipelines"]
        assert isinstance(group, click.Group)
        generate_cmd = group.commands["generate"]
        opts = _get_command_option_names(generate_cmd)
        for opt in ["--all", "--force"]:
            assert opt in opts, f"Missing option: {opt}"

    def test_verify_sync_has_filter_options(self) -> None:
        cmds = _get_typed_commands()
        group = cmds["manage-pipelines"]
        assert isinstance(group, click.Group)
        verify_sync_cmd = group.commands["verify-sync"]
        opts = _get_command_option_names(verify_sync_cmd)
        for opt in ["--customer", "--service", "--table", "--all"]:
            assert opt in opts, f"Missing option: {opt}"


class TestManageMigrationsOptions:
    """manage-migrations subcommands must expose expected options."""

    def test_has_generate_subcommand(self) -> None:
        cmds = _get_typed_commands()
        group = cmds["manage-migrations"]
        assert isinstance(group, click.Group)
        assert "generate" in group.commands

    def test_has_schema_docs_subcommand(self) -> None:
        cmds = _get_typed_commands()
        group = cmds["manage-migrations"]
        assert isinstance(group, click.Group)
        assert "schema-docs" in group.commands


class TestManageServicesOptions:
    """manage-services subcommands must be available."""

    def test_has_config_and_schema_subcommands(self) -> None:
        cmds = _get_typed_commands()
        group = cmds["manage-services"]
        assert isinstance(group, click.Group)
        assert "config" in group.commands
        assert "resources" in group.commands

    def test_schema_has_canonical_nested_subcommands(self) -> None:
        cmds = _get_typed_commands()
        root_group = cmds["manage-services"]
        assert isinstance(root_group, click.Group)
        resources_group = root_group.commands["resources"]
        assert isinstance(resources_group, click.Group)
        assert "custom-tables" in resources_group.commands
        assert "column-templates" in resources_group.commands
        assert "transforms" in resources_group.commands


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

    def _is_required_option(
        self, cmd: click.Command, option_name: str,
    ) -> bool:
        """Check whether an option is marked as required."""
        for param in cmd.params:
            if isinstance(param, click.Option) and option_name in param.opts:
                return bool(param.required)
        return False

    def test_manage_service_dynamic_options(self) -> None:
        """manage-services config dynamic options must have shell_complete."""
        cmd = _get_manage_services_config_command()
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
        ]:
            assert self._has_shell_complete(cmd, opt), (
                f"manage-services config {opt} missing shell_complete callback"
            )

        inspect_cmd = _get_manage_services_resources_inspect_command()
        for opt in ["--service", "--inspect-sink", "--schema"]:
            assert self._has_shell_complete(inspect_cmd, opt), (
                "manage-services resources inspect "
                + f"{opt} missing shell_complete callback"
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

    def test_manage_services_schema_column_templates_dynamic_options(self) -> None:
        """manage-services resources column-templates options have shell_complete."""
        cmd = _get_manage_services_schema_column_templates_command()
        for opt in ["--show", "--edit", "--remove", "--type"]:
            assert self._has_shell_complete(cmd, opt), (
                "manage-services resources column-templates "
                + f"{opt} missing shell_complete callback"
            )

    def test_manage_migrations_env_options_have_shell_complete(self) -> None:
        """manage-migrations env options must provide environment completions."""
        group = _get_manage_migrations_group()
        for subcommand in [
            "schema-docs",
            "apply",
            "status",
            "enable-cdc",
            "clean-cdc",
        ]:
            cmd = group.commands[subcommand]
            assert self._has_shell_complete(cmd, "--env"), (
                f"manage-migrations {subcommand} --env missing shell_complete callback"
            )

    def test_manage_migrations_env_required_on_env_specific_actions(self) -> None:
        """Environment-specific migration actions must require --env."""
        group = _get_manage_migrations_group()
        required_env_subcommands = ["apply", "enable-cdc", "clean-cdc"]
        for subcommand in required_env_subcommands:
            cmd = group.commands[subcommand]
            assert self._is_required_option(cmd, "--env"), (
                f"manage-migrations {subcommand} should require --env"
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
        incomplete = "" if partial_cmd.endswith(" ") else parts.pop() if parts else ""
        comp = ShellComplete(cli, {}, "cdc", "_CDC_COMPLETE")
        return [c.value for c in comp.get_completions(parts, incomplete)]

    # -- manage-services config: entry-point filtering -------------------------

    def test_entry_points_include_service(self) -> None:
        assert "--service" in self._complete("cdc manage-services config --")

    def test_entry_points_include_context_flags(self) -> None:
        opts = self._complete("cdc manage-services config --")
        assert "--add-source-table" in opts
        # --add-sink-table requires --sink which requires --service
        assert "--add-sink-table" not in opts

    def test_entry_points_with_service_unlock_sink(self) -> None:
        """With --service set, sink-related entry-points become visible."""
        opts = self._complete("cdc manage-services config --service directory --")
        assert "--sink" in opts
        # But deeper options still hidden (need --sink first)
        assert "--add-sink-table" not in opts
        assert "--sink-table" not in opts

    def test_positional_service_unlocks_sink(self) -> None:
        """Positional service name also satisfies the service prerequisite."""
        opts = self._complete("cdc manage-services config directory --")
        assert "--sink" in opts

    # -- hierarchical prerequisites -------------------------------------------

    def test_no_service_hides_sink_entry_point(self) -> None:
        """Without --service, --sink is hidden (prerequisite not met)."""
        opts = self._complete("cdc manage-services config --")
        assert "--sink" not in opts
        # But non-sink entry-points remain
        assert "--add-source-table" in opts

    def test_sink_without_service_still_expands(self) -> None:
        """If --sink is somehow present without --service, its sub-options
        still show (prerequisites check sub-options, not the flag itself)."""
        opts = self._complete(
            "cdc manage-services config --sink sink_asma.proxy --"
        )
        assert "--add-sink-table" in opts
        assert "--sink-table" in opts

    def test_sink_table_requires_sink(self) -> None:
        """--sink-table only appears when --sink is active."""
        opts = self._complete(
            "cdc manage-services config directory --sink sink_asma.proxy --"
        )
        assert "--sink-table" in opts

    def test_column_template_requires_sink_table(self) -> None:
        """--add-column-template only appears when --sink-table is active."""
        opts = self._complete(
            "cdc manage-services config --sink asma --sink-table pub.Actor --"
        )
        assert "--add-column-template" in opts
        assert "--remove-column-template" in opts

    def test_column_template_appears_with_add_sink_table(self) -> None:
        """--add-column-template appears when --add-sink-table is active."""
        opts = self._complete(
            "cdc manage-services config --sink asma --add-sink-table pub.Actor --"
        )
        assert "--add-column-template" in opts

    def test_add_transform_appears_with_add_sink_table(self) -> None:
        """--add-transform appears when --add-sink-table is active."""
        opts = self._complete(
            "cdc manage-services config --sink asma --add-sink-table pub.Actor --"
        )
        assert "--add-transform" in opts

    def test_column_name_appears_with_add_sink_table_and_add_column_template(self) -> None:
        """--column-name appears when --add-sink-table and --add-column-template are active."""
        opts = self._complete(
            "cdc manage-services config --sink asma --add-sink-table pub.Actor --add-column-template tpl --"
        )
        assert "--column-name" in opts
        assert "--value" in opts

    def test_column_template_hidden_without_sink_table(self) -> None:
        """--add-column-template hidden when only --sink is set."""
        opts = self._complete(
            "cdc manage-services config --sink sink_asma.proxy --"
        )
        assert "--add-column-template" not in opts

    def test_column_name_requires_add_column_template(self) -> None:
        """--column-name only appears after --add-column-template."""
        opts = self._complete(
            "cdc manage-services config --sink a --sink-table t --add-column-template tpl --"
        )
        assert "--column-name" in opts
        assert "--value" in opts

    def test_column_name_hidden_without_add_column_template(self) -> None:
        """--column-name hidden when only --sink-table is set."""
        opts = self._complete(
            "cdc manage-services config --sink a --sink-table t --"
        )
        assert "--column-name" not in opts
        assert "--value" not in opts

    def test_schema_custom_tables_keeps_service_visible(self) -> None:
        """--service remains visible after selecting custom-tables action."""
        opts = self._complete(
            "cdc manage-services resources custom-tables "
            + "--add-custom-table public.audit_log --"
        )
        assert "--service" in opts

    def test_add_custom_sink_table_shows_from_option(self) -> None:
        """--from is visible once --add-custom-sink-table is active."""
        opts = self._complete(
            "cdc manage-services config "
            + "--service directory "
            + "--sink sink_asma.proxy "
            + "--add-custom-sink-table public.audit_log --"
        )
        assert "--from" in opts

    def test_entry_points_exclude_sub_options(self) -> None:
        opts = self._complete("cdc manage-services config --")
        assert "--schema" not in opts  # sub-option of --inspect
        assert "--primary-key" not in opts  # sub-option of --add-source-table
        assert "--map-column" not in opts  # sub-option of --add-sink-table

    def test_entry_point_partial_all_is_suggested(self) -> None:
        """Regression: --al should suggest --all at config entry-point."""
        opts = self._complete("cdc manage-services config directory --al")
        assert "--all" in opts

    # -- manage-services config: context-filtered options ----------------------

    def test_inspect_context_shows_sub_options(self) -> None:
        opts = self._complete(
            "cdc manage-services resources inspect --inspect --"
        )
        assert "--schema" in opts
        assert "--all" in opts
        assert "--save" in opts
        assert "--track-table" in opts
        assert "--env" in opts

    def test_inspect_save_partial_dash_suggests_all(self) -> None:
        """Regression: after --inspect --save -, --all must still be offered."""
        opts = self._complete(
            "cdc manage-services resources inspect --inspect --save -"
        )
        assert "--all" in opts

    def test_inspect_context_hides_unrelated(self) -> None:
        opts = self._complete(
            "cdc manage-services resources inspect --inspect --"
        )
        assert "--primary-key" not in opts
        assert "--map-column" not in opts

    def test_inspect_sink_all_context_shows_save(self) -> None:
        """Regression: --inspect-sink --all should still suggest --save."""
        opts = self._complete(
            "cdc manage-services resources inspect "
            + "--service directory --inspect-sink --all --"
        )
        assert "--save" in opts

    def test_inspect_sink_value_completion_uses_service_sinks(self) -> None:
        """After --inspect-sink, suggest sink keys for selected service."""
        from unittest.mock import patch

        with patch(
            "cdc_generator.helpers.autocompletions.sinks.list_sink_keys_for_service",
            return_value=["sink_asma.proxy", "sink_asma.notification"],
        ):
            opts = self._complete(
                "cdc manage-services resources inspect "
                + "--service directory --inspect-sink "
            )

        assert "sink_asma.proxy" in opts
        assert "sink_asma.notification" in opts

    def test_inspect_sink_value_completion_autoselects_single_service(self) -> None:
        """When only one service exists, --inspect-sink suggests that service's sinks."""
        from unittest.mock import patch

        with patch(
            "cdc_generator.helpers.autocompletions.services.list_existing_services",
            return_value=["adopus"],
        ), patch(
            "cdc_generator.helpers.autocompletions.sinks.list_sink_keys_for_service",
            return_value=["sink_asma.directory", "sink_asma.chat"],
        ):
            opts = self._complete(
                "cdc manage-services resources inspect --inspect-sink "
            )

        assert "sink_asma.directory" in opts
        assert "sink_asma.chat" in opts

    def test_track_table_value_completion_uses_schema_tables(self) -> None:
        """--track-table suggests schema.table values from service schema resources."""
        from unittest.mock import patch

        with patch(
            "cdc_generator.helpers.autocompletions.tables.list_tables_for_service",
            return_value=["public.users", "public.rooms"],
        ):
            opts = self._complete(
                "cdc manage-services resources inspect "
                + "--service directory --inspect --save --track-table "
            )

        assert "public.users" in opts
        assert "public.rooms" in opts

    def test_resources_root_track_table_completion_uses_source_context(self) -> None:
        """At msr root, --source drives --track-table value completion."""
        from unittest.mock import patch

        with patch(
            "cdc_generator.helpers.autocompletions.tables.list_tables_for_service_autocomplete",
            return_value=["dbo.Actor", "dbo.Address"],
        ):
            opts = self._complete(
                "cdc manage-services resources "
                + "--source adopus --track-table "
            )

        assert "dbo.Actor" in opts
        assert "dbo.Address" in opts

    def test_add_source_table_context(self) -> None:
        opts = self._complete(
            "cdc manage-services config --add-source-table Actor --"
        )
        assert "--primary-key" in opts
        assert "--schema" in opts
        assert "--map-column" not in opts

    def test_add_sink_table_context(self) -> None:
        opts = self._complete(
            "cdc manage-services config --service dir --add-sink-table pub.Actor --"
        )
        assert "--sink" in opts
        assert "--all" in opts
        assert "--target" in opts
        assert "--map-column" in opts
        assert "--primary-key" not in opts

    def test_all_context_shows_add_sink_table_for_fanout(self) -> None:
        """Regression: --all fanout flow should still offer --add-sink-table."""
        opts = self._complete(
            "cdc manage-services config directory --all --"
        )
        assert "--add-sink-table" in opts

    def test_all_fanout_add_sink_table_shows_required_followups(self) -> None:
        """Regression: fanout add-table flow exposes --from/--replicate-structure/--sink-schema."""
        opts = self._complete(
            "cdc manage-services config directory "
            + "--all --add-sink-table --"
        )
        assert "--from" in opts
        assert "--replicate-structure" in opts
        assert "--sink-schema" in opts

    def test_add_sink_table_without_value_still_shows_from(self) -> None:
        """Regression: --from must remain visible after bare --add-sink-table."""
        opts = self._complete(
            "cdc manage-services config directory "
            + "--sink sink_asma.notification "
            + "--add-sink-table --fr"
        )
        assert "--from" in opts

    def test_add_sink_table_without_value_still_shows_from_no_context(self) -> None:
        """Regression: bare --add-sink-table still unlocks --from completion."""
        opts = self._complete(
            "cdc manage-services config --add-sink-table --fr"
        )
        assert "--from" in opts

    def test_source_table_context(self) -> None:
        opts = self._complete(
            "cdc manage-services config --source-table Actor --"
        )
        assert "--ignore-columns" in opts
        assert "--track-columns" in opts

    def test_modify_custom_table_context(self) -> None:
        opts = self._complete(
            "cdc manage-services config --modify-custom-table tbl --"
        )
        assert "--add-column" in opts
        assert "--remove-column" in opts

    # -- multi-context union --------------------------------------------------

    def test_multiple_contexts_union(self) -> None:
        opts = self._complete(
            "cdc manage-services config --add-source-table Actor --source-table public.users --"
        )
        # Union of add_source_table + source_table sub-options
        assert "--schema" in opts
        assert "--track-columns" in opts
        assert "--ignore-columns" in opts
        assert "--primary-key" in opts

    # -- always-visible options persist in context ----------------------------

    def test_always_visible_in_context(self) -> None:
        opts = self._complete("cdc manage-services config --inspect --")
        assert "--service" in opts
        assert "--server" in opts

    # -- positional + smart completion ----------------------------------------

    def test_positional_service_with_context(self) -> None:
        opts = self._complete(
            "cdc manage-services config directory --source-table public.users --"
        )
        assert "--track-columns" in opts
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

    def test_source_groups_update_context_shows_all(self) -> None:
        opts = self._complete("cdc manage-source-groups --update default -")
        assert "--all" in opts

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
            "cdc manage-services config --sink sink_asma.proxy --"
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
            "cdc manage-services config --sink asma --add-sink-table pub.A --"
        )
        # Union: sink actions + add_sink_table sub-options
        assert "--target" in opts
        assert "--map-column" in opts
        assert "--sink-table" in opts

    # -- non-smart commands unaffected ----------------------------------------

    def test_non_smart_command_unchanged(self) -> None:
        opts = self._complete("cdc manage-pipelines generate --")
        assert "--all" in opts
        assert "--force" in opts
