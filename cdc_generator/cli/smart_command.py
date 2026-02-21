"""SmartCommand — context-aware option filtering for shell completion.

``SmartCommand`` is a ``click.Command`` subclass that filters the list
of completions shown when the user presses tab.  Instead of dumping all
40+ options at once, it shows only the options relevant to what has
already been typed on the command line.

The filtering is driven by three data structures:

``smart_groups``
    Maps a "context flag" (option param name) to the set of sub-option
    param names that become visible when that flag is present.

``smart_always``
    A set of option param names that are always visible regardless of
    context (e.g. ``service``, ``server``).

``smart_requires``
    Maps an option param name to a set of prerequisite option param
    names that must be present (active) for the option to appear.
    Enforces hierarchical ordering — e.g. ``sink_table`` requires
    ``sink``, so ``--sink-table`` only appears after ``--sink`` is set.

Option-group definitions for each command live in this module as well
so that click_commands.py stays focused on Click decorators.
"""

from __future__ import annotations

import click
from click.shell_completion import CompletionItem

# ============================================================================
# SmartCommand class
# ============================================================================


class SmartCommand(click.Command):
    """A Click command that filters completion options based on context.

    When the user presses tab, only options relevant to what they've
    already typed are shown.  This is driven by three data structures
    passed as keyword arguments to ``@click.command(cls=SmartCommand)``:

    ``smart_groups``
        Maps a "context flag" (option name without ``--``) to a set of
        sub-option names (also without ``--``) that become visible when
        that flag is present.  Example::

            {"inspect": {"schema", "all", "save", "env"}}

    ``smart_always``
        A set of option names (without ``--``) that are always visible
        regardless of context.  Typically the top-level entry-point
        flags like ``service``, ``create_service``.

    ``smart_requires``
        Maps an option name to a set of prerequisite option names that
        must all be active (present on the command line) for the option
        to appear in completions.  Enforces hierarchical ordering::

            {"sink_table": {"sink"}}       # --sink-table needs --sink first
            {"add_column_template": {"sink_table"}}  # needs --sink-table

        Prerequisites are checked *after* group filtering, so an option
        must be both allowed by its group AND have its prerequisites met.

    If neither attribute is set the command behaves like a normal
    ``click.Command`` — all options are always shown.
    """

    smart_groups: dict[str, set[str]]
    smart_always: set[str]
    smart_requires: dict[str, set[str]]

    def __init__(self, *args: object, **kwargs: object) -> None:
        self.smart_groups = kwargs.pop("smart_groups", {})  # type: ignore[arg-type]
        self.smart_always = kwargs.pop("smart_always", set())  # type: ignore[arg-type]
        self.smart_requires = kwargs.pop("smart_requires", {})  # type: ignore[arg-type]
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]

    def shell_complete(
        self,
        ctx: click.Context,
        incomplete: str,
    ) -> list[CompletionItem]:
        """Override to filter options based on already-typed context flags."""
        # If no smart groups defined, fall back to default behavior
        if not self.smart_groups:
            return super().shell_complete(ctx, incomplete)

        # Get all default completions from Click
        all_completions = super().shell_complete(ctx, incomplete)

        # Determine which context flags are active (group keys only)
        active_contexts = self._get_active_contexts(ctx)

        # All active params including always-visible (for prerequisites)
        all_active = self._get_all_active_params(ctx)

        # If no context group flags are active, show entry-point options
        if not active_contexts:
            return self._filter_to_entry_points(all_completions, all_active)

        # Build the set of allowed option names based on active contexts
        allowed = self._build_allowed_set(active_contexts, all_active)

        return [c for c in all_completions if self._is_allowed(c.value, allowed)]

    def _get_active_contexts(self, ctx: click.Context) -> set[str]:
        """Find which context *group* flags are set in the current params.

        Only checks keys of ``smart_groups`` — these drive which
        sub-options are expanded.  Always-visible options are checked
        separately by ``_get_all_active_params`` for prerequisite
        evaluation.
        """
        active: set[str] = set()
        for flag_name in self.smart_groups:
            val = ctx.params.get(flag_name)
            # Flag is active if it's True (is_flag) or has a non-None value
            if val is True or (val is not None and val is not False and val != ()):
                active.add(flag_name)
                continue

            # For passthrough commands, a bare option token may be present in
            # ctx.args before Click binds a value. Treat it as active context.
            option_token = f"--{flag_name.replace('_', '-')}"
            if option_token in ctx.args:
                active.add(flag_name)
        return active

    def _get_all_active_params(self, ctx: click.Context) -> set[str]:
        """Find ALL option params that are set (groups + always-visible).

        Used for prerequisite checking — prerequisites can reference
        any active param including always-visible ones like ``service``.

        Aliases: ``service_positional`` is treated as ``service`` so
        that prerequisites like ``"sink": {"service"}`` work whether
        the user typed ``--service X`` or just ``X`` as a positional.
        """
        active: set[str] = set()
        check_names = set(self.smart_groups.keys()) | self.smart_always
        for flag_name in check_names:
            val = ctx.params.get(flag_name)
            if val is True or (val is not None and val is not False and val != ()):
                active.add(flag_name)
                continue

            # Passthrough bare option token fallback (same rationale as above).
            option_token = f"--{flag_name.replace('_', '-')}"
            if option_token in ctx.args:
                active.add(flag_name)
        # Alias: positional service counts as "service"
        if "service_positional" in active:
            active.add("service")
        # Alias: sink-table actions can be reached via explicit --sink OR --all
        # fanout mode for add-sink-table.
        if "sink" in active or "all_flag" in active or "all" in active:
            active.add("sink_or_all")
        return active

    def _build_allowed_set(
        self,
        active_contexts: set[str],
        all_active: set[str],
    ) -> set[str]:
        """Union all sub-options for active context flags + always-visible.

        After building the union, removes any option whose prerequisites
        (from ``smart_requires``) are not all satisfied by the full set
        of active params (including always-visible options).
        """
        allowed = set(self.smart_always)
        # The context flags themselves stay visible
        allowed.update(active_contexts)
        for ctx_flag in active_contexts:
            allowed.update(self.smart_groups.get(ctx_flag, set()))

        # Enforce prerequisites: remove options whose required params
        # are not all active
        if self.smart_requires:
            to_remove: set[str] = set()
            for opt_name in allowed:
                required = self.smart_requires.get(opt_name)
                if required and not required.issubset(all_active):
                    to_remove.add(opt_name)
            allowed -= to_remove

        return allowed

    def _filter_to_entry_points(
        self,
        completions: list[CompletionItem],
        all_active: set[str] | None = None,
    ) -> list[CompletionItem]:
        """When no context group is active, show always-visible + entry-point flags.

        If ``smart_requires`` is configured, entry-point flags whose
        prerequisites are not met are also hidden.
        """
        # Entry points = all keys of smart_groups + always-visible
        entry_points = set(self.smart_groups.keys()) | self.smart_always

        # Remove entry-points whose prerequisites are not met
        if self.smart_requires:
            active = all_active or set()
            entry_points = {
                ep for ep in entry_points
                if not self.smart_requires.get(ep)
                or self.smart_requires[ep].issubset(active)
            }

        return [c for c in completions if self._is_allowed(c.value, entry_points)]

    def _is_allowed(self, opt_str: str, allowed_names: set[str]) -> bool:
        """Check if a completion value (e.g. '--add-sink-table') is in allowed set."""
        if not opt_str.startswith("-"):
            return True  # positional args always pass
        # Normalize: --add-sink-table → add_sink_table
        normalized = opt_str.lstrip("-").replace("-", "_")
        return normalized in allowed_names


# ============================================================================
# Option group definitions for smart completion
# ============================================================================


# manage-service: context flag → sub-options that become relevant
# Keys and values use underscore form (Click param names).
MANAGE_SERVICE_GROUPS: dict[str, set[str]] = {
    # ── Source inspection ──────────────────────────────────────
    "inspect": {"schema", "all", "save", "env", "server"},
    # ── Sink inspection ────────────────────────────────────────
    "inspect_sink": {"schema", "all", "save", "env"},
    # ── Add single source table ────────────────────────────────
    "add_source_table": {"primary_key", "schema"},
    # ── Add multiple source tables ─────────────────────────────
    "add_source_tables": {"primary_key"},
    # ── Manage existing source table ───────────────────────────
    "source_table": {"track_columns", "ignore_columns"},
    # ── Remove source table (standalone) ───────────────────────
    "remove_table": set(),
    # ── List source tables (standalone) ────────────────────────
    "list_source_tables": set(),
    # ── Validation (standalone) ────────────────────────────────
    "validate_config": set(),
    "validate_hierarchy": set(),
    "validate_bloblang": set(),
    "generate_validation": set(),
    # ── Sink lifecycle (standalone) ────────────────────────────
    "add_sink": set(),
    "remove_sink": set(),
    "list_sinks": set(),
    "validate_sinks": set(),
    # ── Sink qualifier (--sink narrows to sink actions) ────────
    "sink": {
        "add_sink_table", "remove_sink_table", "sink_table",
        "update_schema", "add_custom_sink_table", "modify_custom_table",
        "from", "from_table",
    },
    # ── Fanout qualifier (--all enables all-sinks add-table flow) ─────────
    "all_flag": {
        "add_sink_table",
        "from",
        "from_table",
        "replicate_structure",
        "sink_schema",
        "target_exists",
    },
    # ── Add sink table (requires --sink) ───────────────────────
    "add_sink_table": {
        "sink", "from", "from_table", "target", "target_exists",
        "target_schema", "sink_schema", "replicate_structure",
        "map_column", "include_sink_columns", "all",
    },
    # ── Remove sink table ──────────────────────────────────────
    "remove_sink_table": {"sink"},
    # ── Update sink table schema ───────────────────────────────
    "update_schema": {"sink", "sink_table"},
    # ── Sink table operations (--sink-table as context) ────────
    "sink_table": {
        "sink", "map_column", "add_column_template",
        "remove_column_template", "list_column_templates",
        "add_transform", "remove_transform", "list_transforms",
        "column_name", "value", "skip_validation",
    },
    # ── Column templates ───────────────────────────────────────
    "add_column_template": {
        "sink", "sink_table", "column_name", "value", "skip_validation",
    },
    "remove_column_template": {"sink", "sink_table"},
    "list_column_templates": {"sink", "sink_table"},
    # ── Transforms ─────────────────────────────────────────────
    "add_transform": {"sink", "sink_table", "skip_validation"},
    "remove_transform": {"sink", "sink_table"},
    "list_transforms": {"sink", "sink_table"},
    # ── Custom sink tables ─────────────────────────────────────
    "add_custom_sink_table": {"sink", "column", "from", "from_table"},
    # ── Modify custom table ────────────────────────────────────
    "modify_custom_table": {"sink", "add_column", "remove_column"},
    # ── Create service (standalone action) ─────────────────────
    "create_service": set(),
    # ── Remove service (standalone action) ─────────────────────
    "remove_service": set(),
}

# Options always shown for manage-service
MANAGE_SERVICE_ALWAYS: set[str] = {
    "service", "service_positional", "server", "all",
}

# Hierarchical prerequisites for manage-service.
# An option only appears if ALL its prerequisites are active.
#
# Hierarchy:
#   service → sink → sink_table → column template / transform ops
#   service → inspect, add_source_table, etc.
#   sink → add_sink_table, remove_sink_table, sink_table, ...
#   sink_table → add_column_template, remove_column_template, ...
#
MANAGE_SERVICE_REQUIRES: dict[str, set[str]] = {
    # ── Sink qualifier requires service ────────────────────────
    # Either --service or positional service_positional satisfies
    # this — see _check_prerequisites() which treats them as aliases.
    "sink": {"service"},
    # ── Sink actions require --sink ────────────────────────────
    "add_sink_table": {"sink_or_all"},
    "remove_sink_table": {"sink"},
    "sink_table": {"sink"},
    "update_schema": {"sink"},
    "add_custom_sink_table": {"sink"},
    "modify_custom_table": {"sink"},
    # ── Sink-table actions require --sink-table ────────────────
    "add_column_template": {"sink_table"},
    "remove_column_template": {"sink_table"},
    "list_column_templates": {"sink_table"},
    "add_transform": {"sink_table"},
    "remove_transform": {"sink_table"},
    "list_transforms": {"sink_table"},
    # ── Column template details require --add-column-template ──
    "column_name": {"add_column_template"},
    "value": {"add_column_template"},
}


# manage-source-groups: context flag → sub-options
MANAGE_SOURCE_GROUPS_GROUPS: dict[str, set[str]] = {
    "update": {"all", "server"},
    "info": set(),
    "list_env_services": set(),
    "add_to_ignore_list": set(),
    "list_ignore_patterns": set(),
    "add_to_schema_excludes": set(),
    "list_schema_excludes": set(),
    "add_env_mapping": set(),
    "list_env_mappings": set(),
    "add_server": {
        "source_type", "host", "port", "user", "password",
    },
    "list_servers": set(),
    "remove_server": set(),
    "set_kafka_topology": set(),
    "add_extraction_pattern": {
        "env", "strip_suffixes", "description",
    },
    "set_extraction_pattern": set(),
    "list_extraction_patterns": set(),
    "remove_extraction_pattern": set(),
    "set_validation_env": set(),
    "list_envs": set(),
    "introspect_types": {"server"},
    "db_definitions": {"server"},
    "pattern": {
        "source_type", "host", "port", "user", "password",
        "extraction_pattern", "environment_aware",
    },
}

MANAGE_SOURCE_GROUPS_ALWAYS: set[str] = set()

MANAGE_SOURCE_GROUPS_REQUIRES: dict[str, set[str]] = {}


# manage-sink-groups: context flag → sub-options
MANAGE_SINK_GROUPS_GROUPS: dict[str, set[str]] = {
    "create": {
        "source_group", "type", "pattern", "environment_aware",
        "no_environment_aware", "for_source_group",
    },
    "add_new_sink_group": {
        "type", "pattern", "environment_aware",
        "no_environment_aware", "for_source_group",
        "host", "port", "user", "password",
    },
    "list_flag": set(),
    "info": set(),
    "inspect": {"server", "include_pattern"},
    "update": {"sink_group", "server", "include_pattern"},
    "introspect_types": {"sink_group"},
    "db_definitions": {"sink_group"},
    "validate": set(),
    "sink_group": {
        "add_server", "remove_server", "server",
        "list_server_extraction_patterns",
        "host", "port", "user", "password", "extraction_patterns",
        "env", "strip_patterns", "env_mapping", "description",
    },
    "add_server": {
        "sink_group", "host", "port", "user", "password",
        "extraction_patterns", "env", "strip_patterns", "env_mapping", "description",
    },
    "server": {
        "sink_group", "extraction_patterns", "env", "strip_patterns",
        "env_mapping", "description",
    },
    "list_server_extraction_patterns": {"sink_group", "server"},
    "remove_server": {"sink_group"},
    "remove": set(),
}

MANAGE_SINK_GROUPS_ALWAYS: set[str] = set()

MANAGE_SINK_GROUPS_REQUIRES: dict[str, set[str]] = {}


# manage-services schema custom-tables: context flag → sub-options
MANAGE_SCHEMA_CUSTOM_TABLES_GROUPS: dict[str, set[str]] = {
    "list_services": set(),
    "list_custom_tables": set(),
    "add_custom_table": {"column"},
    "show_custom_table": set(),
    "remove_custom_table": set(),
}

MANAGE_SCHEMA_CUSTOM_TABLES_ALWAYS: set[str] = {
    "service",
    "list_services",
}

MANAGE_SCHEMA_CUSTOM_TABLES_REQUIRES: dict[str, set[str]] = {
    "list_custom_tables": {"service"},
    "add_custom_table": {"service"},
    "show_custom_table": {"service"},
    "remove_custom_table": {"service"},
    "column": {"add_custom_table"},
}
