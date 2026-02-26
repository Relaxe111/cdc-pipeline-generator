"""Shared argparse.Namespace default values for test files.

Single source of truth for the full set of dispatch-relevant attributes.
Import ``BASE_DEFAULTS`` and ``make_namespace`` from here instead of
duplicating the defaults dict in each test file.
"""

from __future__ import annotations

import argparse
from typing import Any

# Minimal default set shared across all handler test files.
BASE_DEFAULTS: dict[str, object] = {
    # Core
    "service": "proxy",
    "create_service": None,
    "server": None,
    # Source
    "add_source_table": None,
    "add_source_tables": None,
    "remove_table": None,
    "source_table": None,
    "list_source_tables": False,
    "primary_key": None,
    "schema": None,
    "ignore_columns": None,
    "track_columns": None,
    # Inspect
    "inspect": False,
    "inspect_sink": None,
    "all": False,
    "env": "nonprod",
    "save": False,
    # Validation
    "validate_config": False,
    "validate_hierarchy": False,
    "validate_bloblang": False,
    "generate_validation": False,
    # Sink
    "sink": None,
    "add_sink": None,
    "remove_sink": None,
    "add_sink_table": None,
    "remove_sink_table": None,
    "update_schema": None,
    "sink_table": None,
    "from_table": None,
    "replicate_structure": False,
    "sink_schema": None,
    "target_exists": None,
    "target": None,
    "target_schema": None,
    "map_column": None,
    "include_sink_columns": None,
    "list_sinks": False,
    "validate_sinks": False,
    "add_custom_sink_table": None,
    "column": None,
    "modify_custom_table": None,
    "add_column": None,
    "remove_column": None,
    # Templates
    "add_column_template": None,
    "remove_column_template": None,
    "list_column_templates": False,
    "column_name": None,
    "value": None,
    "add_transform": None,
    "remove_transform": None,
    "list_transforms": False,
    "list_template_keys": False,
    "list_transform_rule_keys": False,
    "skip_validation": True,
    # Legacy
    "source": None,
    "source_schema": None,
}


def make_namespace(**overrides: Any) -> argparse.Namespace:
    """Build a full ``argparse.Namespace`` with all dispatch-relevant attrs.

    Usage::

        args = make_namespace(service="proxy", list_sinks=True)
    """
    merged = {**BASE_DEFAULTS, **overrides}
    return argparse.Namespace(**merged)
