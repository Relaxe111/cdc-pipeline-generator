"""Runtime helper for `manage-services resources column-templates` Click command."""

from __future__ import annotations

import sys


def execute_manage_column_templates() -> int:
    """Run column-templates command via generator runtime spec."""
    from cdc_generator.cli.commands import (
        detect_environment,
        get_script_paths,
        run_generator_spec,
    )

    try:
        start_index = sys.argv.index("column-templates") + 1
    except ValueError:
        start_index = 2

    workspace_root, _implementation_name, is_dev_container = detect_environment()
    paths = get_script_paths(workspace_root, is_dev_container)
    cmd_info = {
        "runner": "generator",
        "module": "cdc_generator.cli.column_templates",
        "script": "cli/column_templates.py",
    }
    return run_generator_spec(
        "manage-services resources column-templates",
        cmd_info,
        paths,
        sys.argv[start_index:],
        workspace_root,
    )
