"""Sink listing/rendering helpers."""

from typing import cast

from cdc_generator.helpers.helpers_logging import Colors

from .sink_display import format_cloned_table, format_mapped_table
from .sink_operations_helpers import _parse_sink_key


def format_sink_entry(
    sink_key: str,
    sink_cfg: dict[str, object],
) -> None:
    """Print header + table rows for one sink entry."""
    parsed = _parse_sink_key(sink_key)
    if parsed:
        sg, ts = parsed
        header = (
            f"\n{Colors.BOLD}{Colors.CYAN}{sink_key}{Colors.RESET}"
            f"  {Colors.DIM}(group: {sg}, target: {ts}){Colors.RESET}"
        )
        print(header)
    else:
        print(f"\n{Colors.BOLD}{Colors.CYAN}{sink_key}{Colors.RESET}")

    tables_raw = sink_cfg.get("tables", {})
    tables = cast(dict[str, object], tables_raw) if isinstance(tables_raw, dict) else {}
    if not tables:
        print(f"  {Colors.DIM}No tables configured{Colors.RESET}")
        return

    for tbl_key_raw, tbl_raw in tables.items():
        tbl_key = str(tbl_key_raw)
        tbl_cfg = cast(dict[str, object], tbl_raw) if isinstance(tbl_raw, dict) else {}
        if tbl_cfg.get("target_exists", False):
            format_mapped_table(tbl_key, tbl_cfg)
        else:
            format_cloned_table(tbl_key, tbl_cfg)

    db_raw = sink_cfg.get("databases", {})
    if isinstance(db_raw, dict) and db_raw:
        databases = cast(dict[str, object], db_raw)
        print(f"  {Colors.DIM}Databases:{Colors.RESET}")
        for env, db in databases.items():
            print(f"    {env}: {db}")
