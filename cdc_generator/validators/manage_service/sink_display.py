"""Formatting helpers for sink listing output."""

from typing import cast

from cdc_generator.helpers.helpers_logging import Colors


def format_mapped_table(
    tbl_key: str,
    tbl_cfg: dict[str, object],
) -> None:
    """Print a single mapped table (target_exists=true)."""
    target = tbl_cfg.get("target", "?")
    cols_raw = tbl_cfg.get("columns", {})
    cols = cast(dict[str, str], cols_raw) if isinstance(cols_raw, dict) else {}
    col_count = len(cols)

    line = (
        f"  {Colors.YELLOW}→{Colors.RESET} "
        f"{Colors.CYAN}{tbl_key}{Colors.RESET} "
        f"→ {Colors.OKGREEN}{target}{Colors.RESET} "
        f"{Colors.DIM}(mapped, {col_count} columns){Colors.RESET}"
    )
    print(line)
    for src_col, tgt_col in cols.items():
        print(f"    {Colors.DIM}{src_col} → {tgt_col}{Colors.RESET}")


def format_cloned_table(
    tbl_key: str,
    tbl_cfg: dict[str, object],
) -> None:
    """Print a single cloned table (target_exists=false/absent)."""
    target_schema = tbl_cfg.get("target_schema")
    inc_raw = tbl_cfg.get("include_columns", [])
    inc_cols = cast(list[str], inc_raw) if isinstance(inc_raw, list) else []

    extras: list[str] = []
    if target_schema:
        extras.append(f"schema: {target_schema}")
    if inc_cols:
        extras.append(f"{len(inc_cols)} columns")

    extra_str = f" ({', '.join(extras)})" if extras else ""
    line = (
        f"  {Colors.OKGREEN}≡{Colors.RESET} "
        f"{Colors.CYAN}{tbl_key}{Colors.RESET} "
        f"{Colors.DIM}(clone{extra_str}){Colors.RESET}"
    )
    print(line)
