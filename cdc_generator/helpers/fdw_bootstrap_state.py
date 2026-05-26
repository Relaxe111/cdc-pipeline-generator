"""Helpers for querying, classifying, and rendering bootstrap state."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from cdc_generator.helpers.fdw_bootstrap import FdwBootstrapPlan


@dataclass(frozen=True)
class BootstrapStateRow:
    """One row from ``cdc_management.native_cdc_bootstrap_state``."""

    source_instance_key: str
    source_database: str
    logical_table_name: str
    bootstrap_status: str
    last_completed_at: str
    last_failed_at: str
    last_rows_loaded: str
    last_error: str


@dataclass(frozen=True)
class BootstrapResultRow:
    """One rendered result row for run/retry output."""

    source_instance_key: str
    source_database: str
    logical_table_name: str
    result: str
    rows: str
    note: str
    last_error: str


def build_source_database_map(plan: FdwBootstrapPlan) -> dict[str, str]:
    """Map ``source_instance_key`` values to user-facing source database names."""
    return {f"{source_plan.source_env}_{source_plan.customer_key}": source_plan.source_database for source_plan in plan.source_plans}


def build_state_query(
    status_filter: str | None,
    source_keys: list[str],
    tables: list[str] | None,
) -> str:
    """Build a query for bootstrap state rows."""
    conditions = ["bootstrap_status IS NOT NULL"]
    if status_filter:
        conditions = [f"bootstrap_status = {_quote_sql_literal(status_filter)}"]
    if source_keys:
        quoted_source_keys = ", ".join(_quote_sql_literal(source_key) for source_key in source_keys)
        conditions.append(f"source_instance_key IN ({quoted_source_keys})")
    if tables:
        quoted_tables = ", ".join(_quote_sql_literal(table_name) for table_name in tables)
        conditions.append(f"logical_table_name IN ({quoted_tables})")

    return (
        "SELECT "
        "source_instance_key, "
        "logical_table_name, "
        "COALESCE(bootstrap_status, ''), "
        "COALESCE(last_completed_at::text, ''), "
        "COALESCE(last_failed_at::text, ''), "
        "COALESCE(last_rows_loaded::text, ''), "
        "COALESCE(last_error, '') "
        "FROM cdc_management.native_cdc_bootstrap_state "
        f"WHERE {' AND '.join(conditions)} "
        "ORDER BY source_instance_key, logical_table_name"
    )


def parse_state_rows(
    output_text: str,
    source_database_map: dict[str, str],
) -> list[BootstrapStateRow]:
    """Parse tab-separated psql output into typed state rows."""
    rows: list[BootstrapStateRow] = []
    for columns in _parse_tab_separated_output(output_text, expected_columns=7):
        source_instance_key = columns[0]
        logical_table_name = columns[1]
        if not source_instance_key or not logical_table_name:
            continue
        rows.append(
            BootstrapStateRow(
                source_instance_key=source_instance_key,
                source_database=source_database_map.get(source_instance_key, source_instance_key),
                logical_table_name=logical_table_name,
                bootstrap_status=columns[2],
                last_completed_at=columns[3],
                last_failed_at=columns[4],
                last_rows_loaded=columns[5],
                last_error=columns[6],
            )
        )
    return rows


def classify_result_rows(
    before_rows: list[BootstrapStateRow],
    after_rows: list[BootstrapStateRow],
    *,
    enable_after: bool,
) -> list[BootstrapResultRow]:
    """Classify run/retry results by comparing pre/post state."""
    before_by_key = {(row.source_instance_key, row.logical_table_name): row for row in before_rows}
    after_by_key = {(row.source_instance_key, row.logical_table_name): row for row in after_rows}

    result_rows: list[BootstrapResultRow] = []
    for result_key in sorted(before_by_key):
        before_row = before_by_key[result_key]
        after_row = after_by_key.get(result_key, before_row)
        result_name = _classify_result_name(before_row, after_row)
        note = "disabled" if (not enable_after and result_name == "bootstrapped") else ""
        result_rows.append(
            BootstrapResultRow(
                source_instance_key=after_row.source_instance_key,
                source_database=after_row.source_database,
                logical_table_name=after_row.logical_table_name,
                result=result_name,
                rows=after_row.last_rows_loaded,
                note=note,
                last_error=after_row.last_error,
            )
        )

    return result_rows


def render_status_table(rows: list[BootstrapStateRow]) -> str:
    """Render status rows as a simple aligned table."""
    table_rows = [
        [
            row.source_database,
            row.logical_table_name,
            row.bootstrap_status or "-",
            _format_timestamp(row.last_completed_at or row.last_failed_at),
            _format_row_count(row.last_rows_loaded),
            row.last_error or "-",
        ]
        for row in rows
    ]
    return _render_table(
        ["SOURCE DB", "TABLE", "STATUS", "LAST BOOTSTRAP", "ROWS", "LAST ERROR"],
        table_rows,
    )


def render_result_table(rows: list[BootstrapResultRow]) -> str:
    """Render run/retry result rows as a simple aligned table."""
    include_note = any(row.note for row in rows)
    include_error = any(row.last_error for row in rows)

    headers = ["SOURCE DB", "TABLE", "RESULT", "ROWS"]
    if include_note:
        headers.append("NOTE")
    if include_error:
        headers.append("LAST ERROR")

    table_rows: list[list[str]] = []
    for row in rows:
        output_row = [
            row.source_database,
            row.logical_table_name,
            row.result,
            _format_row_count(row.rows),
        ]
        if include_note:
            output_row.append(row.note or "-")
        if include_error:
            output_row.append(row.last_error or "-")
        table_rows.append(output_row)

    return _render_table(headers, table_rows)


def render_result_summary(
    rows: list[BootstrapResultRow],
    *,
    enable_after: bool,
    target_sink_env: str,
) -> list[str]:
    """Render summary lines for run/retry output."""
    bootstrapped_count = sum(1 for row in rows if row.result == "bootstrapped")
    skipped_count = sum(1 for row in rows if row.result == "skipped")
    failed_count = sum(1 for row in rows if row.result == "failed")

    parts: list[str] = []
    if bootstrapped_count:
        parts.append(f"{bootstrapped_count} bootstrapped")
    if skipped_count:
        parts.append(f"{skipped_count} skipped")
    if failed_count:
        parts.append(f"{failed_count} failed")
    if not parts:
        parts.append("0 processed")

    summary_lines = [f"Summary: {', '.join(parts)}."]
    if not enable_after and bootstrapped_count:
        summary_lines.append("Re-enable with: " + f"cdc fdw apply --target-sink-env {target_sink_env}")
    return summary_lines


def status_rows_to_json(rows: list[BootstrapStateRow]) -> str:
    """Serialize status rows as formatted JSON."""
    return json.dumps([asdict(row) for row in rows], indent=2)


def result_rows_to_json(rows: list[BootstrapResultRow]) -> str:
    """Serialize result rows as formatted JSON."""
    return json.dumps([asdict(row) for row in rows], indent=2)


def count_failed_results(rows: list[BootstrapResultRow]) -> int:
    """Return the number of failed result rows."""
    return sum(1 for row in rows if row.result == "failed")


def _classify_result_name(
    before_row: BootstrapStateRow,
    after_row: BootstrapStateRow,
) -> str:
    """Classify one row based on pre/post bootstrap state."""
    if after_row.bootstrap_status == "failed":
        return "failed"
    if before_row.bootstrap_status == "completed" and after_row.bootstrap_status == "completed":
        return "skipped"
    if after_row.bootstrap_status == "completed":
        return "bootstrapped"
    if after_row.bootstrap_status:
        return after_row.bootstrap_status
    return "unknown"


def _parse_tab_separated_output(
    output_text: str,
    *,
    expected_columns: int,
) -> list[list[str]]:
    """Parse tab-separated psql output while preserving empty values."""
    rows: list[list[str]] = []
    for raw_line in output_text.splitlines():
        if not raw_line.strip():
            continue
        columns = raw_line.rstrip("\n").split("\t")
        if len(columns) < expected_columns:
            columns = columns + ([""] * (expected_columns - len(columns)))
        rows.append([column.strip() for column in columns[:expected_columns]])
    return rows


def _format_timestamp(value: str) -> str:
    """Trim noisy timestamp suffixes for terminal output."""
    if not value:
        return "-"

    trimmed_value = value.strip().replace("T", " ")
    if "+" in trimmed_value:
        trimmed_value = trimmed_value.split("+", 1)[0]
    if trimmed_value.endswith("Z"):
        trimmed_value = trimmed_value[:-1]
    if len(trimmed_value) >= 16:
        return trimmed_value[:16]
    return trimmed_value


def _format_row_count(value: str) -> str:
    """Format a numeric row count with separators when possible."""
    if not value:
        return "-"
    normalized = value.strip()
    if normalized.isdigit():
        return f"{int(normalized):,}"
    return normalized


def _render_table(headers: list[str], rows: list[list[str]]) -> str:
    """Render a simple aligned table."""
    if not rows:
        return ""

    widths = [len(header) for header in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    rendered_lines = [
        "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)),
        "  ".join("-" * widths[index] for index in range(len(headers))),
    ]
    for row in rows:
        rendered_lines.append("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))
    return "\n".join(rendered_lines)


def _quote_sql_literal(value: str) -> str:
    """Quote a SQL string literal for ad-hoc CLI queries."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
