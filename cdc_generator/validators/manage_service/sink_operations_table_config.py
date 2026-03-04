"""Table configuration structures and builders for sink operations."""

from dataclasses import dataclass


@dataclass
class TableConfigOptions:
    """Options for building a sink table configuration."""

    target_exists: bool
    target: str | None = None
    target_schema: str | None = None
    include_columns: list[str] | None = None
    columns: dict[str, str] | None = None
    from_table: str | None = None
    replicate_structure: bool = False
    sink_schema: str | None = None
    column_template: str | None = None
    column_template_name: str | None = None
    column_template_value: str | None = None
    add_transform: str | None = None
    accepted_columns: list[str] | None = None


def _build_table_config(opts: TableConfigOptions) -> dict[str, object]:
    """Build the per-table config dict from the given options.

    target_exists is ALWAYS included in the output.
    """
    cfg: dict[str, object] = {"target_exists": opts.target_exists}

    if opts.from_table is not None:
        cfg["from"] = opts.from_table

    if opts.replicate_structure:
        cfg["replicate_structure"] = True

    if opts.target_exists:
        if opts.target:
            cfg["target"] = opts.target
        if opts.columns:
            cfg["columns"] = opts.columns
    else:
        if opts.target_schema:
            cfg["target_schema"] = opts.target_schema
        if opts.include_columns:
            cfg["include_columns"] = opts.include_columns

    if opts.column_template:
        template_entry: dict[str, str] = {"template": opts.column_template}
        if opts.column_template_name:
            template_entry["name"] = opts.column_template_name
        if opts.column_template_value:
            template_entry["value"] = opts.column_template_value
        cfg["column_templates"] = [template_entry]

    return cfg
