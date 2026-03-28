"""Migration generator package exports.

Policy: this module is import/export-only.
"""

from __future__ import annotations

from cdc_generator.core.column_template_operations import (
    resolve_column_templates,
    resolve_transforms,
)
from cdc_generator.helpers.service_config import (
    get_project_root,
    load_service_config,
)
from cdc_generator.helpers.service_schema_paths import get_service_schema_read_dirs
from cdc_generator.helpers.yaml_loader import load_yaml_file

from .columns import (
    CDC_METADATA_COLUMNS,
)
from .columns import (
    add_cdc_metadata_columns as _add_cdc_metadata_columns,
)
from .data_structures import (
    ExistingColumnDef,
    GenerationResult,
    ManualMigrationHints,
    MigrationColumn,
    RuntimeMode,
    SinkTarget,
    TableMigration,
)
from .file_writers import (
    compute_checksum,
    inject_checksum,
)
from .rendering import (
    build_column_defs_sql,
    build_create_table_sql,
)
from .runtime import (
    build_columns_from_table_def,
    build_full_column_list,
    generate_migrations,
    load_table_definitions,
)
from .service_parsing import (
    derive_target_schemas as _derive_target_schemas,
)
from .service_parsing import (
    get_sinks,
    resolve_sink_target,
)

__all__ = [
    "CDC_METADATA_COLUMNS",
    "ExistingColumnDef",
    "GenerationResult",
    "ManualMigrationHints",
    "MigrationColumn",
    "RuntimeMode",
    "SinkTarget",
    "TableMigration",
    "_add_cdc_metadata_columns",
    "_derive_target_schemas",
    "build_column_defs_sql",
    "build_columns_from_table_def",
    "build_create_table_sql",
    "build_full_column_list",
    "compute_checksum",
    "generate_migrations",
    "get_project_root",
    "get_service_schema_read_dirs",
    "get_sinks",
    "inject_checksum",
    "load_service_config",
    "load_table_definitions",
    "load_yaml_file",
    "resolve_column_templates",
    "resolve_sink_target",
    "resolve_transforms",
]
