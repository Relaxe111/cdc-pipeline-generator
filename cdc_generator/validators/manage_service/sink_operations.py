"""Sink operations for CDC service configuration.

Handles adding, removing, and listing sink configurations in service YAML files.
Sinks define WHERE source tables are sent and HOW they are mapped.

Sink key format: {sink_group}.{target_service}
    - sink_group: references sink-groups.yaml (e.g., sink_asma)
    - target_service: target service/database in that sink group
"""

import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root, load_service_config
from cdc_generator.helpers.service_schema_paths import get_service_schema_read_dirs

from .config import SERVICE_SCHEMAS_DIR, save_service_config

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_SINK_KEY_SEPARATOR = "."
_SINK_KEY_PARTS = 2

# Valid unquoted PostgreSQL identifier: starts with letter or underscore,
# followed by letters, digits, underscores, or dollar signs. Max 63 chars.
_PG_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_$]*$")
_PG_IDENTIFIER_MAX_LENGTH = 63
_TYPE_WITH_ARGS_PATTERN = re.compile(r"^([^()]+)\s*\(.*\)$")

_AUTO_ENGINE = "auto"
_WILDCARD_SOURCE = "*"
_MAP_FILE_PATTERNS = (
    "map-{source}-{sink}.yaml",
    "map-{source}-to-{sink}.yaml",
)
_SOURCE_TYPE_OVERRIDES_PATTERN = "source-{source_group}-type-overrides.yaml"
_OVERRIDES_ALLOWED_TOP_LEVEL = {"metadata", "overrides"}

_PGSQL_FALLBACK_ALIASES: dict[str, str] = {
    "int": "integer",
    "int2": "smallint",
    "int4": "integer",
    "int8": "bigint",
    "decimal": "numeric",
    "varchar": "character varying",
    "char": "character",
    "timestamp": "timestamp without time zone",
    "timestamptz": "timestamp with time zone",
    "bool": "boolean",
    "float4": "real",
    "float8": "double precision",
}

_PGSQL_FALLBACK_BASE_TYPES: set[str] = {
    "smallint",
    "integer",
    "bigint",
    "numeric",
    "real",
    "double precision",
    "boolean",
    "uuid",
    "text",
    "character",
    "character varying",
    "date",
    "time without time zone",
    "timestamp without time zone",
    "timestamp with time zone",
    "bytea",
    "json",
    "jsonb",
    "xml",
    "user-defined",
}


@dataclass(frozen=True)
class TypeCompatibilityMap:
    """Runtime compatibility map for a source/sink engine pair."""

    source_engine: str
    sink_engine: str
    mappings: dict[str, str]
    source_aliases: dict[str, str]
    sink_aliases: dict[str, str]
    compatibility: dict[str, set[str]]


def _normalize_type_name(raw_type: str, aliases: dict[str, str] | None = None) -> str:
    """Normalize SQL type names for compatibility comparisons."""
    normalized = " ".join(raw_type.strip().lower().split())
    if not normalized:
        return normalized

    match = _TYPE_WITH_ARGS_PATTERN.match(normalized)
    if match:
        normalized = " ".join(match.group(1).split())

    if aliases is None:
        return normalized

    current = normalized
    seen: set[str] = set()
    while current in aliases and current not in seen:
        seen.add(current)
        current = aliases[current]
    return current


def _normalize_identifier_name(identifier: str) -> str:
    """Normalize identifier text for case-insensitive matching."""
    return identifier.strip().casefold()


def _normalize_source_table_key(table_key: str) -> str:
    """Normalize ``schema.table`` key used by source type overrides."""
    normalized = " ".join(table_key.strip().split())
    if "." not in normalized:
        raise ValueError(
            "Invalid source-type override table key: "
            + f"'{table_key}'. Expected 'schema.table'."
        )

    schema_name, table_name = normalized.split(".", 1)
    if not schema_name.strip() or not table_name.strip():
        raise ValueError(
            "Invalid source-type override table key: "
            + f"'{table_key}'. Expected non-empty schema and table."
        )

    return f"{schema_name.strip().casefold()}.{table_name.strip().casefold()}"


def _definitions_dir(project_root_key: str) -> str:
    """Return definitions folder path for compatibility maps."""
    from pathlib import Path

    return str(Path(project_root_key) / "services" / "_schemas" / "_definitions")


def _resolve_map_file_path(
    project_root_key: str,
    source_engine: str,
    sink_engine: str,
) -> str | None:
    """Resolve runtime map file path for an engine pair."""
    from pathlib import Path

    definitions_path = Path(_definitions_dir(project_root_key))
    for pattern in _MAP_FILE_PATTERNS:
        candidate = definitions_path / pattern.format(
            source=source_engine,
            sink=sink_engine,
        )
        if candidate.is_file():
            return str(candidate)
    return None


def _resolve_source_type_overrides_path(project_root_key: str) -> str | None:
    """Resolve source-group-scoped source type override file path."""
    from pathlib import Path

    from cdc_generator.helpers.yaml_loader import load_yaml_file

    source_groups_file = Path(project_root_key) / "source-groups.yaml"
    if not source_groups_file.is_file():
        return None

    try:
        source_groups = load_yaml_file(source_groups_file)
    except Exception as exc:
        raise ValueError(
            "Failed to read source groups for source type overrides: "
            + f"'{source_groups_file}': {exc}"
        ) from exc

    source_groups_dict = cast(dict[str, object], source_groups)
    source_group_names = [
        key
        for key in source_groups_dict
        if not key.startswith("_")
    ]
    if len(source_group_names) != 1:
        raise ValueError(
            "Source type overrides require exactly one source group in "
            + f"'{source_groups_file}'. Found {len(source_group_names)}."
        )

    source_group_name = source_group_names[0]
    candidate_name = _SOURCE_TYPE_OVERRIDES_PATTERN.format(
        source_group=source_group_name,
    )
    candidate = Path(_definitions_dir(project_root_key)) / candidate_name
    if candidate.is_file():
        return str(candidate)
    return None


def _extract_aliases(
    data_dict: dict[str, object],
) -> tuple[dict[str, str], dict[str, str]]:
    """Extract normalized source/sink aliases from map payload."""
    aliases_raw = data_dict.get("aliases")
    source_aliases_raw: object = {}
    sink_aliases_raw: object = {}
    if isinstance(aliases_raw, dict):
        aliases_dict = cast(dict[str, object], aliases_raw)
        source_aliases_raw = aliases_dict.get("source", {})
        sink_aliases_raw = aliases_dict.get("sink", {})

    source_aliases: dict[str, str] = {}
    if isinstance(source_aliases_raw, dict):
        for key, value in cast(dict[str, object], source_aliases_raw).items():
            if isinstance(value, str):
                source_aliases[_normalize_type_name(key)] = _normalize_type_name(value)

    sink_aliases: dict[str, str] = {}
    if isinstance(sink_aliases_raw, dict):
        for key, value in cast(dict[str, object], sink_aliases_raw).items():
            if isinstance(value, str):
                sink_aliases[_normalize_type_name(key)] = _normalize_type_name(value)

    return source_aliases, sink_aliases


def _extract_mappings(
    data_dict: dict[str, object],
    mapping_file: object,
    source_aliases: dict[str, str],
    sink_aliases: dict[str, str],
) -> dict[str, str]:
    """Extract normalized source->sink mappings from payload."""
    mappings_raw: object = data_dict.get("mappings")
    if not isinstance(mappings_raw, dict):
        raise ValueError(
            "Invalid type compatibility map format: "
            + f"'{mapping_file}' must define 'mappings:' "
            + "as a mapping of source_type -> sink_type."
        )

    mappings: dict[str, str] = {}
    for key, value in cast(dict[str, object], mappings_raw).items():
        if isinstance(value, str):
            normalized_key = _normalize_type_name(key, source_aliases)
            normalized_value = _normalize_type_name(value, sink_aliases)
            mappings[normalized_key] = normalized_value

    return mappings


def _extract_compatibility(
    data_dict: dict[str, object],
    source_aliases: dict[str, str],
    sink_aliases: dict[str, str],
) -> dict[str, set[str]]:
    """Extract normalized compatibility rules from payload."""
    compatibility_raw = data_dict.get("compatibility")
    if not isinstance(compatibility_raw, dict):
        return {}

    compatibility: dict[str, set[str]] = {}
    for src_type, targets_raw in cast(dict[str, object], compatibility_raw).items():
        normalized_src = (
            _WILDCARD_SOURCE
            if src_type == _WILDCARD_SOURCE
            else _normalize_type_name(src_type, source_aliases)
        )
        targets: set[str] = set()
        if isinstance(targets_raw, list):
            for item in cast(list[object], targets_raw):
                if isinstance(item, str):
                    targets.add(_normalize_type_name(item, sink_aliases))
        elif isinstance(targets_raw, str):
            targets.add(_normalize_type_name(targets_raw, sink_aliases))

        if targets:
            compatibility[normalized_src] = targets

    return compatibility


@lru_cache(maxsize=16)
def _load_source_type_overrides(project_root_key: str) -> dict[str, dict[str, str]]:
    """Load and validate source-only effective type overrides."""
    from pathlib import Path

    from cdc_generator.helpers.yaml_loader import load_yaml_file

    resolved_path = _resolve_source_type_overrides_path(project_root_key)
    if resolved_path is None:
        return {}

    override_file = Path(resolved_path)

    try:
        data = load_yaml_file(override_file)
    except Exception as exc:
        raise ValueError(
            "Failed to read source type overrides: "
            + f"'{override_file}': {exc}"
        ) from exc

    raw_data = cast(dict[str, object], data)
    unknown_top_level = sorted(
        key for key in raw_data if key not in _OVERRIDES_ALLOWED_TOP_LEVEL
    )
    if unknown_top_level:
        raise ValueError(
            "Invalid source type overrides format: "
            + f"'{override_file}' contains unsupported keys: "
            + ", ".join(unknown_top_level)
        )

    overrides_raw = raw_data.get("overrides", {})
    if not isinstance(overrides_raw, dict):
        raise ValueError(
            "Invalid source type overrides format: "
            + f"'{override_file}' must define 'overrides:' as "
            + "a mapping of source_table -> {source_column: effective_type}."
        )

    result: dict[str, dict[str, str]] = {}
    for source_table, columns_raw in cast(dict[str, object], overrides_raw).items():
        normalized_table = _normalize_source_table_key(source_table)

        if not isinstance(columns_raw, dict):
            raise ValueError(
                "Invalid source type overrides format: "
                + f"table '{source_table}' in '{override_file}' "
                + "must map to {source_column: effective_type}."
            )

        normalized_columns: dict[str, str] = {}
        for source_column, effective_type_raw in cast(dict[str, object], columns_raw).items():
            normalized_column = _normalize_identifier_name(source_column)
            if not normalized_column:
                raise ValueError(
                    "Invalid source type overrides format: "
                    + f"table '{source_table}' in '{override_file}' "
                    + "contains an empty source column key."
                )

            if not isinstance(effective_type_raw, str):
                raise ValueError(
                    "Invalid source type overrides format: "
                    + f"{source_table}.{source_column} in '{override_file}' "
                    + "must map to a string effective type."
                )

            effective_type = _normalize_type_name(effective_type_raw)
            if not effective_type:
                raise ValueError(
                    "Invalid source type overrides format: "
                    + f"{source_table}.{source_column} in '{override_file}' "
                    + "has an empty effective type."
                )

            if normalized_column in normalized_columns:
                raise ValueError(
                    "Duplicate source type override key after normalization: "
                    + f"{source_table}.{source_column} in '{override_file}'."
                )

            normalized_columns[normalized_column] = effective_type

        result[normalized_table] = normalized_columns

    return result


def _resolve_effective_source_type(
    project_root_key: str,
    source_type: str,
    source_table: str | None,
    source_column: str | None,
) -> str:
    """Resolve effective source type using optional source-only overrides."""
    if not source_table or not source_column:
        return source_type

    overrides = _load_source_type_overrides(project_root_key)
    if not overrides:
        return source_type

    normalized_table = _normalize_source_table_key(source_table)
    table_overrides = overrides.get(normalized_table)
    if table_overrides is None:
        return source_type

    normalized_column = _normalize_identifier_name(source_column)
    return table_overrides.get(normalized_column, source_type)


@lru_cache(maxsize=64)
def _load_type_compatibility_map(
    project_root_key: str,
    source_engine: str,
    sink_engine: str,
) -> TypeCompatibilityMap:
    """Load ``map-{source}-{sink}.yaml`` as compatibility source of truth."""
    from pathlib import Path

    from cdc_generator.helpers.yaml_loader import load_yaml_file

    resolved_map_path = _resolve_map_file_path(
        project_root_key,
        source_engine,
        sink_engine,
    )
    if resolved_map_path is None:
        raise ValueError(
            "Type compatibility map not found: "
            + f"map-{source_engine}-{sink_engine}.yaml "
            + "(or map-{source}-to-{sink}.yaml). "
            + "Expected file location: "
            + "services/_schemas/_definitions/."
        )
    mapping_file = Path(resolved_map_path)

    try:
        data = load_yaml_file(mapping_file)
    except Exception as exc:
        raise ValueError(
            "Failed to read type compatibility map: "
            + f"'{mapping_file}': {exc}"
        ) from exc

    data_dict = cast(dict[str, object], data)

    source_aliases, sink_aliases = _extract_aliases(data_dict)
    mappings = _extract_mappings(
        data_dict,
        mapping_file,
        source_aliases,
        sink_aliases,
    )
    compatibility = _extract_compatibility(
        data_dict,
        source_aliases,
        sink_aliases,
    )

    return TypeCompatibilityMap(
        source_engine=source_engine,
        sink_engine=sink_engine,
        mappings=mappings,
        source_aliases=source_aliases,
        sink_aliases=sink_aliases,
        compatibility=compatibility,
    )


@lru_cache(maxsize=8)
def _available_type_map_pairs(project_root_key: str) -> list[tuple[str, str]]:
    """List available ``(source_engine, sink_engine)`` map pairs."""
    from pathlib import Path

    definitions_path = Path(_definitions_dir(project_root_key))
    if not definitions_path.is_dir():
        return []

    pairs: list[tuple[str, str]] = []
    for file_path in definitions_path.glob("map-*.yaml"):
        name = file_path.stem
        if not name.startswith("map-"):
            continue

        tail = name[4:]
        if "-to-" in tail:
            source_engine, sink_engine = tail.split("-to-", 1)
        elif "-" in tail:
            source_engine, sink_engine = tail.split("-", 1)
        else:
            continue

        if source_engine and sink_engine:
            pairs.append((source_engine.casefold(), sink_engine.casefold()))

    return sorted(set(pairs))


def _resolve_engine_pair(
    project_root_key: str,
    source_type: str,
    sink_type: str,
    source_engine: str,
    sink_engine: str,
) -> TypeCompatibilityMap:
    """Resolve concrete engine pair for compatibility checks."""
    source_engine_norm = source_engine.casefold()
    sink_engine_norm = sink_engine.casefold()

    if _AUTO_ENGINE not in (source_engine_norm, sink_engine_norm):
        return _load_type_compatibility_map(
            project_root_key,
            source_engine_norm,
            sink_engine_norm,
        )

    candidates = _available_type_map_pairs(project_root_key)
    if not candidates:
        raise ValueError(
            "No type compatibility maps found in services/_schemas/_definitions/. "
            + "Add map-{source}-{sink}.yaml, e.g. map-mssql-pgsql.yaml"
        )

    scoped_candidates = [
        (src, sink)
        for src, sink in candidates
        if source_engine_norm in (_AUTO_ENGINE, src)
        and sink_engine_norm in (_AUTO_ENGINE, sink)
    ]
    if not scoped_candidates:
        raise ValueError(
            "No matching type compatibility map found for requested engines: "
            + f"source='{source_engine}', sink='{sink_engine}'."
        )

    source_raw_normalized = _normalize_type_name(source_type)
    sink_raw_normalized = _normalize_type_name(sink_type)
    best: TypeCompatibilityMap | None = None
    best_score = -1

    for src_engine_name, sink_engine_name in scoped_candidates:
        spec = _load_type_compatibility_map(
            project_root_key,
            src_engine_name,
            sink_engine_name,
        )
        source_normalized = _normalize_type_name(
            source_raw_normalized,
            spec.source_aliases,
        )
        sink_normalized = _normalize_type_name(
            sink_raw_normalized,
            spec.sink_aliases,
        )

        known_source = (
            source_normalized in spec.mappings
            or source_normalized in spec.compatibility
            or _WILDCARD_SOURCE in spec.compatibility
        )
        known_sink = (
            sink_normalized in set(spec.mappings.values())
            or any(sink_normalized in targets for targets in spec.compatibility.values())
        )
        score = int(known_source) + int(known_sink)
        if score > best_score:
            best = spec
            best_score = score

    if best is None:
        raise ValueError(
            "Failed to resolve a compatibility map for source/sink types: "
            + f"'{source_type}' -> '{sink_type}'"
        )

    return best


def _check_with_type_map(
    spec: TypeCompatibilityMap,
    source_type: str,
    sink_type: str,
) -> bool:
    """Evaluate compatibility against a resolved compatibility map."""
    source_normalized = _normalize_type_name(source_type, spec.source_aliases)
    sink_normalized = _normalize_type_name(sink_type, spec.sink_aliases)

    mapped = spec.mappings.get(source_normalized)
    if mapped is not None and mapped == sink_normalized:
        return True

    direct_targets = spec.compatibility.get(source_normalized, set())
    wildcard_targets = spec.compatibility.get(_WILDCARD_SOURCE, set())
    return sink_normalized in direct_targets or sink_normalized in wildcard_targets


@lru_cache(maxsize=1)
def _pgsql_native_fallback_map() -> TypeCompatibilityMap:
    """Fallback compatibility for map-less pgsql↔pgsql contexts."""
    mappings = {type_name: type_name for type_name in _PGSQL_FALLBACK_BASE_TYPES}

    compatibility: dict[str, set[str]] = {
        "smallint": {
            "smallint", "integer", "bigint", "numeric", "real", "double precision",
        },
        "integer": {"integer", "bigint", "numeric", "real", "double precision"},
        "bigint": {"bigint", "numeric", "real", "double precision"},
        "numeric": {"numeric", "real", "double precision"},
        "real": {"real", "double precision"},
        "date": {"date", "timestamp without time zone", "timestamp with time zone"},
        "timestamp without time zone": {
            "timestamp without time zone",
            "timestamp with time zone",
        },
    }

    return TypeCompatibilityMap(
        source_engine="pgsql",
        sink_engine="pgsql",
        mappings=mappings,
        source_aliases=dict(_PGSQL_FALLBACK_ALIASES),
        sink_aliases=dict(_PGSQL_FALLBACK_ALIASES),
        compatibility=compatibility,
    )


def _can_use_pgsql_native_fallback(source_type: str, sink_type: str) -> bool:
    """Return True when both types are recognizable pgsql-native types."""
    fallback_spec = _pgsql_native_fallback_map()
    source_normalized = _normalize_type_name(source_type, fallback_spec.source_aliases)
    sink_normalized = _normalize_type_name(sink_type, fallback_spec.sink_aliases)

    known_sources = (
        set(fallback_spec.mappings.keys())
        | set(fallback_spec.compatibility.keys())
        | {_WILDCARD_SOURCE}
    )
    known_sinks = (
        set(fallback_spec.mappings.values())
        | {
            sink_name
            for target_set in fallback_spec.compatibility.values()
            for sink_name in target_set
        }
    )

    return source_normalized in known_sources and sink_normalized in known_sinks


def validate_pg_schema_name(schema: str) -> str | None:
    """Validate that *schema* is a valid unquoted PostgreSQL identifier.

    Valid identifiers start with a letter or underscore, followed by
    letters, digits, underscores, or dollar signs (max 63 chars).
    Hyphens, spaces, and leading digits are NOT allowed.

    Args:
        schema: Schema name to validate.

    Returns:
        Error message if invalid, None if valid.

    Examples:
        >>> validate_pg_schema_name('public')  # None (valid)
        >>> validate_pg_schema_name('directory_clone')  # None (valid)
        >>> validate_pg_schema_name('directory-clone')  # error message
        >>> validate_pg_schema_name('123abc')  # error message
    """
    if not schema:
        return "Schema name cannot be empty"

    if len(schema) > _PG_IDENTIFIER_MAX_LENGTH:
        return (
            f"Schema name '{schema}' exceeds PostgreSQL maximum "
            f"of {_PG_IDENTIFIER_MAX_LENGTH} characters"
        )

    if not _PG_IDENTIFIER_PATTERN.match(schema):
        # Build a specific hint about what's wrong
        if "-" in schema:
            suggestion = schema.replace("-", "_")
            return (
                f"Schema name '{schema}' contains hyphens which are invalid "
                f"in PostgreSQL identifiers. Use underscores instead: '{suggestion}'"
            )
        if schema[0].isdigit():
            return (
                f"Schema name '{schema}' starts with a digit. "
                "Use a name starting with a letter or underscore, "
                "for example: '_clone' or 'clone_1'"
            )
        return (
            f"Schema name '{schema}' contains invalid characters. "
            "Allowed: letters, digits, underscore (_), dollar sign ($), "
            "and must start with a letter or underscore"
        )

    return None


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Internal helpers — parsing & validation
# ---------------------------------------------------------------------------


def _parse_sink_key(sink_key: str) -> tuple[str, str] | None:
    """Parse 'sink_group.target_service' → (sink_group, target_service).

    Returns None if the format is invalid.
    """
    parts = sink_key.split(_SINK_KEY_SEPARATOR, 1)
    if len(parts) != _SINK_KEY_PARTS:
        return None
    return parts[0], parts[1]


def _validate_sink_group_exists(sink_group: str) -> bool:
    """Return True if *sink_group* exists in sink-groups.yaml."""
    sink_file = get_project_root() / "sink-groups.yaml"
    if not sink_file.exists():
        return False
    try:
        from cdc_generator.helpers.yaml_loader import load_yaml_file

        sink_groups = load_yaml_file(sink_file)
        return sink_group in sink_groups
    except (FileNotFoundError, ValueError):
        return False


def _validate_and_parse_sink_key(sink_key: str) -> tuple[str, str] | None:
    """Parse *sink_key*, printing an error if the format is invalid."""
    parsed = _parse_sink_key(sink_key)
    if parsed is None:
        print_error(
            f"Invalid sink key '{sink_key}'. Expected format: "
            + "sink_group.target_service (e.g., sink_asma.chat)"
        )
    return parsed


# ---------------------------------------------------------------------------
# Internal helpers — schema validation
# ---------------------------------------------------------------------------


def _get_target_service_from_sink_key(sink_key: str) -> str | None:
    """Extract target_service from sink key 'sink_group.target_service'."""
    parsed = _parse_sink_key(sink_key)
    return parsed[1] if parsed else None


def _list_tables_in_service_schemas(target_service: str) -> list[str]:
    """List all tables for sink target across preferred/legacy schema roots.

    Returns:
        List of 'schema.table' strings.
    """
    tables: set[str] = set()
    for service_dir in get_service_schema_read_dirs(target_service, get_project_root()):
        if not service_dir.is_dir():
            continue
        for schema_dir in service_dir.iterdir():
            if not schema_dir.is_dir():
                continue
            for table_file in schema_dir.glob("*.yaml"):
                tables.add(f"{schema_dir.name}.{table_file.stem}")
    return sorted(tables)


def _validate_table_in_schemas(
    sink_key: str,
    table_key: str,
) -> bool:
    """Validate that table_key exists in schema files for the sink target.

    Prints friendly errors if schemas are missing or table not found.

    Returns:
        True if valid, False if validation failed.
    """
    target_service = _get_target_service_from_sink_key(sink_key)
    if not target_service:
        return False

    has_service_dir = any(
        service_dir.is_dir()
        for service_dir in get_service_schema_read_dirs(target_service, get_project_root())
    )
    if not has_service_dir:
        print_error(f"No schemas found for sink target '{target_service}'")
        print_info(
            "To fetch schemas, run:\n"
            + "  cdc manage-services config --service <SERVICE>"
            + f" --inspect-sink {sink_key} --all --save"
        )
        print_info(
            "Or create manually: "
            + f"services/_schemas/{target_service}/<schema>/<Table>.yaml"
        )
        return False

    available = _list_tables_in_service_schemas(target_service)
    if not available:
        print_error(
            f"Schema directory for '{target_service}' exists but is empty"
        )
        print_info(
            "To populate schemas, run:\n"
            + "  cdc manage-services config --service <SERVICE>"
            + f" --inspect-sink {sink_key} --all --save"
        )
        return False

    if table_key not in available:
        print_error(
            f"Table '{table_key}' not found in schemas for "
            + f"target service '{target_service}'"
        )
        print_info(
            "Available tables:\n  "
            + "\n  ".join(available)
        )
        return False

    return True


# ---------------------------------------------------------------------------
# Internal helpers — typed dict access on loaded YAML data
# ---------------------------------------------------------------------------


def _get_source_tables_dict(config: dict[str, object]) -> dict[str, object]:
    """Return source.tables dict (typed), or empty dict if absent."""
    source_raw = config.get("source")
    if not isinstance(source_raw, dict):
        return {}
    source = cast(dict[str, object], source_raw)
    tables_raw = source.get("tables")
    if not isinstance(tables_raw, dict):
        return {}
    return cast(dict[str, object], tables_raw)


def _get_source_table_keys(config: dict[str, object]) -> list[str]:
    """Return list of source table keys (e.g. ['public.users', …])."""
    return [str(k) for k in _get_source_tables_dict(config)]


def _get_sinks_dict(config: dict[str, object]) -> dict[str, object]:
    """Return the sinks section, creating it if absent.

    Returns a *mutable* reference into *config*.
    """
    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        config["sinks"] = {}
        return cast(dict[str, object], config["sinks"])
    return cast(dict[str, object], sinks_raw)


def _get_sink_tables(sink_cfg: dict[str, object]) -> dict[str, object]:
    """Return the tables dict inside a single sink config, creating if absent."""
    tables_raw = sink_cfg.get("tables")
    if not isinstance(tables_raw, dict):
        sink_cfg["tables"] = {}
        return cast(dict[str, object], sink_cfg["tables"])
    return cast(dict[str, object], tables_raw)


def _resolve_sink_config(
    sinks: dict[str, object],
    sink_key: str,
) -> dict[str, object] | None:
    """Return typed sink config dict, or None (with error) if invalid."""
    sink_raw = sinks.get(sink_key)
    if not isinstance(sink_raw, dict):
        print_error(f"Invalid sink configuration for '{sink_key}'")
        return None
    return cast(dict[str, object], sink_raw)


# ---------------------------------------------------------------------------
# Public API — add / remove sink
# ---------------------------------------------------------------------------


def add_sink_to_service(service: str, sink_key: str) -> bool:
    """Add a sink destination to *service*.

    Args:
        service: Service name (e.g., 'directory').
        sink_key: Sink key 'sink_group.target_service' (e.g. 'sink_asma.chat').

    Returns:
        True on success, False otherwise.
    """
    parsed = _validate_and_parse_sink_key(sink_key)
    if parsed is None:
        return False

    sink_group, _target = parsed
    if not _validate_sink_group_exists(sink_group):
        print_error(f"Sink group '{sink_group}' not found in sink-groups.yaml")
        print_info("Run 'cdc manage-sink-groups --list' to see available groups")
        return False

    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks = _get_sinks_dict(config)
    if sink_key in sinks:
        print_warning(f"Sink '{sink_key}' already exists in service '{service}'")
        return False

    sinks[sink_key] = {"tables": {}}
    if not save_service_config(service, config):
        return False

    print_success(f"Added sink '{sink_key}' to service '{service}'")
    return True


def remove_sink_from_service(service: str, sink_key: str) -> bool:
    """Remove a sink destination from *service*.

    Returns:
        True on success, False otherwise.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks = _get_sinks_dict(config)
    if sink_key not in sinks:
        print_warning(f"Sink '{sink_key}' not found in service '{service}'")
        available = [str(k) for k in sinks]
        if available:
            print_info(f"Available sinks: {', '.join(available)}")
        return False

    del sinks[sink_key]
    if not save_service_config(service, config):
        return False

    print_success(f"Removed sink '{sink_key}' from service '{service}'")
    return True


# ---------------------------------------------------------------------------
# Public API — add / remove sink table
# ---------------------------------------------------------------------------


def _build_table_config(opts: TableConfigOptions) -> dict[str, object]:
    """Build the per-table config dict from the given options.

    target_exists is ALWAYS included in the output.
    """
    cfg: dict[str, object] = {"target_exists": opts.target_exists}

    # Add 'from' field if provided
    if opts.from_table is not None:
        cfg["from"] = opts.from_table

    # Add 'replicate_structure' if True
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

    # Add column template if provided
    if opts.column_template:
        template_entry: dict[str, str] = {"template": opts.column_template}
        if opts.column_template_name:
            template_entry["name"] = opts.column_template_name
        if opts.column_template_value:
            template_entry["value"] = opts.column_template_value
        cfg["column_templates"] = [template_entry]

    return cfg


def _validate_table_add(
    config: dict[str, object],
    sink_key: str,
    table_key: str,
    table_opts: dict[str, object],
    skip_schema_validation: bool = False,
) -> tuple[dict[str, object] | None, str | None]:
    """Validate parameters for adding table to sink.

    Args:
        config: Service config dict.
        sink_key: Sink key.
        table_key: Table key to add.
        table_opts: Table options dict.
        skip_schema_validation: If True, skip checking if table exists in service-schemas
            (used for custom tables with --sink-schema).

    Returns:
        (sink_tables_dict, error_msg) — tables dict on success, or None + error.
    """
    sinks = _get_sinks_dict(config)
    sink_cfg = _resolve_sink_config(sinks, sink_key) if sink_key in sinks else None

    if sink_cfg is None:
        return None, f"Sink '{sink_key}' not found"

    tables = _get_sink_tables(sink_cfg)

    if table_key in tables:
        print_warning(f"Table '{table_key}' already in sink '{sink_key}'")
        return None, None  # None error = soft failure (warning already shown)

    if "target_exists" not in table_opts:
        return (
            None,
            "Missing required parameter 'target_exists'. "
            + "Specify --target-exists true (map to existing table) or "
            + "--target-exists false (autocreate clone)",
        )

    # Validate 'from' field references a valid source table
    from_table = table_opts.get("from")
    if from_table is None:
        return (
            None,
            "Missing required parameter 'from'. "
            + "Specify --from <schema.table> to map sink table data source.",
        )

    if from_table is not None:
        source_tables = _get_source_table_keys(config)
        if str(from_table) not in source_tables:
            available = "\n  ".join(source_tables) if source_tables else "(none)"
            return (
                None,
                f"Source table '{from_table}' not found in service.\n"
                + f"Available source tables:\n  {available}",
            )

    # Validate table exists in service-schemas for the sink target
    # Skip for custom tables (when sink_schema is provided)
    if not skip_schema_validation and not _validate_table_in_schemas(sink_key, table_key):
        return None, None  # Error already printed

    return tables, None


def _save_custom_table_structure(
    sink_key: str,
    table_key: str,
    from_table: str,
    source_service: str,
) -> None:
    """Save minimal reference file to service-schemas/{target}/custom-tables/.

    Creates a lightweight YAML reference that points to the source table schema.
    Base structure (columns, PKs, types) is deduced from source at generation time.
    This file stores only source/sink linkage metadata.
    Non-deducible sink behavior (column_templates, transforms) is
    stored in service YAML as the single source of truth.

    Args:
        sink_key: Sink key (e.g., 'sink_asma.notification').
        table_key: Target table key (e.g., 'notification.customer_user').
        from_table: Source table key (e.g., 'public.customer_user').
        source_service: Source service name to find original schema.
    """

    target_service = _get_target_service_from_sink_key(sink_key)
    if not target_service:
        print_warning("Could not determine target service from sink key")
        return

    # Parse source and target table keys
    if "." not in from_table or "." not in table_key:
        print_warning(f"Invalid table format: {from_table} or {table_key}")
        return

    source_schema, source_table = from_table.split(".", 1)
    target_schema, target_table = table_key.split(".", 1)

    source_file = None
    for source_service_dir in get_service_schema_read_dirs(
        source_service,
        get_project_root(),
    ):
        candidate = source_service_dir / source_schema / f"{source_table}.yaml"
        if candidate.exists():
            source_file = candidate
            break

    if source_file is None:
        print_warning(
            "Source table schema not found for "
            + f"{source_service}.{source_schema}.{source_table}\n"
            + "Custom table reference will not be saved. "
            + "Run inspect on source service first."
        )
        return

    # Create minimal reference file
    reference_data: dict[str, object] = {
        "source_reference": {
            "service": source_service,
            "schema": source_schema,
            "table": source_table,
        },
        "sink_target": {
            "schema": target_schema,
            "table": target_table,
        },
    }
        
    # Target directory and file
    target_dir = SERVICE_SCHEMAS_DIR / target_service / "custom-tables"
    target_dir.mkdir(parents=True, exist_ok=True)

    target_file = target_dir / f"{table_key.replace('/', '_')}.yaml"

    # Save minimal reference with comments as header
    try:
        target_file.write_text(
            "# Minimal reference file - base structure deduced from source at generation time\n"
            + "\n"
            + "# source_reference: Points to the source table schema\n"
            + "# sink_target: Defines the target schema/table in sink database\n"
            + "#\n"
            + "# Base structure (columns, types, PKs) is deduced from source at generation time.\n"
            + "# Non-deducible sink behavior (column_templates, transforms)\n"
            + "# is stored in services/<service>.yaml under sinks.<sink>.tables.<table>.\n"
            + "\n",
            encoding="utf-8",
        )

        with target_file.open("a", encoding="utf-8") as f:
            import yaml

            yaml.dump(reference_data, f, default_flow_style=False, sort_keys=False)

        print_success(
            f"Saved custom table reference: {target_file.relative_to(SERVICE_SCHEMAS_DIR.parent)}"
        )
    except Exception as exc:
        print_warning(f"Failed to save custom table reference: {exc}")


def _is_required_sink_column(column: dict[str, Any]) -> bool:
    """Return True if sink column requires source input.

    Required means either primary key or non-nullable without default value.
    """
    is_pk = bool(column.get("primary_key", False))
    nullable = bool(column.get("nullable", True))
    has_default = (
        column.get("default") is not None
        or column.get("default_value") is not None
    )
    return (is_pk or not nullable) and not has_default


def _collect_named_column_types(
    columns: list[dict[str, Any]],
) -> dict[str, str]:
    """Return {column_name: type} for columns with both fields present."""
    result: dict[str, str] = {}
    for col in columns:
        name = col.get("name")
        col_type = col.get("type")
        if isinstance(name, str) and isinstance(col_type, str):
            result[name] = col_type
    return result


def _analyze_identity_coverage(
    source_types: dict[str, str],
    sink_types: dict[str, str],
    source_table: str,
    mapped_sink_columns: set[str],
    pre_covered: set[str] | None = None,
) -> tuple[set[str], list[tuple[str, str, str]]]:
    """Return identity-compatible and identity-incompatible sink columns."""
    identity_covered: set[str] = set()
    incompatible_identity: list[tuple[str, str, str]] = []
    already_covered: set[str] = (
        pre_covered if pre_covered is not None else set()
    )

    for sink_col, sink_type in sink_types.items():
        if sink_col in mapped_sink_columns or sink_col in already_covered:
            continue

        source_type = source_types.get(sink_col)
        if source_type is None:
            continue

        if _check_identity_type_compatibility(
            source_type,
            sink_type,
            source_table=source_table,
            source_column=sink_col,
        ):
            identity_covered.add(sink_col)
            continue

        incompatible_identity.append((sink_col, source_type, sink_type))

    return identity_covered, incompatible_identity


def _check_identity_type_compatibility(
    source_type: str,
    sink_type: str,
    source_table: str | None = None,
    source_column: str | None = None,
) -> bool:
    """Return True when implicit same-name mapping is type-safe.

    Identity mapping is intentionally stricter than explicit --map-column
    mapping. We only allow implicit mapping when source/sink types resolve
    to the same canonical type.
    """
    return _check_type_compatibility(
        source_type,
        sink_type,
        source_table=source_table,
        source_column=source_column,
    )


def _find_required_unmapped_sink_columns(
    sink_columns: list[dict[str, Any]],
    covered: set[str],
) -> list[str]:
    """List required sink columns not covered by explicit/identity mapping."""
    required_unmapped: list[str] = []
    for sink_col in sink_columns:
        col_name = sink_col.get("name")
        if not isinstance(col_name, str):
            continue
        if col_name in covered:
            continue
        if _is_required_sink_column(sink_col):
            required_unmapped.append(col_name)
    return required_unmapped


def _validate_accepted_columns(
    accepted_columns: list[str],
    sink_columns: list[dict[str, Any]],
) -> str | None:
    """Validate --accept-column values reference known sink columns."""
    sink_column_names = {
        str(col.get("name"))
        for col in sink_columns
        if isinstance(col.get("name"), str)
    }
    invalid = sorted(
        column_name
        for column_name in accepted_columns
        if column_name not in sink_column_names
    )
    if not invalid:
        return None

    available = ", ".join(sorted(sink_column_names))
    return (
        "Invalid --accept-column value(s): "
        + ", ".join(invalid)
        + f"\nAvailable sink columns: {available}"
    )


def _collect_column_template_coverage(opts: TableConfigOptions) -> set[str]:
    """Collect sink column names covered by add-time column template options."""
    if not opts.column_template:
        return set()

    if opts.column_template_name:
        return {opts.column_template_name}

    try:
        from cdc_generator.core.column_templates import get_template

        template = get_template(opts.column_template)
    except (FileNotFoundError, ValueError):
        template = None

    if template is None:
        return {opts.column_template}

    return {template.name, opts.column_template}


def _collect_add_transform_coverage(opts: TableConfigOptions) -> set[str]:
    """Collect sink column names covered by add-time transform option."""
    if not opts.add_transform:
        return set()

    return _collect_transform_coverage_from_entries(
        [{"bloblang_ref": opts.add_transform}],
    )


def _extract_bloblang_output_columns(bloblang: str) -> set[str]:
    """Extract probable output column names from Bloblang transform content."""
    from cdc_generator.validators.bloblang_parser import (
        strip_bloblang_comments,
    )

    normalized_bloblang = strip_bloblang_comments(bloblang)
    output_columns: set[str] = set()

    for match in re.finditer(
        r"root\.([a-zA-Z_][a-zA-Z0-9_$]*)\s*=",
        normalized_bloblang,
    ):
        output_columns.add(match.group(1))

    for merge_match in re.finditer(
        r"merge\(\s*\{(.*?)\}\s*\)",
        normalized_bloblang,
        re.DOTALL,
    ):
        body = merge_match.group(1)
        for key_match in re.finditer(r"[\"']([a-zA-Z_][a-zA-Z0-9_$]*)[\"']\s*:", body):
            output_columns.add(key_match.group(1))

    return output_columns


def _collect_transform_coverage_from_entries(
    transforms: list[object],
) -> set[str]:
    """Collect sink column names produced by transform entries."""
    covered: set[str] = set()

    for item in transforms:
        if not isinstance(item, dict):
            continue

        entry = cast(dict[str, object], item)

        expected_output = entry.get("expected_output_column")
        if isinstance(expected_output, str) and expected_output:
            covered.add(expected_output)

        rule_name = entry.get("rule")
        if isinstance(rule_name, str) and rule_name:
            try:
                from cdc_generator.core.transform_rules import get_rule

                rule = get_rule(rule_name)
            except (FileNotFoundError, ValueError):
                rule = None

            if (
                rule is not None
                and rule.output_column is not None
            ):
                covered.add(rule.output_column.name)

        bloblang_ref = entry.get("bloblang_ref")
        if isinstance(bloblang_ref, str) and bloblang_ref:
            try:
                from cdc_generator.core.bloblang_refs import read_bloblang_ref

                bloblang = read_bloblang_ref(bloblang_ref)
            except (FileNotFoundError, ValueError):
                bloblang = None

            if isinstance(bloblang, str) and bloblang:
                covered.update(_extract_bloblang_output_columns(bloblang))

    return covered


def _collect_source_transform_coverage(
    config: dict[str, object],
    source_table: str,
) -> set[str]:
    """Collect sink column names covered by source-table transform rules."""
    source_tables = _get_source_tables_dict(config)
    source_cfg_raw = source_tables.get(source_table)
    if not isinstance(source_cfg_raw, dict):
        return set()

    source_cfg = cast(dict[str, object], source_cfg_raw)
    transforms_raw = source_cfg.get("transforms")
    if not isinstance(transforms_raw, list):
        return set()

    return _collect_transform_coverage_from_entries(cast(list[object], transforms_raw))


def _build_add_table_compatibility_guidance(
    source_table: str,
    sink_table: str,
    incompatible_identity: list[tuple[str, str, str]],
    required_unmapped: list[str],
    source_names: list[str],
) -> str:
    """Build a user-friendly compatibility error with mapping suggestions."""
    guidance_lines: list[str] = [
        "Source/sink column compatibility check failed.",
        f"Source table: {source_table}",
        f"Sink table: {sink_table}",
    ]

    if incompatible_identity:
        guidance_lines.append("Incompatible same-name columns:")
        for col_name, src_type, sink_type in incompatible_identity:
            guidance_lines.append(
                f"  - {col_name}: source={src_type}, sink={sink_type}"
            )
            guidance_lines.append(
                f"    Suggestion: --map-column {col_name} <sink_column>"
            )

    if required_unmapped:
        guidance_lines.append(
            "Required sink columns without compatible source mapping:"
        )
        for col_name in sorted(required_unmapped):
            guidance_lines.append(f"  - {col_name}")
            if source_names:
                candidates = ", ".join(source_names[:5])
                guidance_lines.append(
                    "    Suggestion: add --map-column "
                    + f"<source_column> {col_name} "
                    + f"(available source columns: {candidates})"
                )

    guidance_lines.append(
        "When columns match by name and type, mapping is applied implicitly."
    )
    return "\n".join(guidance_lines)


def _validate_add_table_schema_compatibility(
    config: dict[str, object],
    service: str,
    sink_key: str,
    source_fallback_table: str,
    sink_table_key: str,
    opts: TableConfigOptions,
) -> str | None:
    """Validate source/sink column compatibility for add_sink_table flow.

    Applies only when mapping to an existing target table
    (target_exists=True) and replicate_structure=False.
    """
    if opts.replicate_structure or not opts.target_exists:
        return None

    source_table = opts.from_table if opts.from_table else source_fallback_table
    sink_table = opts.target if opts.target else sink_table_key

    target_service = _get_target_service_from_sink_key(sink_key)
    if target_service is None:
        return f"Invalid sink key format: '{sink_key}'"

    source_columns = _load_table_columns(service, source_table)
    sink_columns = _load_table_columns(target_service, sink_table)
    if source_columns is None or sink_columns is None:
        print_warning(
            "Skipping add-time compatibility check because schema files are missing."
        )
        print_info(
            "Run inspect/save to enable strict checks: "
            + f"--inspect --all --save and --inspect-sink {sink_key} --all --save"
        )
        return None

    explicit_mappings = opts.columns or {}
    mapping_errors = _validate_column_mappings(
        list(explicit_mappings.items()),
        source_columns,
        sink_columns,
        source_table,
        sink_table,
    )
    if mapping_errors:
        details = "\n  - ".join(mapping_errors)
        return (
            "Invalid --map-column configuration:\n"
            + f"  - {details}"
        )

    source_types = _collect_named_column_types(source_columns)
    sink_types = _collect_named_column_types(sink_columns)
    accepted_columns = opts.accepted_columns or []
    accepted_columns_set = {
        column_name.strip()
        for column_name in accepted_columns
        if column_name.strip()
    }
    accepted_validation_error = _validate_accepted_columns(
        sorted(accepted_columns_set),
        sink_columns,
    )
    if accepted_validation_error:
        return accepted_validation_error

    mapped_sink_columns = set(explicit_mappings.values())
    template_covered = _collect_column_template_coverage(opts)
    transform_covered = _collect_source_transform_coverage(config, source_table)
    add_transform_covered = _collect_add_transform_coverage(opts)
    generated_covered = (
        template_covered
        | transform_covered
        | add_transform_covered
        | accepted_columns_set
    )
    try:
        identity_covered, incompatible_identity = _analyze_identity_coverage(
            source_types,
            sink_types,
            source_table,
            mapped_sink_columns,
            generated_covered,
        )
    except ValueError as exc:
        return (
            "Type compatibility map error: "
            + str(exc)
        )
    covered = mapped_sink_columns | generated_covered | identity_covered
    required_unmapped = _find_required_unmapped_sink_columns(
        sink_columns,
        covered,
    )

    if not incompatible_identity and not required_unmapped:
        return None

    source_names = sorted(source_types.keys())
    return _build_add_table_compatibility_guidance(
        source_table,
        sink_table,
        incompatible_identity,
        required_unmapped,
        source_names,
    )


def add_sink_table(
    service: str,
    sink_key: str,
    table_key: str,
    table_opts: dict[str, object] | None = None,
) -> bool:
    """Add *table_key* to the sink identified by *sink_key*.

    Args:
        service: Service name.
        sink_key: Sink key (e.g., 'sink_asma.chat').
        table_key: Source table in format 'schema.table'.
        table_opts: Optional table config dict. REQUIRED key:
            target_exists (bool). Other keys: target, target_schema,
            include_columns, columns, from, replicate_structure, sink_schema,
            column_template, column_template_name, column_template_value.

    Returns:
        True on success, False otherwise.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    opts = table_opts if table_opts is not None else {}

    # Handle sink_schema override - change table_key schema
    sink_schema = opts.get("sink_schema")
    final_table_key = table_key

    if sink_schema is not None:
        # Validate schema name before using it
        schema_error = validate_pg_schema_name(str(sink_schema))
        if schema_error:
            print_error(schema_error)
            return False

        # Override schema in table_key
        if "." in table_key:
            _schema, table_name = table_key.split(".", 1)
            final_table_key = f"{sink_schema}.{table_name}"
            print_info(
                f"Using sink schema '{sink_schema}' "
                + f"(table: {final_table_key})"
            )
        else:
            print_error(
                f"Invalid table key '{table_key}': expected 'schema.table' format"
            )
            return False

    tables, error = _validate_table_add(
        config, sink_key, final_table_key, opts, skip_schema_validation=sink_schema is not None
    )

    if error or tables is None:
        if error:
            print_error(error)
        return False

    target_exists = bool(opts.get("target_exists", False))
    target = opts.get("target")
    from_table = opts.get("from")
    replicate_structure = bool(opts.get("replicate_structure", False))

    # Validate target_schema if provided
    raw_target_schema = opts.get("target_schema")
    if raw_target_schema is not None:
        ts_error = validate_pg_schema_name(str(raw_target_schema))
        if ts_error:
            print_error(ts_error)
            return False

    config_opts = TableConfigOptions(
        target_exists=target_exists,
        target=str(target) if target else None,
        target_schema=(
            str(opts["target_schema"]) if "target_schema" in opts else None
        ),
        include_columns=(
            cast(list[str], opts["include_columns"])
            if "include_columns" in opts
            else None
        ),
        columns=(
            cast(dict[str, str], opts["columns"])
            if "columns" in opts
            else None
        ),
        from_table=str(from_table) if from_table is not None else None,
        replicate_structure=replicate_structure,
        sink_schema=str(sink_schema) if sink_schema is not None else None,
        column_template=(
            str(opts["column_template"])
            if "column_template" in opts
            else None
        ),
        column_template_name=(
            str(opts["column_template_name"])
            if "column_template_name" in opts
            else None
        ),
        column_template_value=(
            str(opts["column_template_value"])
            if "column_template_value" in opts
            else None
        ),
        add_transform=(
            str(opts["add_transform"])
            if "add_transform" in opts
            else None
        ),
        accepted_columns=(
            cast(list[str], opts["accepted_columns"])
            if "accepted_columns" in opts
            else None
        ),
    )

    compatibility_error = _validate_add_table_schema_compatibility(
        config,
        service,
        sink_key,
        table_key,
        final_table_key,
        config_opts,
    )
    if compatibility_error:
        print_error(compatibility_error)
        return False

    tables[final_table_key] = _build_table_config(config_opts)

    if not save_service_config(service, config):
        return False

    # Save custom table reference if replicate_structure is enabled
    # Use from_table if provided, otherwise source table is final_table_key
    if sink_schema is not None and replicate_structure:
        source_table = str(from_table) if from_table else table_key
        _save_custom_table_structure(
            sink_key,
            final_table_key,
            source_table,
            service,
        )

    label = f"→ '{target}'" if target_exists and target else "(clone)"
    print_success(f"Added table '{final_table_key}' {label} to sink '{sink_key}'")
    return True


def remove_sink_table(service: str, sink_key: str, table_key: str) -> bool:
    """Remove *table_key* from a service sink.

    Also removes the related custom-table YAML file from
    ``service-schemas/{target_service}/custom-tables/`` if it exists.

    Returns:
        True on success, False otherwise.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks = _get_sinks_dict(config)
    if sink_key not in sinks:
        print_error(f"Sink '{sink_key}' not found in service '{service}'")
        return False

    sink_cfg = _resolve_sink_config(sinks, sink_key)
    if sink_cfg is None:
        return False

    tables = _get_sink_tables(sink_cfg)
    if table_key not in tables:
        print_warning(f"Table '{table_key}' not found in sink '{sink_key}'")
        return False

    del tables[table_key]
    if not save_service_config(service, config):
        return False

    # Clean up custom-table YAML if it exists
    _remove_custom_table_file(sink_key, table_key)

    print_success(f"Removed table '{table_key}' from sink '{sink_key}'")
    return True


def _remove_custom_table_file(sink_key: str, table_key: str) -> None:
    """Remove the custom-table YAML reference file if it exists.

    Args:
        sink_key: Sink key (e.g., 'sink_asma.proxy').
        table_key: Table key (e.g., 'directory-clone.customers').
    """
    target_service = _get_target_service_from_sink_key(sink_key)
    if not target_service:
        return

    filename = f"{table_key.replace('/', '_')}.yaml"
    removed_paths: list[str] = []
    for service_dir in get_service_schema_read_dirs(target_service, get_project_root()):
        custom_file = service_dir / "custom-tables" / filename
        if custom_file.is_file():
            custom_file.unlink()
            removed_paths.append(str(custom_file.relative_to(get_project_root())))

    if removed_paths:
        print_info(
            "Removed custom table file(s): " + ", ".join(removed_paths)
        )


def _validate_schema_update_inputs(
    service: str,
    sink_key: str,
    table_key: str,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Validate inputs for update_sink_table_schema.

    Returns:
        Tuple of (config, tables_dict) if valid, None on error.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return None

    sinks = _get_sinks_dict(config)
    if sink_key not in sinks:
        print_error(f"Sink '{sink_key}' not found in service '{service}'")
        return None

    sink_cfg = _resolve_sink_config(sinks, sink_key)
    if sink_cfg is None:
        return None

    tables = _get_sink_tables(sink_cfg)
    if table_key not in tables:
        print_error(f"Table '{table_key}' not found in sink '{sink_key}'")
        print_info(
            f"Available tables in '{sink_key}':\n  "
            + "\n  ".join(str(k) for k in tables)
        )
        return None

    return config, tables


def update_sink_table_schema(
    service: str,
    sink_key: str,
    table_key: str,
    new_schema: str,
) -> bool:
    """Update the schema portion of a sink table's name.

    Args:
        service: Service name.
        sink_key: Sink key (e.g., 'sink_asma.chat').
        table_key: Current table key (e.g., 'public.customer_user').
        new_schema: New schema name (e.g., 'calendar').

    Returns:
        True on success, False otherwise.

    Example:
        update_sink_table_schema(
            'directory', 'sink_asma.calendar',
            'public.customer_user', 'calendar'
        )
        # Changes 'public.customer_user' to 'calendar.customer_user'
    """
    # Validate new schema name
    schema_error = validate_pg_schema_name(new_schema)
    if schema_error:
        print_error(schema_error)
        return False

    result = _validate_schema_update_inputs(service, sink_key, table_key)
    if result is None:
        return False

    config, tables = result

    # Parse current table key to get table name
    if "." not in table_key:
        print_error(
            f"Invalid table key '{table_key}': expected 'schema.table' format"
        )
        return False

    parts = table_key.split(".", 1)
    old_schema = parts[0]
    table_name = parts[1]
    new_table_key = f"{new_schema}.{table_name}"

    # Check if new table key already exists
    if new_table_key in tables:
        print_error(
            f"Table '{new_table_key}' already exists in sink '{sink_key}'"
        )
        return False

    # Move the table config to new key
    table_config = tables[table_key]
    tables[new_table_key] = table_config
    del tables[table_key]

    if not save_service_config(service, config):
        return False

    print_success(
        f"Updated table schema: '{old_schema}.{table_name}' → "
        + f"'{new_schema}.{table_name}' in sink '{sink_key}'"
    )
    return True


# ---------------------------------------------------------------------------
# Public API — column mapping
# ---------------------------------------------------------------------------


def map_sink_column(
    service: str,
    sink_key: str,
    table_key: str,
    source_column: str,
    target_column: str,
) -> bool:
    """Add/update a column mapping.  Sets target_exists=true automatically.

    Returns:
        True on success, False otherwise.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks = _get_sinks_dict(config)
    if sink_key not in sinks:
        print_error(f"Sink '{sink_key}' not found in service '{service}'")
        return False

    sink_cfg = _resolve_sink_config(sinks, sink_key)
    if sink_cfg is None:
        return False

    tables = _get_sink_tables(sink_cfg)
    if table_key not in tables:
        print_error(f"Table '{table_key}' not found in sink '{sink_key}'")
        return False

    tbl_raw = tables[table_key]
    if not isinstance(tbl_raw, dict):
        tbl_raw = {}
        tables[table_key] = tbl_raw
    tbl_cfg = cast(dict[str, object], tbl_raw)
    tbl_cfg["target_exists"] = True

    cols_raw = tbl_cfg.get("columns")
    if not isinstance(cols_raw, dict):
        tbl_cfg["columns"] = {}
        cols_raw = tbl_cfg["columns"]
    cols = cast(dict[str, str], cols_raw)
    cols[source_column] = target_column

    if not save_service_config(service, config):
        return False

    print_success(
        f"Mapped column '{source_column}' → '{target_column}'"
        + f" in '{table_key}' of sink '{sink_key}'"
    )
    return True


# ---------------------------------------------------------------------------
# Public API — column mapping on existing sink table (with validation)
# ---------------------------------------------------------------------------


def _load_table_columns(
    service: str,
    table_key: str,
) -> list[dict[str, Any]] | None:
    """Load column definitions from service-schemas/{service}/{schema}/{table}.yaml.

    Returns:
        List of column dicts (name, type, nullable, primary_key), or None.
    """
    if "." not in table_key:
        return None

    schema, table = table_key.split(".", 1)
    schema_file = SERVICE_SCHEMAS_DIR / service / schema / f"{table}.yaml"
    if not schema_file.exists():
        return None

    try:
        from cdc_generator.helpers.yaml_loader import load_yaml_file

        data = load_yaml_file(schema_file)
        columns = data.get("columns", [])
        if isinstance(columns, list):
            return cast(list[dict[str, Any]], columns)
    except (FileNotFoundError, ValueError):
        pass
    return None


def _get_column_type(
    columns: list[dict[str, Any]],
    column_name: str,
) -> str | None:
    """Find a column's type from a column definitions list."""
    for col in columns:
        if col.get("name") == column_name:
            raw_type = col.get("type")
            return str(raw_type) if raw_type is not None else None
    return None


def _check_type_compatibility(
    source_type: str,
    sink_type: str,
    source_engine: str = _AUTO_ENGINE,
    sink_engine: str = _AUTO_ENGINE,
    source_table: str | None = None,
    source_column: str | None = None,
) -> bool:
    """Check if source_type is compatible with sink_type.

    Uses runtime YAML type map(s) from ``services/_schemas/_definitions/``.
    """
    project_root = get_project_root()
    try:
        spec = _resolve_engine_pair(
            str(project_root),
            source_type,
            sink_type,
            source_engine,
            sink_engine,
        )
    except ValueError as exc:
        if (
            source_engine == _AUTO_ENGINE
            and sink_engine == _AUTO_ENGINE
            and "No type compatibility maps found" in str(exc)
            and _can_use_pgsql_native_fallback(source_type, sink_type)
        ):
            spec = _pgsql_native_fallback_map()
        else:
            raise

    effective_source_type = _resolve_effective_source_type(
        str(project_root),
        source_type,
        source_table,
        source_column,
    )

    return _check_with_type_map(spec, effective_source_type, sink_type)


def check_type_compatibility(
    source_type: str,
    sink_type: str,
    source_engine: str = _AUTO_ENGINE,
    sink_engine: str = _AUTO_ENGINE,
    source_table: str | None = None,
    source_column: str | None = None,
) -> bool:
    """Public compatibility helper for source/sink SQL column types."""
    return _check_type_compatibility(
        source_type,
        sink_type,
        source_engine,
        sink_engine,
        source_table,
        source_column,
    )


def _resolve_source_table_from_sink(
    config: dict[str, object],
    sink_key: str,
    table_key: str,
) -> str | None:
    """Resolve the source table for a sink table entry.

    Checks the 'from' field first, then falls back to matching source tables.
    """
    sinks = _get_sinks_dict(config)
    sink_cfg = _resolve_sink_config(sinks, sink_key)
    if sink_cfg is None:
        return None

    tables = _get_sink_tables(sink_cfg)
    tbl_raw = tables.get(table_key)
    if not isinstance(tbl_raw, dict):
        return None

    tbl_cfg = cast(dict[str, object], tbl_raw)

    # Check explicit 'from' field
    from_table = tbl_cfg.get("from")
    if isinstance(from_table, str):
        return from_table

    # Fall back: check if sink table_key matches a source table
    source_tables = _get_source_table_keys(config)
    if table_key in source_tables:
        return table_key

    return None


def _validate_column_mappings(
    column_mappings: list[tuple[str, str]],
    source_columns: list[dict[str, Any]],
    sink_columns: list[dict[str, Any]],
    source_table: str,
    table_key: str,
) -> list[str]:
    """Validate column mapping pairs against source/sink schemas.

    Returns:
        List of error messages (empty = all valid).
    """
    source_col_names = {col["name"] for col in source_columns if "name" in col}
    sink_col_names = {col["name"] for col in sink_columns if "name" in col}

    errors: list[str] = []
    for src_col, tgt_col in column_mappings:
        if src_col not in source_col_names:
            errors.append(
                f"Source column '{src_col}' not found in "
                + f"'{source_table}' (available: "
                + f"{', '.join(sorted(source_col_names))})"
            )
            continue

        if tgt_col not in sink_col_names:
            errors.append(
                f"Sink column '{tgt_col}' not found in "
                + f"'{table_key}' (available: "
                + f"{', '.join(sorted(sink_col_names))})"
            )
            continue

        # Type compatibility check
        src_type = _get_column_type(source_columns, src_col)
        tgt_type = _get_column_type(sink_columns, tgt_col)
        if src_type and tgt_type:
            if _is_text_like_type(tgt_type) and (
                _is_numeric_like_type(src_type)
                or _is_uuid_like_type(src_type)
            ):
                continue

            try:
                is_compatible = _check_type_compatibility(
                    src_type,
                    tgt_type,
                    source_table=source_table,
                    source_column=src_col,
                )
            except ValueError as exc:
                errors.append(
                    "Type compatibility map error: "
                    + str(exc)
                )
                continue

            if not is_compatible:
                errors.append(
                    f"Type mismatch: '{src_col}' ({src_type}) "
                    + f"→ '{tgt_col}' ({tgt_type})"
                )
    return errors


def _is_text_like_type(type_name: str) -> bool:
    """Return True for text-like SQL sink types."""
    normalized = _normalize_type_name(type_name)
    return any(
        marker in normalized
        for marker in (
            "char",
            "text",
            "string",
            "varchar",
            "nvarchar",
            "nchar",
            "citext",
            "user-defined",
        )
    )


def _is_uuid_like_type(type_name: str) -> bool:
    """Return True for UUID-like SQL types."""
    normalized = _normalize_type_name(type_name)
    return "uuid" in normalized or "uniqueidentifier" in normalized


def _is_numeric_like_type(type_name: str) -> bool:
    """Return True for numeric SQL types."""
    normalized = _normalize_type_name(type_name)
    return any(
        marker in normalized
        for marker in (
            "int",
            "integer",
            "bigint",
            "smallint",
            "tinyint",
            "numeric",
            "decimal",
            "float",
            "double",
            "real",
            "money",
        )
    )


def _apply_column_mappings(
    tables: dict[str, object],
    table_key: str,
    column_mappings: list[tuple[str, str]],
) -> dict[str, str]:
    """Write column mapping pairs into the table config.

    Returns:
        The columns dict after applying.
    """
    tbl_raw = tables[table_key]
    if not isinstance(tbl_raw, dict):
        tbl_raw = {}
        tables[table_key] = tbl_raw
    tbl_cfg = cast(dict[str, object], tbl_raw)
    tbl_cfg["target_exists"] = True

    cols_raw = tbl_cfg.get("columns")
    if not isinstance(cols_raw, dict):
        tbl_cfg["columns"] = {}
        cols_raw = tbl_cfg["columns"]
    cols = cast(dict[str, str], cols_raw)

    for src_col, tgt_col in column_mappings:
        cols[src_col] = tgt_col
    return cols


def _warn_unmapped_required(
    sink_columns: list[dict[str, Any]],
    cols: dict[str, str],
    source_col_names: set[str],
    sink_col_names: set[str],
) -> None:
    """Warn about unmapped non-nullable sink columns."""
    mapped_sink_cols = set(cols.values())
    identity_mapped = source_col_names & sink_col_names
    all_covered = mapped_sink_cols | identity_mapped

    unmapped_required: list[str] = []
    for col in sink_columns:
        col_name = col.get("name", "")
        nullable = col.get("nullable", True)
        is_pk = col.get("primary_key", False)
        if (not nullable or is_pk) and col_name not in all_covered:
            unmapped_required.append(col_name)

    if unmapped_required:
        print_warning(
            "Unmapped required sink columns (non-nullable or PK): "
            + ", ".join(sorted(unmapped_required))
        )


def _load_schemas_for_mapping(
    source_service: str,
    source_table: str,
    target_service: str,
    table_key: str,
    service: str,
    sink_key: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
    """Load source and sink column schemas for column mapping.

    Returns:
        (source_columns, sink_columns) tuple, or None on error.
    """
    source_columns = _load_table_columns(source_service, source_table)
    sink_columns = _load_table_columns(target_service, table_key)

    if source_columns is None:
        src_path = source_table.replace(".", "/")
        print_error(
            "Source table schema not found: "
            + f"service-schemas/{source_service}/{src_path}.yaml"
        )
        print_info(
            "Run: cdc manage-services config --service "
            + f"{service} --inspect --all --save"
        )
        return None

    if sink_columns is None:
        tgt_path = table_key.replace(".", "/")
        print_error(
            "Sink table schema not found: "
            + f"service-schemas/{target_service}/{tgt_path}.yaml"
        )
        print_info(
            f"Run: cdc manage-services config --service {service}"
            + f" --inspect-sink {sink_key} --all --save"
        )
        return None

    return source_columns, sink_columns


@dataclass
class _MappingContext:
    """Resolved context needed for column mapping."""

    config: dict[str, object]
    tables: dict[str, object]
    source_table: str
    source_columns: list[dict[str, Any]]
    sink_columns: list[dict[str, Any]]


def _resolve_mapping_context(
    service: str,
    sink_key: str,
    table_key: str,
) -> _MappingContext | None:
    """Resolve and validate all context needed for column mapping.

    Returns:
        _MappingContext on success, None on error (messages printed).
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return None

    sinks = _get_sinks_dict(config)
    if sink_key not in sinks:
        print_error(f"Sink '{sink_key}' not found in service '{service}'")
        return None

    sink_cfg = _resolve_sink_config(sinks, sink_key)
    if sink_cfg is None:
        return None

    tables = _get_sink_tables(sink_cfg)
    if table_key not in tables:
        print_error(f"Table '{table_key}' not found in sink '{sink_key}'")
        available = [str(k) for k in tables]
        if available:
            print_info(f"Available tables: {', '.join(available)}")
        return None

    source_table = _resolve_source_table_from_sink(config, sink_key, table_key)
    if source_table is None:
        print_error(
            f"Cannot determine source table for '{table_key}' in sink '{sink_key}'"
        )
        print_info(
            "Ensure the sink table has a 'from' field or matches a source table"
        )
        return None

    target_service = _get_target_service_from_sink_key(sink_key)
    if target_service is None:
        print_error(f"Invalid sink key format: '{sink_key}'")
        return None

    schemas = _load_schemas_for_mapping(
        service, source_table, target_service, table_key, service, sink_key,
    )
    if schemas is None:
        return None
    source_columns, sink_columns = schemas

    return _MappingContext(
        config=config,
        tables=tables,
        source_table=source_table,
        source_columns=source_columns,
        sink_columns=sink_columns,
    )


def map_sink_columns(
    service: str,
    sink_key: str,
    table_key: str,
    column_mappings: list[tuple[str, str]],
) -> bool:
    """Map multiple columns on an existing sink table with validation.

    Validates that:
    - Source columns exist in the source table schema
    - Sink columns exist in the sink table schema
    - Column types are compatible between source and sink
    - Warns about unmapped required (non-nullable) sink columns

    Args:
        service: Service name.
        sink_key: Sink key (e.g., 'sink_asma.proxy').
        table_key: Sink table key (e.g., 'public.directory_user_name').
        column_mappings: List of (source_column, sink_column) tuples.

    Returns:
        True on success, False on validation error.
    """
    ctx = _resolve_mapping_context(service, sink_key, table_key)
    if ctx is None:
        return False

    # Validate each mapping
    errors = _validate_column_mappings(
        column_mappings, ctx.source_columns, ctx.sink_columns,
        ctx.source_table, table_key,
    )
    if errors:
        for err in errors:
            print_error(f"  ✗ {err}")
        return False

    # Apply mappings
    cols = _apply_column_mappings(ctx.tables, table_key, column_mappings)

    if not save_service_config(service, ctx.config):
        return False

    for src_col, tgt_col in column_mappings:
        print_success(f"Mapped column '{src_col}' → '{tgt_col}'")

    # Warn about unmapped required sink columns
    source_col_names = {
        col["name"] for col in ctx.source_columns if "name" in col
    }
    sink_col_names = {
        col["name"] for col in ctx.sink_columns if "name" in col
    }
    _warn_unmapped_required(
        ctx.sink_columns, cols, source_col_names, sink_col_names,
    )

    print_info("Run 'cdc generate' to update pipelines")
    return True


# ---------------------------------------------------------------------------
# Public API — list & validate
# ---------------------------------------------------------------------------


def _format_mapped_table(
    tbl_key: str,
    tbl_cfg: dict[str, object],
) -> None:
    """Print a single *mapped* table (target_exists=true)."""
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


def _format_cloned_table(
    tbl_key: str,
    tbl_cfg: dict[str, object],
) -> None:
    """Print a single *cloned* table (target_exists=false/absent)."""
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


def _format_sink_entry(
    sink_key: str,
    sink_cfg: dict[str, object],
) -> None:
    """Print header + table rows for one sink entry."""
    # Header
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

    # Tables
    tables_raw = sink_cfg.get("tables", {})
    tables = cast(dict[str, object], tables_raw) if isinstance(tables_raw, dict) else {}
    if not tables:
        print(f"  {Colors.DIM}No tables configured{Colors.RESET}")
        return

    for tbl_key_raw, tbl_raw in tables.items():
        tbl_key = str(tbl_key_raw)
        tbl_cfg = cast(dict[str, object], tbl_raw) if isinstance(tbl_raw, dict) else {}
        if tbl_cfg.get("target_exists", False):
            _format_mapped_table(tbl_key, tbl_cfg)
        else:
            _format_cloned_table(tbl_key, tbl_cfg)

    # Databases
    db_raw = sink_cfg.get("databases", {})
    if isinstance(db_raw, dict) and db_raw:
        databases = cast(dict[str, object], db_raw)
        print(f"  {Colors.DIM}Databases:{Colors.RESET}")
        for env, db in databases.items():
            print(f"    {env}: {db}")


def list_sinks(service: str) -> bool:
    """List all sinks configured for *service*.

    Returns:
        True if sinks were found and displayed, False otherwise.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict) or not sinks_raw:
        print_info(f"No sinks configured for service '{service}'")
        return False

    sinks = cast(dict[str, object], sinks_raw)
    print_header(f"Sinks for service '{service}'")

    for sk_raw, sc_raw in sinks.items():
        sk = str(sk_raw)
        sc = cast(dict[str, object], sc_raw) if isinstance(sc_raw, dict) else {}
        _format_sink_entry(sk, sc)

    src_count = len(_get_source_table_keys(config))
    print(f"\n{Colors.DIM}Source tables: {src_count}{Colors.RESET}")
    return True


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_single_sink(
    sink_key_str: str,
    sink_raw: object,
    source_tables: list[str],
) -> list[str]:
    """Validate one sink entry, returning a list of error messages."""
    errors: list[str] = []

    parsed = _parse_sink_key(sink_key_str)
    if parsed is None:
        errors.append(
            f"Invalid sink key '{sink_key_str}'. "
            + "Expected: sink_group.target_service"
        )
        return errors

    sink_group, _ts = parsed
    if not _validate_sink_group_exists(sink_group):
        errors.append(
            f"Sink group '{sink_group}' (in '{sink_key_str}') "
            + "not found in sink-groups.yaml"
        )

    if not isinstance(sink_raw, dict):
        return errors

    sink_cfg = cast(dict[str, object], sink_raw)
    tables_raw = sink_cfg.get("tables", {})
    if not isinstance(tables_raw, dict):
        return errors

    tables = cast(dict[str, object], tables_raw)
    for tbl_key_raw, tbl_raw in tables.items():
        tbl_key = str(tbl_key_raw)

        # Validate schema portion of table key
        if "." in tbl_key:
            tbl_schema = tbl_key.split(".", 1)[0]
            schema_err = validate_pg_schema_name(tbl_schema)
            if schema_err:
                errors.append(
                    f"Table '{tbl_key}' in sink '{sink_key_str}': {schema_err}"
                )

        if tbl_key not in source_tables:
            print_warning(
                f"Table '{tbl_key}' in sink '{sink_key_str}'"
                + " not found in source.tables"
            )
        if not isinstance(tbl_raw, dict):
            continue
        tbl_cfg = cast(dict[str, object], tbl_raw)

        # REQUIRED: target_exists must be present
        if "target_exists" not in tbl_cfg:
            errors.append(
                f"Table '{tbl_key}' in sink '{sink_key_str}' "
                + "missing required field 'target_exists'. "
                + "Use 'target_exists: true' (map to existing table) or "
                + "'target_exists: false' (autocreate clone)"
            )
            continue

        target_exists = tbl_cfg.get("target_exists", False)
        if not isinstance(target_exists, bool):
            errors.append(
                f"Table '{tbl_key}' in sink '{sink_key_str}' "
                + "has invalid 'target_exists' value. Must be true or false"
            )
            continue

        if target_exists and "target" not in tbl_cfg:
            errors.append(
                f"Table '{tbl_key}' in sink '{sink_key_str}'"
                + " has target_exists=true but no 'target' field"
            )

    return errors


def validate_sinks(service: str) -> bool:
    """Validate sink configuration for *service*.

    Checks sink key format, sink group existence, source table presence,
    and required fields for target_exists=true tables.

    Returns:
        True if all validations pass, False otherwise.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict) or not sinks_raw:
        print_info(f"No sinks configured for service '{service}'")
        return True

    sinks = cast(dict[str, object], sinks_raw)
    source_tables = _get_source_table_keys(config)
    all_valid = True

    for sk_raw, sc_raw in sinks.items():
        for error in _validate_single_sink(str(sk_raw), sc_raw, source_tables):
            print_error(f"  ✗ {error}")
            all_valid = False

    if all_valid:
        print_success(f"Sink configuration for service '{service}' is valid")
    else:
        print_error(f"Sink validation failed for service '{service}'")

    return all_valid
