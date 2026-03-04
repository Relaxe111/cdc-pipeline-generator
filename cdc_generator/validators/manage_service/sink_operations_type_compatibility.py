"""Type compatibility and validation for sink operations.

Contains type mapping, compatibility checking, and PostgreSQL identifier validation.
"""

import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, cast

from cdc_generator.helpers.yaml_loader import load_yaml_file
from cdc_generator.validators.manage_service.sink_operations_type_utils import (
    _extract_aliases,
    _extract_compatibility,
    _extract_mappings,
    _normalize_identifier_name,
    _normalize_source_table_key,
    _normalize_type_name,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Valid unquoted PostgreSQL identifier: starts with letter or underscore,
# followed by letters, digits, underscores, or dollar signs. Max 63 chars.
_PG_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_$]*$")
_PG_IDENTIFIER_MAX_LENGTH = 63

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




@lru_cache(maxsize=16)
def _load_source_type_overrides(project_root_key: str) -> dict[str, dict[str, str]]:
    """Load and validate source-only effective type overrides."""
    from pathlib import Path

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

            if isinstance(effective_type_raw, str):
                effective_type = _normalize_type_name(effective_type_raw)
            elif isinstance(effective_type_raw, dict):
                entry_raw = cast(dict[str, object], effective_type_raw)
                type_value_raw = entry_raw.get("type", entry_raw.get("effective_type"))
                if not isinstance(type_value_raw, str):
                    raise ValueError(
                        "Invalid source type overrides format: "
                        + f"{source_table}.{source_column} in '{override_file}' "
                        + "must define 'type' as a string."
                    )
                effective_type = _normalize_type_name(type_value_raw)
            else:
                raise ValueError(
                    "Invalid source type overrides format: "
                    + f"{source_table}.{source_column} in '{override_file}' "
                    + "must map to a string type or object with 'type'."
                )

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
        data_raw = cast(object, load_yaml_file(mapping_file))
    except Exception as exc:
        raise ValueError(
            "Failed to read type compatibility map: "
            + f"'{mapping_file}': {exc}"
        ) from exc

    if not isinstance(data_raw, dict):
        raise ValueError(
            "Invalid type compatibility map format: "
            + f"'{mapping_file}' must contain a YAML mapping at the top level."
        )

    data_dict = cast(dict[str, object], data_raw)

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
        _WILDCARD_SOURCE,
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
    candidate_load_errors: list[str] = []

    for src_engine_name, sink_engine_name in scoped_candidates:
        try:
            spec = _load_type_compatibility_map(
                project_root_key,
                src_engine_name,
                sink_engine_name,
            )
        except ValueError as exc:
            candidate_load_errors.append(
                f"{src_engine_name}->{sink_engine_name}: {exc}"
            )
            continue

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
        if candidate_load_errors:
            raise ValueError(
                "No usable type compatibility map found for source/sink types: "
                + f"'{source_type}' -> '{sink_type}'. "
                + "Invalid map candidates: "
                + "; ".join(candidate_load_errors)
            )
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