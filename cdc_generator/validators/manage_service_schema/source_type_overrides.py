"""Source type override operations for manage-services resources CLI.

Provides read/update helpers for
``services/_schemas/_definitions/source-{source_group}-type-overrides.yaml``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import print_error, print_info, print_success
from cdc_generator.helpers.service_config import get_project_root, load_service_config
from cdc_generator.helpers.service_schema_paths import get_service_schema_read_dirs
from cdc_generator.helpers.yaml_loader import load_yaml_file, save_yaml_file
from cdc_generator.validators.manage_service.sink_operations import check_type_compatibility
from cdc_generator.validators.manage_service_schema.type_definitions import get_all_type_names

_SOURCE_GROUPS_FILE = "source-groups.yaml"
_OVERRIDES_PATTERN = "source-{source_group}-type-overrides.yaml"
_MAPPING_PAIR_SIZE = 2
_WILDCARD_SOURCE = "*"


def autodetect_single_service() -> str:
    """Return the only configured service name when unambiguous."""
    services_dir = get_project_root() / "services"
    if not services_dir.is_dir():
        return ""

    service_names = sorted(
        path.stem
        for path in services_dir.glob("*.yaml")
        if path.is_file() and not path.name.startswith("_")
    )
    return service_names[0] if len(service_names) == 1 else ""


def normalize_source_column_ref(raw_ref: str) -> str:
    """Normalize ``schema.table.column`` reference for matching."""
    parts = [part.strip() for part in raw_ref.split(".")]
    if len(parts) != 3:  # noqa: PLR2004
        raise ValueError(
            "Invalid source override reference '"
            + raw_ref
            + "'. Expected format: schema.table.column"
        )
    if any(not part for part in parts):
        raise ValueError(
            "Invalid source override reference '"
            + raw_ref
            + "'. Schema, table, and column must be non-empty."
        )
    return f"{parts[0].casefold()}.{parts[1].casefold()}.{parts[2].casefold()}"


def parse_set_override_spec(spec: str) -> tuple[str, str]:
    """Parse ``schema.table.column:type`` specification."""
    if ":" not in spec:
        raise ValueError(
            "Invalid --set-source-override value '"
            + spec
            + "'. Expected format: schema.table.column:type"
        )

    ref_raw, type_raw = spec.split(":", 1)
    normalized_ref = normalize_source_column_ref(ref_raw)
    normalized_type = " ".join(type_raw.strip().lower().split())
    if not normalized_type:
        raise ValueError(
            "Invalid --set-source-override value '"
            + spec
            + "'. Missing type after ':'."
        )
    return normalized_ref, normalized_type


def _resolve_single_source_group() -> tuple[str, dict[str, Any]]:
    """Resolve the single source group required by current implementation model."""
    source_groups_path = get_project_root() / _SOURCE_GROUPS_FILE
    if not source_groups_path.is_file():
        raise ValueError(
            "Missing source groups file: '"
            + str(source_groups_path)
            + "'."
        )

    source_groups_raw = load_yaml_file(source_groups_path)
    source_groups = cast(dict[str, object], source_groups_raw)
    source_group_names = sorted(
        name
        for name in source_groups
        if not name.startswith("_")
    )
    if len(source_group_names) != 1:
        raise ValueError(
            "Source overrides require exactly one source group in '"
            + str(source_groups_path)
            + f"'. Found {len(source_group_names)}."
        )

    source_group_name = source_group_names[0]
    source_group_raw = source_groups.get(source_group_name)
    if not isinstance(source_group_raw, dict):
        raise ValueError(
            "Invalid source group definition for '"
            + source_group_name
            + "'."
        )
    return source_group_name, cast(dict[str, Any], source_group_raw)


def _resolve_source_engine(source_group: dict[str, Any]) -> str:
    """Resolve normalized source engine key used by definitions files."""
    raw_source_type = source_group.get("source_type")
    source_type = str(raw_source_type).strip().casefold()
    if source_type in {"postgres", "postgresql", "pgsql"}:
        return "pgsql"
    if source_type == "mssql":
        return "mssql"
    raise ValueError(
        "Unsupported source_type in source groups: '"
        + str(raw_source_type)
        + "'. Expected one of: mssql, postgres, postgresql."
    )


def get_allowed_source_types() -> list[str]:
    """Return allowed canonical source types for current source engine."""
    _source_group_name, source_group = _resolve_single_source_group()
    source_engine = _resolve_source_engine(source_group)
    return get_all_type_names(source_engine)


def resolve_overrides_file_path() -> Path:
    """Return source-group-scoped override file path."""
    source_group_name, _source_group = _resolve_single_source_group()
    definitions_dir = get_project_root() / "services" / "_schemas" / "_definitions"
    return definitions_dir / _OVERRIDES_PATTERN.format(source_group=source_group_name)


def _build_initial_overrides_data() -> dict[str, object]:
    """Return default YAML structure for a new overrides file."""
    return {
        "metadata": {"version": 1},
        "overrides": {},
    }


def _sort_overrides(overrides: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    """Return deterministic case-insensitive sorted overrides map."""
    sorted_tables = sorted(overrides.keys(), key=str.casefold)
    result: dict[str, dict[str, str]] = {}
    for table_name in sorted_tables:
        columns = overrides[table_name]
        sorted_columns = {
            column_name: columns[column_name]
            for column_name in sorted(columns.keys(), key=str.casefold)
        }
        result[table_name] = sorted_columns
    return result


def load_source_type_overrides() -> tuple[Path, dict[str, object], dict[str, dict[str, str]]]:
    """Load source type overrides file, returning path + full data + normalized overrides."""
    override_path = resolve_overrides_file_path()

    if override_path.is_file():
        raw_data = load_yaml_file(override_path)
        data = cast(dict[str, object], raw_data)
    else:
        data = _build_initial_overrides_data()

    overrides_raw = data.get("overrides", {})
    overrides_data = cast(dict[str, object], overrides_raw)
    overrides: dict[str, dict[str, str]] = {}

    for table_raw, column_map_raw in overrides_data.items():
        if not isinstance(column_map_raw, dict):
            continue
        table_key = normalize_source_column_ref(f"{table_raw}.x").rsplit(".", 1)[0]

        normalized_columns: dict[str, str] = {}
        for col_raw, type_raw in cast(dict[str, object], column_map_raw).items():
            if not isinstance(type_raw, str):
                continue
            column_name = col_raw.strip().casefold()
            normalized_type = " ".join(type_raw.strip().lower().split())
            if column_name and normalized_type:
                normalized_columns[column_name] = normalized_type

        overrides[table_key] = normalized_columns

    return override_path, data, overrides


def list_override_refs(service: str) -> list[str]:
    """Return override refs as ``schema.table.column:type`` for display."""
    _ensure_service_exists(service)
    _override_path, _data, overrides = load_source_type_overrides()
    source_ref_display_map = {
        normalize_source_column_ref(ref): ref
        for ref in list_source_column_refs(service)
    }

    result: list[str] = []
    for table_name in sorted(overrides.keys(), key=str.casefold):
        for column_name in sorted(overrides[table_name].keys(), key=str.casefold):
            normalized_ref = f"{table_name}.{column_name}"
            display_ref = source_ref_display_map.get(normalized_ref, normalized_ref)
            result.append(f"{display_ref}:{overrides[table_name][column_name]}")
    return result


def list_overridden_column_refs(service: str) -> list[str]:
    """Return overridden refs as ``schema.table.column``."""
    _ensure_service_exists(service)
    _override_path, _data, overrides = load_source_type_overrides()
    source_ref_display_map = {
        normalize_source_column_ref(ref): ref
        for ref in list_source_column_refs(service)
    }

    result: list[str] = []
    for table_name in sorted(overrides.keys(), key=str.casefold):
        for column_name in sorted(overrides[table_name].keys(), key=str.casefold):
            normalized_ref = f"{table_name}.{column_name}"
            result.append(source_ref_display_map.get(normalized_ref, normalized_ref))
    return result


def list_source_column_refs(service: str) -> list[str]:
    """Return available source refs as ``schema.table.column`` from schema files."""
    _ensure_service_exists(service)
    from cdc_generator.helpers.autocompletions.tables import (
        list_columns_for_table,
        list_source_tables_for_service,
    )

    refs_by_normalized: dict[str, str] = {}
    for table_key in list_source_tables_for_service(service):
        parts = table_key.split(".", 1)
        if len(parts) != 2:  # noqa: PLR2004
            continue
        schema_name, table_name = parts

        for col_ref in list_columns_for_table(service, schema_name, table_name):
            normalized_ref = normalize_source_column_ref(col_ref)
            display_parts = [part.strip() for part in col_ref.split(".")]
            display_ref = ".".join(display_parts)
            refs_by_normalized.setdefault(normalized_ref, display_ref)

    return sorted(refs_by_normalized.values(), key=str.casefold)


def resolve_source_ref_display(service: str, source_ref: str) -> str:
    """Resolve normalized or mixed-case source ref to schema-cased display form."""
    normalized_ref = normalize_source_column_ref(source_ref)
    for ref in list_source_column_refs(service):
        if normalize_source_column_ref(ref) == normalized_ref:
            return ref
    return source_ref.strip()


def _ensure_service_exists(service: str) -> None:
    """Validate service exists by attempting to load it."""
    try:
        load_service_config(service)
    except FileNotFoundError as exc:
        raise ValueError(
            "Service not found: '"
            + service
            + "'. Use --service (or --source) with a valid service name."
        ) from exc


def _load_table_column_types(service: str, table_key: str) -> dict[str, str]:
    """Load ``column -> type`` for ``schema.table`` from schema resources."""
    if "." not in table_key:
        return {}

    schema_name, table_name = table_key.split(".", 1)
    for schema_dir in get_service_schema_read_dirs(service):
        table_file = schema_dir / schema_name / f"{table_name}.yaml"
        if not table_file.is_file():
            continue

        table_data = load_yaml_file(table_file)
        columns_raw = table_data.get("columns", [])
        columns = cast(list[object], columns_raw) if isinstance(columns_raw, list) else []

        result: dict[str, str] = {}
        for column_raw in columns:
            if not isinstance(column_raw, dict):
                continue
            column = cast(dict[str, object], column_raw)
            name_raw = column.get("name")
            type_raw = column.get("type")
            if not isinstance(name_raw, str) or not isinstance(type_raw, str):
                continue
            result[name_raw.casefold()] = " ".join(type_raw.strip().lower().split())
        return result

    return {}


def _iter_target_type_usages(
    service: str,
    source_table: str,
    source_column: str,
) -> list[str]:
    """Collect sink target column types that consume this source column."""
    config = load_service_config(service)
    sinks_raw = config.get("sinks", {})
    sinks = cast(dict[str, object], sinks_raw) if isinstance(sinks_raw, dict) else {}

    source_table_cf = source_table.casefold()
    source_column_cf = source_column.casefold()
    target_types: list[str] = []

    source_types = _load_table_column_types(service, source_table)
    source_column_exists = source_column_cf in source_types
    if not source_column_exists:
        return []

    source_table_keys = _source_table_keys(config)

    for sink_key, sink_cfg_raw in sinks.items():
        sink_usages = _iter_sink_target_type_usages(
            sink_key,
            sink_cfg_raw,
            source_table_cf,
            source_column_cf,
            source_table_keys,
        )
        target_types.extend(sink_usages)

    return target_types


def _source_table_keys(config: dict[str, object]) -> set[str]:
    """Return source table keys declared in service config."""
    source_raw = config.get("source")
    if not isinstance(source_raw, dict):
        return set()

    source_cfg = cast(dict[str, object], source_raw)
    tables_raw = source_cfg.get("tables")
    if not isinstance(tables_raw, dict):
        return set()

    return {table_name.casefold() for table_name in cast(dict[str, object], tables_raw)}


def _iter_sink_target_type_usages(
    sink_key: str,
    sink_cfg_raw: object,
    source_table_cf: str,
    source_column_cf: str,
    source_table_keys: set[str],
) -> list[str]:
    """Collect target type usages for one sink entry."""
    if not isinstance(sink_cfg_raw, dict):
        return []

    sink_cfg = cast(dict[str, object], sink_cfg_raw)
    tables_raw = sink_cfg.get("tables")
    if not isinstance(tables_raw, dict):
        return []

    sink_parts = sink_key.split(".", 1)
    if len(sink_parts) != _MAPPING_PAIR_SIZE:
        return []
    target_service = sink_parts[1]

    usages: list[str] = []
    for target_table, target_cfg_raw in cast(dict[str, object], tables_raw).items():
        target_cfg = cast(dict[str, object], target_cfg_raw) if isinstance(target_cfg_raw, dict) else None
        if target_cfg is None:
            continue

        resolved_source_table = _resolve_source_table_for_sink_table(
            target_table,
            target_cfg,
            source_table_keys,
        )
        if resolved_source_table != source_table_cf:
            continue

        target_column_types = _load_table_column_types(target_service, target_table)
        if not target_column_types:
            continue

        explicit_target_to_source = _parse_explicit_map_columns(target_cfg)
        usages.extend(
            _collect_target_types_for_source_column(
                target_column_types,
                explicit_target_to_source,
                source_column_cf,
            )
        )

    return usages


def _resolve_source_table_for_sink_table(
    target_table: str,
    target_cfg: dict[str, object],
    source_table_keys: set[str],
) -> str:
    """Resolve source table key for sink table config."""
    from_table_raw = target_cfg.get("from")
    if isinstance(from_table_raw, str):
        return from_table_raw.casefold()
    return target_table.casefold() if target_table.casefold() in source_table_keys else ""


def _parse_explicit_map_columns(target_cfg: dict[str, object]) -> dict[str, str]:
    """Parse explicit map_columns into ``target -> source`` mapping."""
    explicit_target_to_source: dict[str, str] = {}
    map_columns_raw = target_cfg.get("map_columns")
    if not isinstance(map_columns_raw, list):
        return explicit_target_to_source

    for map_entry in cast(list[object], map_columns_raw):
        if isinstance(map_entry, str) and ":" in map_entry:
            target_col_raw, source_col_raw = map_entry.split(":", 1)
            explicit_target_to_source[target_col_raw.strip().casefold()] = source_col_raw.strip().casefold()
            continue

        if not isinstance(map_entry, list | tuple):
            continue
        pair = list(cast(list[object] | tuple[object, ...], map_entry))
        if len(pair) != _MAPPING_PAIR_SIZE:
            continue
        if not isinstance(pair[0], str) or not isinstance(pair[1], str):
            continue
        explicit_target_to_source[pair[0].strip().casefold()] = pair[1].strip().casefold()

    return explicit_target_to_source


def _collect_target_types_for_source_column(
    target_column_types: dict[str, str],
    explicit_target_to_source: dict[str, str],
    source_column_cf: str,
) -> list[str]:
    """Collect explicit + implicit target types that use source column."""
    collected: list[str] = []

    for target_col, mapped_source_col in explicit_target_to_source.items():
        if mapped_source_col != source_column_cf:
            continue
        target_type = target_column_types.get(target_col)
        if target_type:
            collected.append(target_type)

    implicit_target_type = target_column_types.get(source_column_cf)
    if implicit_target_type:
        mapped_source_for_same_name = explicit_target_to_source.get(source_column_cf)
        if mapped_source_for_same_name in {None, source_column_cf}:
            collected.append(implicit_target_type)

    return collected


def _normalize_type_name(raw_type: str) -> str:
    """Normalize SQL type names for strict map overlap checks."""
    normalized = " ".join(raw_type.strip().lower().split())
    if "(" in normalized and normalized.endswith(")"):
        normalized = normalized.split("(", 1)[0].strip()
    return normalized


def _map_file_candidates_for_source_engine(source_engine: str) -> list[Path]:
    """List map files that start with current source engine."""
    definitions_dir = get_project_root() / "services" / "_schemas" / "_definitions"
    if not definitions_dir.is_dir():
        return []

    candidates = list(definitions_dir.glob(f"map-{source_engine}-*.yaml"))
    candidates.extend(list(definitions_dir.glob(f"map-{source_engine}-to-*.yaml")))
    return sorted(set(candidates))


def _extract_normalized_aliases(map_data: dict[str, object]) -> tuple[dict[str, str], dict[str, str]]:
    """Extract normalized source/sink aliases from map file payload."""
    aliases_raw = map_data.get("aliases")
    if not isinstance(aliases_raw, dict):
        return {}, {}

    aliases = cast(dict[str, object], aliases_raw)
    source_raw = aliases.get("source")
    sink_raw = aliases.get("sink")

    source_aliases: dict[str, str] = {}
    if isinstance(source_raw, dict):
        for key, value in cast(dict[str, object], source_raw).items():
            if isinstance(value, str):
                source_aliases[_normalize_type_name(key)] = _normalize_type_name(value)

    sink_aliases: dict[str, str] = {}
    if isinstance(sink_raw, dict):
        for key, value in cast(dict[str, object], sink_raw).items():
            if isinstance(value, str):
                sink_aliases[_normalize_type_name(key)] = _normalize_type_name(value)

    return source_aliases, sink_aliases


def _apply_alias(name: str, aliases: dict[str, str]) -> str:
    """Resolve alias chains for normalized type names."""
    current = name
    seen: set[str] = set()
    while current in aliases and current not in seen:
        seen.add(current)
        current = aliases[current]
    return current


def _map_payload_has_overlap(
    map_data: dict[str, object],
    source_type: str,
    target_type: str,
) -> bool:
    """Return True when a map payload contains direct overlap for source/target."""
    source_aliases, sink_aliases = _extract_normalized_aliases(map_data)
    normalized_source = _apply_alias(_normalize_type_name(source_type), source_aliases)
    normalized_target = _apply_alias(_normalize_type_name(target_type), sink_aliases)

    mappings_raw = map_data.get("mappings")
    mappings = cast(dict[str, object], mappings_raw) if isinstance(mappings_raw, dict) else {}
    mapped_target = mappings.get(normalized_source)
    if isinstance(mapped_target, str):
        mapped_target_normalized = _apply_alias(_normalize_type_name(mapped_target), sink_aliases)
        if mapped_target_normalized == normalized_target:
            return True

    compatibility_raw = map_data.get("compatibility")
    compatibility = cast(dict[str, object], compatibility_raw) if isinstance(compatibility_raw, dict) else {}

    direct_targets_raw = compatibility.get(normalized_source)
    direct_targets = cast(list[object], direct_targets_raw) if isinstance(direct_targets_raw, list) else []
    if any(
        _apply_alias(_normalize_type_name(item), sink_aliases) == normalized_target
        for item in direct_targets
        if isinstance(item, str)
    ):
        return True

    wildcard_targets_raw = compatibility.get(_WILDCARD_SOURCE)
    wildcard_targets = cast(list[object], wildcard_targets_raw) if isinstance(wildcard_targets_raw, list) else []
    return any(
        _apply_alias(_normalize_type_name(item), sink_aliases) == normalized_target
        for item in wildcard_targets
        if isinstance(item, str)
    )


def _has_direct_type_overlap(source_type: str, target_type: str) -> bool:
    """Return True when map files provide direct/compat overlap for this pair."""
    _source_group_name, source_group = _resolve_single_source_group()
    source_engine = _resolve_source_engine(source_group)
    for map_file in _map_file_candidates_for_source_engine(source_engine):
        map_raw = load_yaml_file(map_file)
        map_data = cast(dict[str, object], map_raw)
        if _map_payload_has_overlap(map_data, source_type, target_type):
            return True
    return False


def validate_override_type_for_column(
    service: str,
    source_ref: str,
    override_type: str,
) -> None:
    """Validate override type against known target mappings for the source column."""
    _ensure_service_exists(service)

    normalized_ref = normalize_source_column_ref(source_ref)
    schema_name, table_name, column_name = normalized_ref.split(".", 2)
    source_table = f"{schema_name}.{table_name}"

    source_column_refs = {
        normalize_source_column_ref(ref)
        for ref in list_source_column_refs(service)
    }
    if normalized_ref not in source_column_refs:
        raise ValueError(
            "Source column not found in schema resources: '"
            + normalized_ref
            + "'. Run inspect/save first or verify service/source table config."
        )

    allowed_types = {type_name.casefold() for type_name in get_allowed_source_types()}
    if override_type.casefold() not in allowed_types:
        raise ValueError(
            "Unsupported source override type '"
            + override_type
            + "'. Allowed types come from services/_schemas/_definitions/<source-engine>.yaml"
        )

    target_types = _iter_target_type_usages(service, source_table, column_name)
    for target_type in target_types:
        if not _has_direct_type_overlap(override_type, target_type):
            raise ValueError(
                "Override type '"
                + override_type
                + "' has no direct compatibility overlap for "
                + normalized_ref
                + f" (target type '{target_type}')."
            )
        if not check_type_compatibility(override_type, target_type):
            raise ValueError(
                "Override type '"
                + override_type
                + "' is incompatible with existing sink mapping for "
                + normalized_ref
                + f" (target type '{target_type}')."
            )


def set_source_override(service: str, spec: str) -> bool:
    """Set source type override from ``schema.table.column:type`` specification."""
    normalized_ref, override_type = parse_set_override_spec(spec)
    validate_override_type_for_column(service, normalized_ref, override_type)

    display_ref = resolve_source_ref_display(service, normalized_ref)
    display_parts = [part.strip() for part in display_ref.split(".")]
    display_table = f"{display_parts[0]}.{display_parts[1]}"
    display_column = display_parts[2]

    source_table = normalized_ref.rsplit(".", 1)[0]
    source_column = normalized_ref.rsplit(".", 1)[1]

    override_path, file_data, overrides = load_source_type_overrides()
    table_overrides = overrides.setdefault(source_table, {})
    existing_type = table_overrides.get(source_column)
    if existing_type is not None:
        if existing_type == override_type:
            print_info(
                "Source override already set: "
                + f"{normalized_ref}:{override_type}"
            )
            return True
        print_error(
            "Conflicting source override already exists for "
            + normalized_ref
            + f": '{existing_type}'. Remove it first before setting '{override_type}'."
        )
        return False

    table_overrides[source_column] = override_type
    sorted_overrides = _sort_overrides(overrides)

    display_overrides: dict[str, dict[str, str]] = {}
    for table_name, columns in sorted_overrides.items():
        for column_name, mapped_type in columns.items():
            normalized_key = f"{table_name}.{column_name}"
            if normalized_key == normalized_ref:
                display_overrides.setdefault(display_table, {})[display_column] = mapped_type
                continue

            display_key = resolve_source_ref_display(service, normalized_key)
            display_key_parts = [part.strip() for part in display_key.split(".")]
            display_table_name = f"{display_key_parts[0]}.{display_key_parts[1]}"
            display_column_name = display_key_parts[2]
            display_overrides.setdefault(display_table_name, {})[display_column_name] = mapped_type

    sorted_display_overrides = {
        table_name: {
            column_name: columns[column_name]
            for column_name in sorted(columns.keys(), key=str.casefold)
        }
        for table_name, columns in sorted(display_overrides.items(), key=lambda item: item[0].casefold())
    }

    new_data = dict(file_data)
    new_data["metadata"] = new_data.get("metadata", {"version": 1})
    new_data["overrides"] = sorted_display_overrides
    save_yaml_file(cast(dict[str, Any], new_data), override_path)

    print_success(f"Set source override: {display_ref}:{override_type}")
    print_info(f"Saved: {override_path.relative_to(get_project_root())}")
    return True


def remove_source_override(service: str, source_ref: str) -> bool:
    """Remove source type override for ``schema.table.column`` reference."""
    _ensure_service_exists(service)
    normalized_ref = normalize_source_column_ref(source_ref)
    source_table = normalized_ref.rsplit(".", 1)[0]
    source_column = normalized_ref.rsplit(".", 1)[1]

    override_path, file_data, overrides = load_source_type_overrides()
    table_overrides = overrides.get(source_table)
    if table_overrides is None or source_column not in table_overrides:
        print_error("Source override not found: " + normalized_ref)
        return False

    del table_overrides[source_column]
    if not table_overrides:
        del overrides[source_table]

    new_data = dict(file_data)
    new_data["metadata"] = new_data.get("metadata", {"version": 1})
    new_data["overrides"] = _sort_overrides(overrides)
    save_yaml_file(cast(dict[str, Any], new_data), override_path)

    display_ref = resolve_source_ref_display(service, normalized_ref)
    print_success(f"Removed source override: {display_ref}")
    print_info(f"Saved: {override_path.relative_to(get_project_root())}")
    return True


def print_source_overrides(service: str) -> None:
    """Print source overrides for service context."""
    _ensure_service_exists(service)
    refs = list_override_refs(service)
    if not refs:
        print_info("No source type overrides configured.")
        return

    print_info(f"Source type overrides ({len(refs)}):")
    for ref in refs:
        print(f"  {ref}")
