"""Type utility helpers for sink operation compatibility logic."""

import re
from typing import cast

_TYPE_WITH_ARGS_PATTERN = re.compile(r"^([^()]+)\s*\(.*\)$")


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
    wildcard_source: str,
) -> dict[str, set[str]]:
    """Extract normalized compatibility rules from payload."""
    compatibility_raw = data_dict.get("compatibility")
    if not isinstance(compatibility_raw, dict):
        return {}

    compatibility: dict[str, set[str]] = {}
    for src_type, targets_raw in cast(dict[str, object], compatibility_raw).items():
        normalized_src = (
            wildcard_source
            if src_type == wildcard_source
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
