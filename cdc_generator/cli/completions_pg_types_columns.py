"""PostgreSQL type and custom column-spec completion helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from click.shell_completion import CompletionItem

SafeCall = Callable[..., list[str]]
FilterFn = Callable[[list[str], str], list[CompletionItem]]

_COLUMN_SPEC_NAME_ONLY_PARTS = 1
_COLUMN_SPEC_NAME_TYPE_PARTS = 2


def _extract_default_aliases(raw_defaults: object) -> list[str]:
    """Extract default alias names from a YAML defaults node."""
    aliases: list[str] = []

    if isinstance(raw_defaults, str):
        return [raw_defaults]

    if isinstance(raw_defaults, dict):
        defaults_map = cast(dict[str, Any], raw_defaults)
        return [str(alias) for alias in defaults_map]

    if isinstance(raw_defaults, list):
        defaults_list = cast(list[Any], raw_defaults)
        for item in defaults_list:
            if isinstance(item, str):
                aliases.append(item)
            elif isinstance(item, dict):
                item_map = cast(dict[str, Any], item)
                aliases.extend(str(alias) for alias in item_map)

    return aliases


def _default_aliases_from_definitions(col_type: str) -> list[str]:
    """Resolve default aliases from new schema declarations model only."""
    from cdc_generator.helpers.service_config import get_project_root
    from cdc_generator.helpers.yaml_loader import load_yaml_file

    project_root = get_project_root()
    definitions_file = (
        project_root
        / "services"
        / "_schemas"
        / "_definitions"
        / "pgsql.yaml"
    )
    if not definitions_file.is_file():
        return []

    try:
        raw_data = load_yaml_file(definitions_file)
        data = cast(dict[str, Any], raw_data)
        normalized_type = col_type.strip().lower()
        resolved: list[str] = []

        type_defaults_raw = data.get("type_defaults")
        if isinstance(type_defaults_raw, dict):
            type_defaults = cast(dict[str, Any], type_defaults_raw)
            explicit = type_defaults.get(normalized_type)
            resolved.extend(_extract_default_aliases(explicit))

        categories_raw = data.get("categories")
        if isinstance(categories_raw, dict):
            categories = cast(dict[str, Any], categories_raw)
            for category_data in categories.values():
                if not isinstance(category_data, dict):
                    continue
                category = cast(dict[str, Any], category_data)
                type_names_raw = category.get("types")
                if not isinstance(type_names_raw, list):
                    continue

                type_names_list = cast(list[Any], type_names_raw)
                type_names = {
                    str(type_name).strip().lower()
                    for type_name in type_names_list
                    if isinstance(type_name, str)
                }
                if normalized_type not in type_names:
                    continue

                defaults_from_category = _extract_default_aliases(
                    category.get("defaults"),
                )
                if defaults_from_category:
                    resolved.extend(defaults_from_category)

        unique: list[str] = []
        seen: set[str] = set()
        for alias in resolved:
            if alias in seen:
                continue
            seen.add(alias)
            unique.append(alias)
        return unique
    except Exception:
        return []


def _default_modifiers_for_pg_type(col_type: str) -> list[str]:
    """Return sensible ``default_*`` completion modifiers for a PG type."""
    normalized = col_type.strip().lower()
    return _default_aliases_from_definitions(normalized)


def complete_pg_types_impl(
    incomplete: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete with PostgreSQL column types."""
    from cdc_generator.helpers.autocompletions.types import list_pg_column_types

    return filter_items(safe_call(list_pg_column_types), incomplete)


def complete_custom_table_column_spec_impl(
    incomplete: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete ``--column`` specs for schema custom table CRUD.

    Supported format: ``name:type[:modifier[:modifier...]]``.
    """
    parts = incomplete.split(":")

    if len(parts) == _COLUMN_SPEC_NAME_ONLY_PARTS:
        return []

    col_name = parts[0].strip()
    if not col_name:
        return []

    if len(parts) == _COLUMN_SPEC_NAME_TYPE_PARTS:
        type_prefix = parts[1]
        from cdc_generator.helpers.autocompletions.types import list_pg_column_types

        pg_types = safe_call(list_pg_column_types)
        candidates = [
            f"{col_name}:{pg_type}"
            for pg_type in pg_types
            if pg_type.startswith(type_prefix)
        ]
        return filter_items(candidates, incomplete)

    col_type = parts[1].strip().lower()
    modifiers = parts[2:]
    active_modifiers = {
        mod.strip().lower() for mod in modifiers[:-1] if mod.strip()
    }
    modifier_prefix = modifiers[-1] if modifiers else ""

    default_candidates = _default_modifiers_for_pg_type(col_type)

    structural_candidates: list[str] = []
    has_pk = "pk" in active_modifiers
    has_not_null = "not_null" in active_modifiers
    has_nullable = "nullable" in active_modifiers

    if not has_nullable:
        if not has_pk:
            structural_candidates.append("pk")
        if not has_not_null and not has_pk:
            structural_candidates.append("not_null")

    if not has_pk and not has_not_null and not has_nullable:
        structural_candidates.append("nullable")

    modifier_candidates = [
        *structural_candidates,
        *default_candidates,
    ]

    base = ":".join(parts[:-1])
    candidates = [
        f"{base}:{candidate}"
        for candidate in modifier_candidates
        if candidate.startswith(modifier_prefix)
        and candidate.lower() not in active_modifiers
    ]
    return filter_items(candidates, incomplete)
