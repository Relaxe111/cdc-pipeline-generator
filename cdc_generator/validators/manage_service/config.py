"""Configuration management for CDC service files."""

from typing import Any, cast

from cdc_generator.helpers.helpers_logging import print_error
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.service_schema_paths import (
    get_schema_write_root,
    get_service_schema_read_dirs,
)
from cdc_generator.helpers.yaml_loader import yaml

PROJECT_ROOT = get_project_root()
SERVICES_DIR = PROJECT_ROOT / "services"
SERVICE_SCHEMAS_DIR = get_schema_write_root(PROJECT_ROOT)
MIN_SIMILAR_BLOCK_SIZE = 2
ANCHOR_SUFFIX_START = 2
ANCHOR_PATH_PARTS = 2


def _sanitize_anchor_part(value: str) -> str:
    """Create a stable anchor-safe token from any key/path fragment."""
    safe = "".join(ch.lower() if ch.isalnum() else "_" for ch in value)
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.strip("_") or "node"


def _build_anchor_name(path_parts: list[str], used_names: set[str]) -> str:
    """Build a deterministic, unique anchor name for a mapping path."""
    base_parts = [
        _sanitize_anchor_part(part) for part in path_parts[-ANCHOR_PATH_PARTS:]
    ]
    base = "shared_defaults_" + "_".join(part for part in base_parts if part)
    candidate = base
    counter = ANCHOR_SUFFIX_START
    while candidate in used_names:
        candidate = f"{base}_{counter}"
        counter += 1
    used_names.add(candidate)
    return candidate


def _compact_similar_mapping_block(
    container: dict[str, object],
    path_parts: list[str],
    used_anchor_names: set[str],
) -> None:
    """Compact similar sibling mapping entries into anchor+merge form.

    A block is compacted when 2+ sibling entries are dicts with identical key sets
    and they share at least one common key-value pair while also having at least one
    differing key.
    """
    try:
        from ruamel.yaml.comments import CommentedMap
        from ruamel.yaml.mergevalue import MergeValue
    except Exception:
        return

    grouped_keys: dict[tuple[str, ...], list[str]] = {}
    for key, value in container.items():
        if not isinstance(value, dict):
            continue
        if "<<" in value:
            continue
        typed_value = cast(dict[str, object], value)
        key_signature = tuple(typed_value.keys())
        grouped_keys.setdefault(key_signature, []).append(key)

    for key_signature, sibling_keys in grouped_keys.items():
        if len(sibling_keys) < MIN_SIMILAR_BLOCK_SIZE:
            continue

        first_entry_raw = container.get(sibling_keys[0])
        if not isinstance(first_entry_raw, dict):
            continue
        first_entry = cast(dict[str, object], first_entry_raw)

        common_keys: list[str] = []
        for entry_key in key_signature:
            expected_value = first_entry.get(entry_key)
            if all(
                isinstance(container.get(sibling_key), dict)
                and cast(dict[str, object], container[sibling_key]).get(entry_key)
                == expected_value
                for sibling_key in sibling_keys
            ):
                common_keys.append(entry_key)

        if not common_keys:
            continue
        if len(common_keys) == len(key_signature):
            continue

        shared_defaults = CommentedMap()
        for shared_key in key_signature:
            if shared_key in common_keys:
                shared_defaults[shared_key] = first_entry.get(shared_key)

        anchor_name = _build_anchor_name(path_parts, used_anchor_names)
        cast(Any, shared_defaults).yaml_set_anchor(anchor_name, always_dump=True)

        for sibling_key in sibling_keys:
            sibling_raw = container.get(sibling_key)
            if not isinstance(sibling_raw, dict):
                continue
            sibling = cast(dict[str, object], sibling_raw)

            merged = CommentedMap()
            merge_value = MergeValue()
            cast(Any, merge_value).append(shared_defaults)
            merge_value.merge_pos = 0
            cast(Any, merged).add_yaml_merge(merge_value)

            for field_key in key_signature:
                if field_key in common_keys:
                    continue
                merged[field_key] = sibling.get(field_key)

            container[sibling_key] = merged


def _compact_repetitive_yaml_blocks(
    node: dict[str, object],
    path_parts: list[str],
    used_anchor_names: set[str],
) -> None:
    """Recursively compact repetitive mapping blocks in YAML-like structures."""
    for key, value in list(node.items()):
        if isinstance(value, dict):
            _compact_repetitive_yaml_blocks(
                cast(dict[str, object], value),
                [*path_parts, str(key)],
                used_anchor_names,
            )

    _compact_similar_mapping_block(node, path_parts, used_anchor_names)


def get_available_services() -> list[str]:
    """Get list of available services from services/ directory."""
    if not SERVICES_DIR.exists():
        return []
    return [f.stem for f in SERVICES_DIR.glob("*.yaml")]


def load_service_schema_tables(service: str, schema: str) -> list[str]:
    """Load table names from service schemas under preferred/legacy paths."""
    table_names: set[str] = set()
    for service_dir in get_service_schema_read_dirs(service, PROJECT_ROOT):
        schema_dir = service_dir / schema
        if not schema_dir.exists():
            continue
        table_names.update(f.stem for f in schema_dir.glob("*.yaml"))
    return sorted(table_names)


def get_table_schema_definition(service: str, schema: str, table: str) -> dict[str, object] | None:
    """Load table definition from preferred/legacy service schema paths."""
    for service_dir in get_service_schema_read_dirs(service, PROJECT_ROOT):
        table_file = service_dir / schema / f"{table}.yaml"
        if not table_file.exists():
            continue
        with table_file.open(encoding="utf-8") as f:
            return yaml.load(f)  # type: ignore[return-value]
    return None


def save_service_config(service: str, config: dict[str, object]) -> bool:
    """Save service configuration to file, using new format (service name as root key)."""
    try:
        service_file = SERVICES_DIR / f"{service}.yaml"

        # Remove 'service' field if present (it's redundant in new format)
        config_to_save = {k: v for k, v in config.items() if k != 'service'}

        # Keep repetitive YAML blocks concise and deterministic.
        _compact_repetitive_yaml_blocks(config_to_save, [service], set())

        # Wrap in service name key
        wrapped_config = {service: config_to_save}

        with service_file.open('w', encoding='utf-8') as f:
            yaml.dump(wrapped_config, f)
        return True
    except Exception as e:
        print_error(f"Failed to save config: {e}")
        return False


def detect_service_mode(service: str) -> str:
    """Detect service mode (db-per-tenant or shared-db).

    Supports both:
    - New: server_group field resolved via source-groups pattern
    - Legacy: mode field (direct value)
    """
    from cdc_generator.helpers.service_config import load_service_config
    from cdc_generator.helpers.yaml_loader import load_yaml_file

    def _normalize_mode(raw_mode: object) -> str | None:
        normalized = str(raw_mode).strip().lower()
        if normalized in {"db-per-tenant", "db_shared", "db-shared"}:
            return "db-shared" if normalized in {"db_shared", "db-shared"} else "db-per-tenant"
        if normalized in {"shared-db", "shared_db"}:
            return "db-shared"
        return None

    try:
        config = load_service_config(service)

        # Resolve via source-groups using server_group key when available.
        server_group = config.get('server_group')
        if isinstance(server_group, str) and server_group:
            source_groups_file = get_project_root() / 'source-groups.yaml'
            source_groups_data = load_yaml_file(source_groups_file)
            groups = source_groups_data.get('server_group')
            if isinstance(groups, dict):
                group_cfg = groups.get(server_group)
                if isinstance(group_cfg, dict):
                    resolved = _normalize_mode(
                        group_cfg.get('pattern', '')
                    )
                    if resolved is not None:
                        return resolved

        # Fall back to legacy mode field
        resolved_mode = _normalize_mode(config.get('mode', 'db-per-tenant'))
        if resolved_mode is not None:
            return resolved_mode
        return 'db-per-tenant'
    except Exception:
        return 'db-per-tenant'
