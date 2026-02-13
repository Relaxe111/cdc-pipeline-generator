"""Configuration management for CDC service files."""


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
    - New: server_group field (adopus=db-per-tenant, asma=db-shared)
    - Legacy: mode field (direct value)
    """
    from cdc_generator.helpers.service_config import load_service_config
    try:
        config = load_service_config(service)

        # Try new server_group field first
        server_group = config.get('server_group')
        if server_group:
            # Map server_group to mode
            if server_group == 'adopus':
                return 'db-per-tenant'
            if server_group == 'asma':
                return 'db-shared'

        # Fall back to legacy mode field
        mode = config.get('mode', 'db-per-tenant')
        return str(mode) if mode else 'db-per-tenant'
    except Exception:
        return 'db-per-tenant'
