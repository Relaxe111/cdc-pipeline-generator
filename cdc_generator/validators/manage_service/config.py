"""Configuration management for CDC service files."""


from cdc_generator.helpers.helpers_logging import print_error
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.yaml_loader import yaml

PROJECT_ROOT = get_project_root()
SERVICES_DIR = PROJECT_ROOT / "services"
SERVICE_SCHEMAS_DIR = PROJECT_ROOT / "service-schemas"


def get_available_services() -> list[str]:
    """Get list of available services from services/ directory."""
    if not SERVICES_DIR.exists():
        return []
    return [f.stem for f in SERVICES_DIR.glob("*.yaml")]


def load_service_schema_tables(service: str, schema: str) -> list[str]:
    """Load table names from service-schemas/{service}/{schema}/*.yaml"""
    schema_dir = SERVICE_SCHEMAS_DIR / service / schema
    if not schema_dir.exists():
        return []
    return sorted([f.stem for f in schema_dir.glob("*.yaml")])


def get_table_schema_definition(service: str, schema: str, table: str) -> dict[str, object] | None:
    """Load table definition from service-schemas/{service}/{schema}/{table}.yaml"""
    table_file = SERVICE_SCHEMAS_DIR / service / schema / f"{table}.yaml"
    if not table_file.exists():
        return None
    with open(table_file) as f:
        return yaml.load(f)  # type: ignore[return-value]


def save_service_config(service: str, config: dict[str, object]) -> bool:
    """Save service configuration to file, using new format (service name as root key)."""
    try:
        service_file = SERVICES_DIR / f"{service}.yaml"

        # Remove 'service' field if present (it's redundant in new format)
        config_to_save = {k: v for k, v in config.items() if k != 'service'}

        # Wrap in service name key
        wrapped_config = {service: config_to_save}

        with open(service_file, 'w') as f:
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
    except:
        return 'db-per-tenant'
