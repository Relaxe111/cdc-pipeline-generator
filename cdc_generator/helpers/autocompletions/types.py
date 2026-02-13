"""PostgreSQL type autocompletion functions."""

from typing import Any, cast

from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.yaml_loader import load_yaml_file


def _load_types_from_definitions() -> list[str]:
    """Load PG types from new schema definitions model only."""
    definitions_file = (
        get_project_root()
        / "services"
        / "_schemas"
        / "_definitions"
        / "pgsql.yaml"
    )
    if not definitions_file.is_file():
        return []

    try:
        data = load_yaml_file(definitions_file)
        if not isinstance(data, dict):
            return []

        data_dict = cast(dict[str, Any], data)
        categories = data_dict.get("categories")
        if not isinstance(categories, dict):
            return []

        all_types: list[str] = []
        categories_dict = cast(dict[str, Any], categories)
        for category_data in categories_dict.values():
            if not isinstance(category_data, dict):
                continue
            category_dict = cast(dict[str, Any], category_data)
            raw_types = category_dict.get("types", [])
            if not isinstance(raw_types, list):
                continue
            all_types.extend(
                str(type_name)
                for type_name in raw_types
                if isinstance(type_name, str)
            )

        return sorted(set(all_types))
    except Exception:
        return []


def list_pg_column_types() -> list[str]:
    """List PostgreSQL column types for --column autocompletion.

    Reads only from the new schema declaration file:
    - services/_schemas/_definitions/pgsql.yaml

    Returns:
        List of type names.

    Expected YAML structure (service-schemas/types/pgsql.yaml):
        categories:
          Numeric:
            types:
              - bigint
              - integer
          Text:
            types:
              - text
              - varchar

    Example:
        >>> list_pg_column_types()
        ['bigint', 'boolean', 'integer', 'text', 'timestamp', 'uuid']
    """
    return _load_types_from_definitions()

