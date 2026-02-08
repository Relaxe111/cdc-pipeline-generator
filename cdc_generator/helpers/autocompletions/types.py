"""PostgreSQL type autocompletion functions."""

from typing import Any, cast

from cdc_generator.helpers.autocompletions.utils import find_directory_upward
from cdc_generator.helpers.yaml_loader import load_yaml_file


def list_pg_column_types() -> list[str]:
    """List PostgreSQL column types for --column autocompletion.

    Reads from service-schemas/types/pgsql.yaml if available,
    otherwise returns hardcoded common types.

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
    # Try to load from service-schemas directory
    schemas_dir = find_directory_upward('service-schemas')
    types_file = schemas_dir / "types" / "pgsql.yaml" if schemas_dir else None

    if types_file and types_file.is_file():
        try:
            data = load_yaml_file(types_file)
            if data:
                data_dict = cast(dict[str, Any], data)
                all_types: list[str] = []
                categories = data_dict.get('categories', {})
                if isinstance(categories, dict):
                    categories_dict = cast(dict[str, Any], categories)
                    for cat_data in categories_dict.values():
                        if isinstance(cat_data, dict):
                            cat_dict = cast(dict[str, Any], cat_data)
                            types = cat_dict.get('types', [])
                            if isinstance(types, list):
                                types_list = cast(list[Any], types)
                                all_types.extend(
                                    str(t) for t in types_list if isinstance(t, str)
                                )
                if all_types:
                    return sorted(all_types)
        except Exception:
            pass

    # Fallback: common PostgreSQL types
    return [
        "bigint", "boolean", "bytea", "char", "citext", "date",
        "double precision", "integer", "interval", "json", "jsonb",
        "numeric", "real", "serial", "bigserial", "smallint",
        "text", "time", "timestamp", "timestamptz", "uuid", "varchar",
    ]

