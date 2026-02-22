"""PostgreSQL type autocompletion functions."""

from typing import Any, cast

from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.yaml_loader import load_yaml_file

_FALLBACK_PG_TYPES = (
    "bigint",
    "boolean",
    "bytea",
    "char",
    "character",
    "character varying",
    "cidr",
    "date",
    "double precision",
    "inet",
    "integer",
    "json",
    "jsonb",
    "macaddr",
    "numeric",
    "real",
    "smallint",
    "text",
    "time",
    "timestamp",
    "timestamptz",
    "uuid",
    "varchar",
)


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


def _load_types_from_mapping_file() -> list[str]:
    """Load PG types from services/_schemas/_definitions/map-mssql-pgsql.yaml."""
    mapping_file = (
        get_project_root()
        / "services"
        / "_schemas"
        / "_definitions"
        / "map-mssql-pgsql.yaml"
    )
    if not mapping_file.is_file():
        return []

    try:
        data = load_yaml_file(mapping_file)
        if not isinstance(data, dict):
            return []

        data_dict = cast(dict[str, Any], data)
        pg_types: set[str] = set()

        pgsql_to_mssql_raw = data_dict.get("pgsql_to_mssql")
        if isinstance(pgsql_to_mssql_raw, dict):
            pgsql_to_mssql = cast(dict[str, Any], pgsql_to_mssql_raw)
            pg_types.update(
                key.strip()
                for key in pgsql_to_mssql
                if isinstance(key, str) and key.strip()
            )

        mssql_to_pgsql_raw = data_dict.get("mssql_to_pgsql")
        if isinstance(mssql_to_pgsql_raw, dict):
            mssql_to_pgsql = cast(dict[str, Any], mssql_to_pgsql_raw)
            pg_types.update(
                value.strip()
                for value in mssql_to_pgsql.values()
                if isinstance(value, str) and value.strip()
            )

        return sorted(pg_types)
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
    from_definitions = _load_types_from_definitions()
    if from_definitions:
        return from_definitions

    from_mapping = _load_types_from_mapping_file()
    if from_mapping:
        return from_mapping

    return sorted(_FALLBACK_PG_TYPES)

