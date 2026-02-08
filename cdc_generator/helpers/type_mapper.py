"""Type mapper for converting column types between source and sink database engines.

Loads bidirectional mapping files from service-schemas/adapters/ and provides
column type conversion between database engines (e.g., MSSQL → PostgreSQL).

Mapping files follow the naming convention:
    {source}-to-{sink}.mapping.yaml

Each file contains:
    source_engine: mssql
    sink_engine: pgsql
    mappings:
        <source_type>: <target_type>
    fallback: text

Example:
    >>> mapper = TypeMapper("mssql", "pgsql")
    >>> mapper.map_type("uniqueidentifier")
    'uuid'
    >>> mapper.map_type("datetime2")
    'timestamp'
    >>> mapper.map_type("unknown_type")
    'text'

Bidirectional usage (reverse direction):
    >>> mapper = TypeMapper("pgsql", "mssql")
    >>> mapper.map_type("uuid")
    'uniqueidentifier'
"""

from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.yaml_loader import load_yaml_file

# Adapters directory within the package
_ADAPTERS_DIR = Path(__file__).parent.parent / "service-schemas" / "adapters"

# Expected number of parts when splitting adapter name by '-to-'
_ADAPTER_NAME_PARTS = 2


class TypeMapper:
    """Bidirectional type mapper between database engines.

    Loads a mapping file and resolves column types from source to sink engine.
    Supports reverse lookups by swapping the mapping direction.

    Attributes:
        source_engine: Source database engine (e.g., 'mssql', 'pgsql').
        sink_engine: Target database engine (e.g., 'pgsql', 'mssql').
        fallback: Default type when no mapping is found.
    """

    def __init__(self, source_engine: str, sink_engine: str) -> None:
        """Initialize type mapper by loading the appropriate mapping file.

        Args:
            source_engine: Source database engine identifier.
            sink_engine: Target database engine identifier.

        Raises:
            FileNotFoundError: If no mapping file exists for the engine pair.
        """
        self.source_engine = source_engine
        self.sink_engine = sink_engine
        self._mappings: dict[str, str] = {}
        self.fallback = "text"
        self._load_mappings()

    def _load_mappings(self) -> None:
        """Load mapping file for the source→sink engine pair.

        Tries direct file first ({source}-to-{sink}.mapping.yaml),
        then tries reverse file ({sink}-to-{source}.mapping.yaml)
        and inverts the mappings.

        Raises:
            FileNotFoundError: If no mapping file exists in either direction.
        """
        # Try direct mapping file
        direct_file = _ADAPTERS_DIR / f"{self.source_engine}-to-{self.sink_engine}.mapping.yaml"
        if direct_file.exists():
            self._load_from_file(direct_file, reverse=False)
            return

        # Try reverse mapping file (read it backwards)
        reverse_file = _ADAPTERS_DIR / f"{self.sink_engine}-to-{self.source_engine}.mapping.yaml"
        if reverse_file.exists():
            self._load_from_file(reverse_file, reverse=True)
            return

        msg = (
            f"No type mapping file found for {self.source_engine}→{self.sink_engine}. "
            + f"Expected: {direct_file.name} or {reverse_file.name} "
            + f"in {_ADAPTERS_DIR}"
        )
        raise FileNotFoundError(msg)

    def _load_from_file(self, file_path: Path, *, reverse: bool) -> None:
        """Load and parse a mapping YAML file.

        Args:
            file_path: Path to the mapping YAML file.
            reverse: If True, swap keys and values for reverse direction.
        """
        raw = load_yaml_file(file_path)
        data = cast(dict[str, Any], raw)

        raw_mappings = data.get("mappings", {})
        if not isinstance(raw_mappings, dict):
            msg = f"Invalid mappings format in {file_path}"
            raise ValueError(msg)

        mappings = cast(dict[str, str], raw_mappings)

        if reverse:
            # Invert: {target: source} → {source: target}
            # For duplicate values, first occurrence wins
            reversed_map: dict[str, str] = {}
            for source_type, target_type in mappings.items():
                if target_type not in reversed_map:
                    reversed_map[target_type] = source_type
            self._mappings = reversed_map
        else:
            self._mappings = dict(mappings)

        fallback = data.get("fallback")
        if isinstance(fallback, str):
            self.fallback = fallback

    def map_type(self, source_type: str) -> str:
        """Convert a source column type to the equivalent sink type.

        Performs case-insensitive lookup. Returns fallback type if no mapping found.

        Args:
            source_type: The source database column type (e.g., 'uniqueidentifier').

        Returns:
            The equivalent sink column type (e.g., 'uuid').

        Example:
            >>> mapper = TypeMapper("mssql", "pgsql")
            >>> mapper.map_type("uniqueidentifier")
            'uuid'
            >>> mapper.map_type("BIGINT")
            'bigint'
        """
        # Try exact match first
        if source_type in self._mappings:
            return self._mappings[source_type]

        # Try case-insensitive match
        lower_type = source_type.lower()
        for key, value in self._mappings.items():
            if key.lower() == lower_type:
                return value

        return self.fallback

    def map_columns(
        self,
        columns: list[dict[str, Any]],
    ) -> list[dict[str, str | bool]]:
        """Convert a list of source column definitions to sink column definitions.

        Each column dict must have 'name' and 'type' keys. Additional keys
        like 'nullable' and 'primary_key' are preserved as-is.

        Args:
            columns: List of source column dicts from service-schemas.

        Returns:
            List of column dicts with types converted to sink engine types.

        Example:
            >>> mapper = TypeMapper("mssql", "pgsql")
            >>> source_cols = [
            ...     {"name": "id", "type": "uniqueidentifier", "nullable": False},
            ...     {"name": "name", "type": "nvarchar", "nullable": True},
            ... ]
            >>> mapper.map_columns(source_cols)
            [
                {"name": "id", "type": "uuid", "nullable": False},
                {"name": "name", "type": "varchar", "nullable": True},
            ]
        """
        result: list[dict[str, str | bool]] = []
        for col in columns:
            name = col.get("name", "")
            source_type = col.get("type", "")
            if not isinstance(name, str) or not isinstance(source_type, str):
                continue

            mapped: dict[str, str | bool] = {
                "name": name,
                "type": self.map_type(source_type),
            }

            nullable = col.get("nullable")
            if isinstance(nullable, bool):
                mapped["nullable"] = nullable

            primary_key = col.get("primary_key")
            if isinstance(primary_key, bool):
                mapped["primary_key"] = primary_key

            result.append(mapped)

        return result

    @property
    def available_source_types(self) -> list[str]:
        """List all source types that have mappings defined.

        Returns:
            Sorted list of source type names.
        """
        return sorted(self._mappings.keys())

    @property
    def available_sink_types(self) -> list[str]:
        """List all unique sink types in the mappings.

        Returns:
            Sorted list of unique target type names.
        """
        return sorted(set(self._mappings.values()))


def get_available_adapters() -> list[str]:
    """List all available adapter mapping files.

    Returns:
        List of adapter names (e.g., ['mssql-to-pgsql', 'pgsql-to-pgsql']).

    Example:
        >>> get_available_adapters()
        ['mssql-to-pgsql', 'pgsql-to-pgsql']
    """
    if not _ADAPTERS_DIR.exists():
        return []

    adapters: list[str] = []
    for f in sorted(_ADAPTERS_DIR.glob("*.mapping.yaml")):
        # Strip .mapping.yaml suffix
        adapter_name = f.name.replace(".mapping.yaml", "")
        adapters.append(adapter_name)

    return adapters


def get_supported_engines() -> set[str]:
    """Get all engine identifiers found in adapter mapping files.

    Returns:
        Set of engine names (e.g., {'mssql', 'pgsql'}).

    Example:
        >>> get_supported_engines()
        {'mssql', 'pgsql'}
    """
    engines: set[str] = set()
    for adapter in get_available_adapters():
        parts = adapter.split("-to-")
        if len(parts) == _ADAPTER_NAME_PARTS:
            engines.add(parts[0])
            engines.add(parts[1])
    return engines
