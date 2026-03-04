"""Type compatibility helpers for sink operations."""

from cdc_generator.helpers.service_config import get_project_root

from .sink_operations_type_compatibility import (
    _AUTO_ENGINE,
    _can_use_pgsql_native_fallback,
    _check_with_type_map,
    _pgsql_native_fallback_map,
    _resolve_effective_source_type,
    _resolve_engine_pair,
)


def check_type_compatibility_impl(
    source_type: str,
    sink_type: str,
    source_engine: str = _AUTO_ENGINE,
    sink_engine: str = _AUTO_ENGINE,
    source_table: str | None = None,
    source_column: str | None = None,
) -> bool:
    """Check if source_type is compatible with sink_type.

    Uses runtime YAML type map(s) from ``services/_schemas/_definitions/``.
    """
    project_root = get_project_root()
    try:
        spec = _resolve_engine_pair(
            str(project_root),
            source_type,
            sink_type,
            source_engine,
            sink_engine,
        )
    except ValueError as exc:
        if (
            source_engine == _AUTO_ENGINE
            and sink_engine == _AUTO_ENGINE
            and "No type compatibility maps found" in str(exc)
            and _can_use_pgsql_native_fallback(source_type, sink_type)
        ):
            spec = _pgsql_native_fallback_map()
        else:
            raise

    effective_source_type = _resolve_effective_source_type(
        str(project_root),
        source_type,
        source_table,
        source_column,
    )

    return _check_with_type_map(spec, effective_source_type, sink_type)
