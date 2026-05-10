# pyright: reportMissingImports=false
"""Type-safe pymssql loader with optional dependency handling.

Provides a single import point for pymssql with proper type hints.
All modules should import from here instead of importing pymssql directly.

Usage:
    from cdc_generator.helpers.mssql_loader import mssql, has_pymssql, MSSQLConnection

    if not has_pymssql:
        print_error("pymssql not installed")
        return None

    conn = mssql.connect(server=host, port=port, ...)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from cdc_generator.helpers.pymssql_stub import (
        MSSQLConnection,
        MSSQLCursor,
        MSSQLModule,
    )

# ---------------------------------------------------------------------------
# Runtime import — pymssql is optional
# ---------------------------------------------------------------------------

_mssql_module: MSSQLModule | None = None
has_pymssql: bool = False

try:
    import pymssql as _pymssql_raw

    _mssql_module = cast("MSSQLModule", _pymssql_raw)
    has_pymssql = True
except ImportError:
    pass


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

_INSTALL_HINT = "pymssql is not installed. Install it with: pip install pymssql\nNote: pymssql requires FreeTDS. On macOS: brew install freetds"


class MSSQLNotAvailableError(Exception):
    """Raised when pymssql is not installed but MSSQL operations are requested."""


def ensure_pymssql() -> MSSQLModule:
    """Return the pymssql module or raise with a helpful message.

    Returns:
        The validated pymssql module, properly typed.

    Raises:
        MSSQLNotAvailableError: If pymssql is not installed.
    """
    if _mssql_module is None:
        raise MSSQLNotAvailableError(_INSTALL_HINT)
    return _mssql_module


def create_mssql_connection(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    connect_timeout: int = 10,
    query_timeout: int = 30,
) -> MSSQLConnection:
    """Create a pymssql connection with proper typing.

    Args:
        host: Server hostname.
        port: Server port.
        database: Database name.
        user: Username.
        password: Password.
        connect_timeout: Login timeout in seconds.
        query_timeout: Query timeout in seconds.

    Returns:
        A typed pymssql connection object.

    Raises:
        MSSQLNotAvailableError: If pymssql is not installed.
    """
    mod = ensure_pymssql()
    initial_database = database.strip() or "master"
    return mod.connect(
        server=host,
        port=port,
        database=initial_database,
        user=user,
        password=password,
        login_timeout=connect_timeout,
        timeout=query_timeout,
    )


# Re-export types for convenience
__all__ = [
    "MSSQLConnection",
    "MSSQLCursor",
    "MSSQLModule",
    "MSSQLNotAvailableError",
    "create_mssql_connection",
    "ensure_pymssql",
    "has_pymssql",
]
