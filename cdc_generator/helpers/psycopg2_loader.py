# pyright: reportMissingImports=false, reportMissingModuleSource=false
"""Type-safe psycopg2 loader with optional dependency handling.

Provides a single import point for psycopg2 with proper type hints.
All modules should import from here instead of importing psycopg2 directly.

Usage:
    from cdc_generator.helpers.psycopg2_loader import (
        psycopg2, has_psycopg2, PgConnection,
    )

    if not has_psycopg2:
        print_error("psycopg2 not installed")
        return None

    conn = psycopg2.connect(host=host, port=port, dbname=db, ...)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from cdc_generator.helpers.psycopg2_stub import (
        PgConnection,
        PgCursor,
        Psycopg2Module,
    )

# ---------------------------------------------------------------------------
# Runtime import — psycopg2 is optional
# ---------------------------------------------------------------------------

_pg_module: Psycopg2Module | None = None
has_psycopg2: bool = False

try:
    import psycopg2 as _psycopg2_raw
    import psycopg2.extras as _pg_extras  # ensure submodule loaded

    _pg_module = cast("Psycopg2Module", _psycopg2_raw)
    _ = _pg_extras  # prevent unused import warning
    has_psycopg2 = True
except ImportError:
    pass


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

_INSTALL_HINT = (
    "psycopg2 is not installed. Install it with: "
    "pip install psycopg2-binary"
)


class PostgresNotAvailableError(Exception):
    """Raised when psycopg2 is not installed but PostgreSQL operations are requested."""


def ensure_psycopg2() -> Psycopg2Module:
    """Return the psycopg2 module or raise with a helpful message.

    Returns:
        The validated psycopg2 module, properly typed.

    Raises:
        PostgresNotAvailableError: If psycopg2 is not installed.
    """
    if _pg_module is None:
        raise PostgresNotAvailableError(_INSTALL_HINT)
    return _pg_module


def create_postgres_connection(
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
    *,
    connect_timeout: int | None = None,
) -> PgConnection:
    """Create a psycopg2 connection with proper typing.

    Args:
        host: Server hostname.
        port: Server port.
        dbname: Database name.
        user: Username.
        password: Password.
        connect_timeout: Optional connection timeout in seconds.

    Returns:
        A typed psycopg2 connection object.

    Raises:
        PostgresNotAvailableError: If psycopg2 is not installed.
    """
    mod = ensure_psycopg2()
    if connect_timeout is not None:
        return mod.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=connect_timeout,
        )
    return mod.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )


# Re-export types for convenience
__all__ = [
    "PgConnection",
    "PgCursor",
    "PostgresNotAvailableError",
    "Psycopg2Module",
    "create_postgres_connection",
    "ensure_psycopg2",
    "has_psycopg2",
]
