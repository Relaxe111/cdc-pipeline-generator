"""Type stubs for psycopg2 connection interface.

Provides only the interface we actually use in this project.
Used during type checking (TYPE_CHECKING) to avoid importing
the optional psycopg2 dependency.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol


class PgCursorFactory(Protocol):
    """Protocol for psycopg2 cursor factories (e.g. RealDictCursor)."""

    ...


class PgCursor(Protocol):
    """Type stub for psycopg2 cursor interface."""

    def execute(
        self,
        query: str,
        parameters: Sequence[object] | None = None,
    ) -> None:
        """Execute a SQL query."""
        ...

    def fetchall(self) -> list[Any]:
        """Fetch all remaining rows."""
        ...

    def fetchone(self) -> tuple[Any, ...] | dict[str, Any] | None:
        """Fetch next row."""
        ...

    def close(self) -> None:
        """Close the cursor."""
        ...

    def __iter__(self) -> PgCursor:
        """Iterate over rows."""
        ...

    def __next__(self) -> tuple[Any, ...] | dict[str, Any]:
        """Fetch next row during iteration."""
        ...


class PgConnection(Protocol):
    """Type stub for psycopg2 connection interface."""

    def cursor(
        self,
        *,
        cursor_factory: type[object] | None = None,
    ) -> PgCursor:
        """Create a cursor."""
        ...

    def close(self) -> None:
        """Close the connection."""
        ...

    def commit(self) -> None:
        """Commit the current transaction."""
        ...

    def rollback(self) -> None:
        """Roll back the current transaction."""
        ...


class PgExtras(Protocol):
    """Type stub for psycopg2.extras module."""

    RealDictCursor: type[object]


class Psycopg2Module(Protocol):
    """Type stub for the psycopg2 module interface."""

    extras: PgExtras

    def connect(
        self,
        *,
        host: str,
        port: int | str,
        dbname: str | None = None,
        database: str | None = None,
        user: str,
        password: str,
        connect_timeout: int | None = None,
    ) -> PgConnection:
        """Create a new PostgreSQL connection."""
        ...

    class OperationalError(Exception):
        """Database operational error."""

        ...
