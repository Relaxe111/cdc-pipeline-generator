"""Type stubs for pymssql connection interface.

Provides only the interface we actually use in this project.
Used during type checking (TYPE_CHECKING) to avoid importing
the optional pymssql dependency.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol


class MSSQLCursor(Protocol):
    """Type stub for pymssql cursor interface."""

    def execute(self, query: str, args: Sequence[object] | None = None) -> None:
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

    def __iter__(self) -> MSSQLCursor:
        """Iterate over rows."""
        ...

    def __next__(self) -> tuple[Any, ...] | dict[str, Any]:
        """Fetch next row during iteration."""
        ...


class MSSQLConnection(Protocol):
    """Type stub for pymssql connection interface."""

    def cursor(self, *, as_dict: bool = False) -> MSSQLCursor:
        """Create a cursor. Use as_dict=True for dict rows."""
        ...

    def close(self) -> None:
        """Close the connection."""
        ...

    def commit(self) -> None:
        """Commit the current transaction."""
        ...


class MSSQLModule(Protocol):
    """Type stub for the pymssql module interface."""

    def connect(
        self,
        *,
        server: str,
        port: int | str,
        database: str,
        user: str,
        password: str,
    ) -> MSSQLConnection:
        """Create a new MSSQL connection."""
        ...
