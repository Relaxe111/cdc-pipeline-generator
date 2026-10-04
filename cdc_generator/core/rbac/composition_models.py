"""Immutable bounded composition values; no database or authorization side effects."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WriterRule:
    """Explicit owner declaration for one existing platform role and command."""

    role: str
    command: str
    columns: tuple[str, ...]
    using: tuple[tuple[str, str], ...] | None
    check: tuple[tuple[str, str], ...] | None


@dataclass(frozen=True)
class Policy:
    """An exact pg_policy readback; retained values are never in the write set."""

    name: str
    command: str
    permissive: bool
    using: str | None
    check: str | None


@dataclass(frozen=True)
class Composition:
    """Validated declaration, compatible retained definitions and catalog readback."""

    writers: tuple[WriterRule, ...]
    retained: tuple[Policy, ...]
    # Exact JSON text compared against a deterministic PostgreSQL readback.
    membership: str
    defaults: str
    acl: str
    owner: str
    schema_acl: str
