"""Existing cdc CLI surface for the bounded offline RBAC compiler."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import click

from cdc_generator.core.rbac.artifacts import check, emit
from cdc_generator.core.rbac.rendering import render_migration, select_permissions
from cdc_generator.core.rbac.validation import compile_contract, doctor, generate_schema, load_json

_INPUT = click.Path(exists=True, dir_okay=False, path_type=str)
_HSR = click.Path(exists=True, file_okay=False, path_type=str)


def _run(action: Callable[[], object]) -> None:
    """Return a useful nonzero CLI failure before any dependent mutation."""
    try:
        click.echo(json.dumps(action(), indent=2, sort_keys=True))
    except (ValueError, OSError) as error:
        raise click.ClickException(str(error)) from error


@click.group(name="rbac")
def rbac_cmd() -> None:
    """Compile flat editor OpenDD permissions locally into canonical artifacts."""


@rbac_cmd.command(name="doctor")
def doctor_cmd() -> None:
    """Verify pinned local validator and official schemas, without networking."""
    _run(doctor)


@rbac_cmd.command(name="schema")
@click.option("--catalog", required=True, type=_INPUT)
def schema_cmd(catalog: str) -> None:
    """Print the official permission schema constrained to real catalog names."""
    _run(lambda: generate_schema(load_json(Path(catalog))))


@rbac_cmd.command(name="validate")
@click.option("--source", required=True, type=_INPUT)
@click.option("--catalog", required=True, type=_INPUT)
def validate_cmd(source: str, catalog: str) -> None:
    """Validate official shape and the admitted flat compiler semantics."""

    def action() -> object:
        contract = compile_contract(load_json(Path(source)), load_json(Path(catalog)))
        return {"table": f"{contract.schema}.{contract.table}", "roles": [rule.role for rule in contract.rules], "pins": doctor()}

    _run(action)


@rbac_cmd.command(name="generate")
@click.option("--source", required=True, type=_INPUT)
@click.option("--catalog", required=True, type=_INPUT)
def generate_cmd(source: str, catalog: str) -> None:
    """Print deterministic first-install SQL and SELECT metadata for review."""

    def action() -> object:
        contract = compile_contract(load_json(Path(source)), load_json(Path(catalog)))
        up, down = render_migration(contract, None)
        return {"up.sql": up.decode(), "down.sql": down.decode(), "select_permissions": select_permissions(contract), "pins": doctor()}

    _run(action)


@rbac_cmd.command(name="emit-migration")
@click.option("--source", required=True, type=_INPUT)
@click.option("--catalog", required=True, type=_INPUT)
@click.option("--hsr", required=True, type=_HSR)
@click.option("--migration-version", required=True)
def emit_cmd(source: str, catalog: str, hsr: str, migration_version: str) -> None:
    """Merge SELECT into the owner and emit immutable, explicitly ordered SQL."""
    _run(lambda: emit(Path(hsr), Path(source), Path(catalog), migration_version))


@rbac_cmd.command(name="check")
@click.option("--source", required=True, type=_INPUT)
@click.option("--catalog", required=True, type=_INPUT)
@click.option("--hsr", required=True, type=_HSR)
def check_cmd(source: str, catalog: str, hsr: str) -> None:
    """Detect source, SQL, owned SELECT and provenance drift; verify G4 identity."""
    _run(lambda: check(Path(hsr), Path(source), Path(catalog)))
