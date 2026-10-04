"""Offline, relocatable input receipts and regenerable emission history."""

from __future__ import annotations

import base64
import binascii
import re
from importlib.metadata import version
from pathlib import Path

from cdc_generator.core.rbac.rendering import render_migration, select_permissions
from cdc_generator.core.rbac.validation import Contract, Json, canonical, compile_contract, digest, doctor, mapping, parse_json, sequence, string

COMPILER = "cdc-rbac-flat-v2"
LOCK_FORMAT = 3
PACKAGE = Path(__file__).resolve().parents[2]
IMPLEMENTATION_FILES = (
    "core/rbac/artifacts.py",
    "core/rbac/provenance.py",
    "core/rbac/rendering.py",
    "core/rbac/validation.py",
    "helpers/yaml_loader.py",
    "core/migration_generator/file_writers.py",
)
RUNTIME = ("jsonschema", "jsonschema-specifications", "referencing", "rpds-py", "attrs", "ruamel.yaml")
MIGRATION = r"migrations/default/[0-9]{13}_rbac_editor_qnrs"


def implementation() -> dict[str, Json]:
    """Fingerprint packaged compiler code and dependencies, never a checkout path."""
    return {
        "files": {name: digest((PACKAGE / name).read_bytes()) for name in IMPLEMENTATION_FILES},
        "runtime": {name: version(name) for name in RUNTIME},
    }


def snapshot(source: bytes, catalog: bytes) -> tuple[Contract, dict[str, Json]]:
    """Bind both exact owner-authored bytes and their canonical JSON meaning."""
    values = {"source": parse_json(source), "catalog": parse_json(catalog)}
    receipts: dict[str, Json] = {
        name: {
            "base64": base64.b64encode(raw).decode("ascii"),
            "sha256": digest(raw),
            "canonical_sha256": digest(canonical(values[name])),
        }
        for name, raw in [("source", source), ("catalog", catalog)]
    }
    return compile_contract(values["source"], values["catalog"]), receipts


def restore(value: Json) -> tuple[Contract, dict[str, Json]]:
    """Rehash and revalidate stored bytes instead of trusting a claimed input hash."""
    inputs = mapping(value)
    try:
        source = base64.b64decode(string(mapping(inputs.get("source"))["base64"]), validate=True)
        catalog = base64.b64decode(string(mapping(inputs.get("catalog"))["base64"]), validate=True)
    except (KeyError, binascii.Error) as error:
        raise ValueError("Malformed input byte receipt") from error
    contract, expected = snapshot(source, catalog)
    if canonical(inputs) != canonical(expected):
        raise ValueError("Input byte/canonical hash provenance mismatch")
    return contract, expected


def contract_hash(contract: Contract | None) -> str | None:
    """Match the semantic fingerprint embedded in generated SQL headers."""
    return digest(canonical({"source": contract.source, "catalog": contract.catalog})) if contract else None


def emission(migration: str, current: Contract, inputs: dict[str, Json], previous: Contract | None) -> dict[str, Json]:
    """Record the reproducible outputs and predecessor for one immutable emission."""
    up, down = render_migration(current, previous)
    return {
        "migration": migration,
        "inputs": inputs,
        "contract_sha256": contract_hash(current),
        "previous_contract_sha256": contract_hash(previous),
        "files": {f"{migration}/{filename}": digest(data) for filename, data in [("up.sql", up), ("down.sql", down)]},
        "select": {
            "path": f"metadata/databases/default/tables/{current.schema}_{current.table}.yaml",
            "sha256": digest(canonical(select_permissions(current))),
        },
    }


def seal(state: dict[str, Json]) -> dict[str, Json]:
    """Hash the canonical payload; publication's reviewed lock hash anchors identity."""
    payload = {key: value for key, value in state.items() if key != "sha256"}
    return {**payload, "sha256": digest(canonical(payload))}


def verify_history(state: dict[str, Json]) -> tuple[Contract, Contract | None]:
    """Regenerate every migration and require complete, ordered input/output history."""
    if state.get("lock_format") != LOCK_FORMAT:
        raise ValueError("Require lock format 3 with original input receipts for every emission; v2 history cannot be inferred")
    if state.get("compiler") != COMPILER or state.get("pins") != doctor() or state.get("implementation") != implementation():
        raise ValueError("Compiler/validator/runtime provenance drift")
    if state != seal(state):
        raise ValueError("Canonical lock payload hash mismatch")
    history = sequence(state.get("history"))
    if not history:
        raise ValueError("Missing immutable emission history")
    previous = None
    current = None
    last_migration = ""
    files: dict[str, Json] = {}
    for value in history:
        entry = mapping(value)
        migration = string(entry.get("migration"))
        if not re.fullmatch(MIGRATION, migration) or migration <= last_migration:
            raise ValueError("Emission history must have strictly ordered canonical migration identities")
        current, inputs = restore(entry.get("inputs"))
        expected = emission(migration, current, inputs, previous)
        if canonical(entry) != canonical(expected):
            raise ValueError(f"Regenerated emission provenance mismatch: {migration}")
        files.update(mapping(expected["files"]))
        last_migration = migration
        previous = current
    # The latest convenience fields are assertions, never independent authority.
    latest = mapping(history[-1])
    current, inputs = restore(latest["inputs"])
    previous = restore(mapping(history[-2])["inputs"])[0] if len(history) > 1 else None
    expected_fields: dict[str, Json] = {
        "source": current.source,
        "catalog": current.catalog,
        "input_hashes": {name: mapping(value)["sha256"] for name, value in inputs.items()},
        "previous": {"source": previous.source, "catalog": previous.catalog} if previous else None,
        "migration": last_migration,
        "files": files,
        "select": latest["select"],
    }
    if any(canonical(state.get(key)) != canonical(value) for key, value in expected_fields.items()):
        raise ValueError("Latest input/output fields disagree with emission history")
    allowed = {*expected_fields, "compiler", "lock_format", "pins", "implementation", "history", "preserved", "sha256"}
    if set(state) != allowed:
        raise ValueError("Unexpected or missing lock provenance fields")
    return current, previous
