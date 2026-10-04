"""Canonical Hasura emission, immutable history and reproducible drift checks."""

from __future__ import annotations

import io
import re
from pathlib import Path
from typing import cast

from cdc_generator.core.rbac.rendering import render_migration, select_permissions
from cdc_generator.core.rbac.validation import Contract, Json, canonical, compile_contract, digest, doctor, load_json, mapping, string
from cdc_generator.helpers.yaml_loader import load_yaml_file, yaml

HASURA_CLI_VERSION = 3
LOCK = Path("rbac/.rbac-lock.json")
TABLES = Path("metadata/databases/default/tables")
G4 = TABLES / "public_queries.yaml"


def _path(root: Path, relative: Path) -> Path:
    """Reject symlink destinations and any path escaping the owning artifact."""
    path = root / relative
    local_paths = [path]
    parent = path.parent
    while parent not in (root, parent.parent):
        local_paths.append(parent)
        parent = parent.parent
    if path.resolve().is_relative_to(root.resolve()) and not any(parent.is_symlink() for parent in local_paths):
        return path
    raise ValueError(f"Artifact path escapes owner or is a symlink: {relative}")


def _table_path(contract: Contract) -> Path:
    """Use the owning Hasura CLI per-table metadata convention."""
    return TABLES / f"{contract.schema}_{contract.table}.yaml"


def _load_lock(root: Path) -> dict[str, Json] | None:
    """Read provenance only from the owning artifact's RBAC working directory."""
    path = _path(root, LOCK)
    return mapping(load_json(path)) if path.exists() else None


def _contract(state: dict[str, Json]) -> Contract:
    """Revalidate stored compiler inputs instead of trusting compiled predicates."""
    return compile_contract(state["source"], state["catalog"])


def _merge(root: Path, current: Contract, previous: Contract | None) -> bytes:
    """Change only the owned SELECT block; preserve relationships and other keys."""
    relative = _table_path(current)
    metadata = load_yaml_file(_path(root, relative))
    identity = metadata.get("table")
    if identity != {"schema": current.schema, "name": current.table}:
        raise ValueError(f"Metadata identity mismatch: {relative}")
    expected = select_permissions(previous) if previous else None
    existing = metadata.get("select_permissions")
    if existing != expected:
        raise ValueError("Unowned or drifted SELECT permissions; structured merge refused")
    if any(metadata.get(f"{verb}_permissions") for verb in ("insert", "update", "delete")):
        raise ValueError("Owner-exclusive editor metadata contains mutation permissions")
    metadata["select_permissions"] = select_permissions(current)
    stream = io.StringIO()
    yaml.dump(metadata, stream)
    return stream.getvalue().encode()


def _project(root: Path) -> None:
    """Require the supported canonical Hasura CLI v3 project layout."""
    config = load_yaml_file(_path(root, Path("config.yaml")))
    if (
        config.get("version") != HASURA_CLI_VERSION
        or config.get("metadata_directory", "metadata") != "metadata"
        or config.get("migrations_directory", "migrations") != "migrations"
    ):
        raise ValueError("Require canonical Hasura CLI version 3 project layout")
    g4 = load_yaml_file(_path(root, G4))
    if g4.get("table") != {"schema": "public", "name": "queries"}:
        raise ValueError("G4 metadata identity must be public.queries")


def verify_state(root: Path, state: dict[str, Json]) -> None:
    """Verify hashes and recompute SQL/SELECT projection, without writing files."""
    if state.get("compiler") != "cdc-rbac-flat-v1" or state.get("pins") != doctor():
        raise ValueError("Compiler/validator provenance drift")
    for relative, expected in mapping(state["files"]).items():
        path = _path(root, Path(relative))
        if digest(path.read_bytes()) != expected:
            raise ValueError(f"Artifact drift: {relative}")
    for relative, expected in mapping(state["preserved"]).items():
        if digest(_path(root, Path(relative)).read_bytes()) != expected:
            raise ValueError(f"Separately owned metadata drift: {relative}")
    current = _contract(state)
    prior_value = state["previous"]
    previous = _contract(mapping(prior_value)) if prior_value is not None else None
    up, down = render_migration(current, previous)
    migration = Path(string(state["migration"]))
    for filename, content in [("up.sql", up), ("down.sql", down)]:
        if _path(root, migration / filename).read_bytes() != content:
            raise ValueError(f"Generated SQL/provenance mismatch: {filename}")
    metadata = load_yaml_file(_path(root, _table_path(current)))
    if metadata.get("table") != {"schema": current.schema, "name": current.table} or metadata.get("select_permissions") != select_permissions(
        current
    ):
        raise ValueError("Generated SELECT/provenance mismatch")
    if any(metadata.get(f"{verb}_permissions") for verb in ("insert", "update", "delete")):
        raise ValueError("Generated owner-exclusive Hasura mutations are prohibited")


def check(root: Path, source_path: Path, catalog_path: Path) -> dict[str, Json]:
    """Regenerate and compare inputs, outputs and preserved G4 identity/bytes."""
    _project(root)
    state = _load_lock(root)
    if state is None:
        raise ValueError("Missing RBAC lock; emit a reviewed migration first")
    verify_state(root, state)
    hashes: dict[str, Json] = {"source": digest(source_path.read_bytes()), "catalog": digest(catalog_path.read_bytes())}
    if state["input_hashes"] != hashes:
        raise ValueError("Source/catalog drift: emit a new immutable migration")
    current = compile_contract(load_json(source_path), load_json(catalog_path))
    if canonical(current.source) != canonical(state["source"]) or canonical(current.catalog) != canonical(state["catalog"]):
        raise ValueError("Input/provenance mismatch")
    return state


def _write_set(root: Path, outputs: dict[Path, bytes]) -> None:
    """Restore the owned write set on a filesystem failure; publish lock last."""
    originals: dict[Path, bytes | None] = {}
    try:
        for relative, content in outputs.items():
            path = _path(root, relative)
            originals[path] = path.read_bytes() if path.exists() else None
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
    except OSError:
        for path, content in originals.items():
            if content is None:
                path.unlink(missing_ok=True)
            else:
                path.write_bytes(content)
        raise


def emit(root: Path, source_path: Path, catalog_path: Path, migration_version: str) -> dict[str, Json]:
    """Emit an explicit-version immutable migration and structured SELECT merge."""
    _project(root)
    current = compile_contract(load_json(source_path), load_json(catalog_path))
    state = _load_lock(root)
    previous = None
    if state:
        verify_state(root, state)
        previous = _contract(state)
        if canonical({"source": current.source, "catalog": current.catalog}) == canonical({"source": previous.source, "catalog": previous.catalog}):
            return check(root, source_path, catalog_path)
    if not re.fullmatch(r"[0-9]{13}", migration_version):
        raise ValueError("Migration version must be an explicit 13-digit Unix millisecond value")
    directory = Path("migrations/default")
    versions = [path.name.split("_", 1)[0] for path in _path(root, directory).iterdir() if path.is_dir()] if _path(root, directory).exists() else []
    if any(value.isdigit() and int(value) >= int(migration_version) for value in versions):
        raise ValueError("Migration version must sort after every existing migration")
    migration = directory / f"{migration_version}_rbac_{current.schema}_{current.table}"
    metadata_path = _table_path(current)
    with _path(root, TABLES / "tables.yaml").open() as stream:
        index = yaml.load(stream)
    if not isinstance(index, list) or index.count(f"!include {metadata_path.name}") != 1:
        raise ValueError("Canonical table metadata is absent from Hasura tables.yaml")
    merged = _merge(root, current, previous)
    up, down = render_migration(current, previous)
    outputs = {migration / "up.sql": up, migration / "down.sql": down, metadata_path: merged}
    files = dict(mapping(state["files"])) if state else {}
    files.update({str(path): digest(content) for path, content in outputs.items()})
    manifest: dict[str, Json] = {
        "compiler": "cdc-rbac-flat-v1",
        "pins": cast(dict[str, Json], doctor()),
        "source": current.source,
        "catalog": current.catalog,
        "input_hashes": {"source": digest(source_path.read_bytes()), "catalog": digest(catalog_path.read_bytes())},
        "previous": {"source": previous.source, "catalog": previous.catalog} if previous else None,
        "migration": str(migration),
        "files": files,
        "preserved": {str(G4): digest(_path(root, G4).read_bytes())},
    }
    outputs[LOCK] = canonical(manifest)
    _write_set(root, outputs)
    return manifest
