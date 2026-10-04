"""Canonical Hasura emission, immutable history and reproducible drift checks."""

from __future__ import annotations

import copy
import io
import re
from pathlib import Path
from typing import Protocol, cast

from cdc_generator.core.rbac.provenance import COMPILER, LOCK_FORMAT, MIGRATION, emission, implementation, seal, snapshot, verify_history
from cdc_generator.core.rbac.rendering import render_migration, select_permissions
from cdc_generator.core.rbac.validation import Contract, Json, canonical, compile_contract, digest, doctor, load_json, mapping
from cdc_generator.helpers.yaml_loader import ConfigDict, YAMLLoader, create_yaml_loader, load_yaml_file, yaml

HASURA_CLI_VERSION = 3
LOCK = Path("rbac/.rbac-lock.json")
TABLES = Path("metadata/databases/default/tables")
G4 = TABLES / "public_queries.yaml"
G4_IDENTITY: dict[str, Json] = {str(G4): {"table": {"schema": "public", "name": "queries"}}}


class _HasuraYaml(YAMLLoader, Protocol):
    """The existing ruamel loader's indentation interface for CLI exports."""

    def indent(self, *, mapping: int, sequence: int, offset: int) -> None:
        """Configure Hasura CLI's two-space mapping and indented sequences."""
        ...


class _KeyLocations(Protocol):
    """Locations retained by the existing round-trip YAML loader."""

    def key(self, name: str) -> tuple[int, int]:
        """Return a mapping key's zero-based line and column."""
        ...


class _LocatedMapping(Protocol):
    """The ruamel mapping's source locations at the loader boundary."""

    lc: _KeyLocations


def _select_block(contract: Contract) -> str:
    """Serialize only the owned block in Hasura CLI export indentation."""
    loader = cast(_HasuraYaml, create_yaml_loader())
    loader.indent(mapping=2, sequence=4, offset=2)
    stream = io.StringIO()
    loader.dump({"select_permissions": select_permissions(contract)}, stream)
    return stream.getvalue()


def _replace_select(original: str, metadata: ConfigDict, block: str) -> str:
    """Use parsed key locations to preserve every byte outside the SELECT block."""
    if "select_permissions" not in metadata:
        return original + ("" if original.endswith("\n") else "\n") + block
    locations = cast(_LocatedMapping, metadata).lc
    start, column = locations.key("select_permissions")
    lines = original.splitlines(keepends=True)
    if column != 0 or not lines[start].startswith("select_permissions:"):
        raise ValueError("SELECT merge requires a block-style Hasura CLI table export")
    following = [locations.key(key)[0] for key in metadata if locations.key(key)[0] > start]
    end = min(following, default=len(lines))
    while end > start + 1 and (not lines[end - 1].strip() or lines[end - 1].startswith("#")):
        end -= 1
    return "".join(lines[:start]) + block + "".join(lines[end:])


def _metadata_select(metadata: ConfigDict) -> Json:
    """Normalize the false aggregation default omitted by Hasura CLI exports."""
    permissions = copy.deepcopy(cast(Json, metadata.get("select_permissions")))
    if isinstance(permissions, list):
        for entry in permissions:
            if isinstance(entry, dict):
                permission = entry.get("permission")
                if isinstance(permission, dict) and permission.get("allow_aggregations") is False:
                    del permission["allow_aggregations"]
    return permissions


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
    if not path.exists():
        return None
    state = mapping(load_json(path))
    if path.read_bytes() != canonical(state):
        raise ValueError("Compiler-owned lock is not canonical JSON")
    return state


def _contract(state: dict[str, Json]) -> Contract:
    """Revalidate stored compiler inputs instead of trusting compiled predicates."""
    return compile_contract(state["source"], state["catalog"])


def _merge(root: Path, current: Contract, previous: Contract | None) -> bytes:
    """Change only the owned SELECT block; preserve relationships and other keys."""
    relative = _table_path(current)
    path = _path(root, relative)
    metadata = load_yaml_file(path)
    identity = metadata.get("table")
    if identity != {"schema": current.schema, "name": current.table}:
        raise ValueError(f"Metadata identity mismatch: {relative}")
    expected = select_permissions(previous) if previous else None
    existing = _metadata_select(metadata)
    if existing != expected:
        raise ValueError("Unowned or drifted SELECT permissions; structured merge refused")
    if any(metadata.get(f"{verb}_permissions") for verb in ("insert", "update", "delete")):
        raise ValueError("Owner-exclusive editor metadata contains mutation permissions")
    merged = _replace_select(path.read_bytes().decode(), metadata, _select_block(current))
    if yaml.load(io.StringIO(merged)) != {**metadata, "select_permissions": select_permissions(current)}:
        raise ValueError("SELECT merge would change separately owned metadata")
    return merged.encode()


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
    current, _previous = verify_history(state)
    if set(mapping(state["files"])) != _owned_files(root):
        raise ValueError("Compiler-owned migration inventory differs from the lock")
    for relative, expected in mapping(state["files"]).items():
        path = _path(root, Path(relative))
        if digest(path.read_bytes()) != expected:
            raise ValueError(f"Artifact drift: {relative}")
    if state["preserved"] != G4_IDENTITY:
        raise ValueError("Separately owned G4 identity provenance mismatch")
    _table_index(root, current)
    metadata = load_yaml_file(_path(root, _table_path(current)))
    if metadata.get("table") != {"schema": current.schema, "name": current.table} or _metadata_select(metadata) != select_permissions(current):
        raise ValueError("Generated SELECT/provenance mismatch")
    if any(metadata.get(f"{verb}_permissions") for verb in ("insert", "update", "delete")):
        raise ValueError("Generated owner-exclusive Hasura mutations are prohibited")


def _owned_files(root: Path) -> set[str]:
    """Detect deleted locks/history and unrecorded files in the reserved namespace."""
    directory = _path(root, Path("migrations/default"))
    result: set[str] = set()
    if directory.exists():
        for path in directory.iterdir():
            if path.name.endswith("_rbac_editor_qnrs"):
                relative = path.relative_to(root)
                if not re.fullmatch(MIGRATION, str(relative)) or not _path(root, relative).is_dir():
                    raise ValueError("Noncanonical compiler-owned migration identity")
                if {file.name for file in path.iterdir()} != {"up.sql", "down.sql"}:
                    raise ValueError("Incomplete or extra compiler-owned migration files")
                result.update(str(_path(root, file.relative_to(root)).relative_to(root)) for file in path.iterdir())
    return result


def _table_index(root: Path, current: Contract) -> None:
    """Require exactly one active canonical include on both emission and check."""
    with _path(root, TABLES / "tables.yaml").open() as stream:
        index = yaml.load(stream)
    if not isinstance(index, list) or index.count(f"!include {_table_path(current).name}") != 1:
        raise ValueError("Canonical table metadata is absent from Hasura tables.yaml")


def check(root: Path, source_path: Path, catalog_path: Path) -> dict[str, Json]:
    """Compare owned inputs/outputs structurally and verify G4 identity only."""
    _project(root)
    state = _load_lock(root)
    if state is None:
        raise ValueError("Missing RBAC lock; emit a reviewed migration first")
    verify_state(root, state)
    source, catalog = source_path.read_bytes(), catalog_path.read_bytes()
    hashes: dict[str, Json] = {"source": digest(source), "catalog": digest(catalog)}
    if state["input_hashes"] != hashes:
        raise ValueError("Source/catalog drift: emit a new immutable migration")
    return state


def _write_set(root: Path, outputs: dict[Path, bytes]) -> None:
    """Restore the owned write set on a filesystem failure; publish lock last."""
    if any(_path(root, relative).resolve() == _path(root, G4).resolve() for relative in outputs):
        raise ValueError("Separately owned G4 must never be in the compiler write set")
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
    current, inputs = snapshot(source_path.read_bytes(), catalog_path.read_bytes())
    if (current.schema, current.table) != ("editor", "qnrs"):
        raise ValueError("Canonical emission is bounded to editor.qnrs")
    state = _load_lock(root)
    previous = None
    if state:
        verify_state(root, state)
        previous = _contract(state)
        if canonical({"source": current.source, "catalog": current.catalog}) == canonical({"source": previous.source, "catalog": previous.catalog}):
            return check(root, source_path, catalog_path)
    elif _owned_files(root):
        raise ValueError("Missing RBAC lock for existing compiler-owned migrations; deleting provenance is prohibited")
    if not re.fullmatch(r"[0-9]{13}", migration_version):
        raise ValueError("Migration version must be an explicit 13-digit Unix millisecond value")
    directory = Path("migrations/default")
    versions = [path.name.split("_", 1)[0] for path in _path(root, directory).iterdir() if path.is_dir()] if _path(root, directory).exists() else []
    if any(value.isdigit() and int(value) >= int(migration_version) for value in versions):
        raise ValueError("Migration version must sort after every existing migration")
    migration = directory / f"{migration_version}_rbac_{current.schema}_{current.table}"
    metadata_path = _table_path(current)
    _table_index(root, current)
    merged = _merge(root, current, previous)
    up, down = render_migration(current, previous)
    outputs = {migration / "up.sql": up, migration / "down.sql": down, metadata_path: merged}
    files = dict(mapping(state["files"])) if state else {}
    files.update({str(migration / filename): digest(content) for filename, content in [("up.sql", up), ("down.sql", down)]})
    history = list(cast(list[Json], state["history"])) if state else []
    history.append(emission(str(migration), current, inputs, previous))
    manifest = seal(
        {
            "compiler": COMPILER,
            "lock_format": LOCK_FORMAT,
            "implementation": implementation(),
            "pins": cast(dict[str, Json], doctor()),
            "source": current.source,
            "catalog": current.catalog,
            "input_hashes": {name: mapping(value)["sha256"] for name, value in inputs.items()},
            "history": history,
            "previous": {"source": previous.source, "catalog": previous.catalog} if previous else None,
            "migration": str(migration),
            "files": files,
            "select": {"path": str(metadata_path), "sha256": digest(canonical(select_permissions(current)))},
            "preserved": copy.deepcopy(G4_IDENTITY),
        }
    )
    outputs[LOCK] = canonical(manifest)
    _write_set(root, outputs)
    return manifest
