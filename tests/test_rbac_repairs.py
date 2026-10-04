"""Regression evidence for the independent review's bounded D1 and D2 repairs."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from cdc_generator.core.rbac import artifacts
from cdc_generator.core.rbac.artifacts import G4, LOCK, TABLES, check, emit
from cdc_generator.core.rbac.validation import canonical, load_json, mapping, sequence
from cdc_generator.helpers.yaml_loader import load_yaml_file, save_yaml_file

FIXTURES = Path(__file__).parent / "fixtures/rbac"
EDITOR = TABLES / "editor_qnrs.yaml"


@pytest.fixture()
def owner(tmp_path: Path) -> Path:
    """Create a disposable owning project without touching any artifact checkout."""
    (tmp_path / TABLES).mkdir(parents=True)
    (tmp_path / "config.yaml").write_text("version: 3\nmetadata_directory: metadata\n")
    (tmp_path / TABLES / "tables.yaml").write_text('- "!include editor_qnrs.yaml"\n- "!include public_queries.yaml"\n')
    for filename in ("editor_qnrs.yaml", "public_queries.yaml"):
        (tmp_path / TABLES / filename).write_bytes((FIXTURES / filename).read_bytes())
    (tmp_path / "source.json").write_bytes((FIXTURES / "editor.rbac.json").read_bytes())
    (tmp_path / "catalog.json").write_bytes((FIXTURES / "editor.catalog.json").read_bytes())
    return tmp_path


def _narrow_source(owner: Path) -> None:
    """Request an actual admitted therapist predicate change for upgrade tests."""
    source = sequence(load_json(owner / "source.json"))
    permissions = sequence(mapping(mapping(source[0])["definition"])["permissions"])
    mapping(permissions[2])["select"] = mapping(permissions[0])["select"]
    (owner / "source.json").write_bytes(canonical(source))


@pytest.mark.parametrize("edit", ["g4_write", "relationship", "format", "select_format", "select_default"])
def test_other_owners_can_evolve_metadata_then_check_and_upgrade(owner: Path, edit: str) -> None:
    """G4 owner edits, relationship changes and re-exports do not deadlock the lock."""
    state = emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    if edit == "g4_write":
        path = owner / G4
        metadata = load_yaml_file(path)
        metadata["update_permissions"] = [{"role": "legacy_owner", "permission": {"columns": ["id"], "filter": {}}}]
        save_yaml_file(metadata, path)
    elif edit == "relationship":
        path = owner / EDITOR
        path.write_text(path.read_text().replace("name: content_instances_qnr", "name: owner_renamed_relationship", 1))
    elif edit == "format":
        for path in [owner / EDITOR, owner / G4]:
            save_yaml_file(load_yaml_file(path), path)
    else:
        path = owner / EDITOR
        metadata = load_yaml_file(path)
        if edit == "select_default":
            value = sequence(json.loads(json.dumps(metadata["select_permissions"])))
            for entry in value:
                mapping(mapping(entry)["permission"])["allow_aggregations"] = False
            metadata["select_permissions"] = value
        prefix = path.read_text().split("select_permissions:\n", 1)[0]
        path.write_text(prefix + "select_permissions: " + json.dumps(metadata["select_permissions"]) + "\n")
    before = {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}
    assert check(owner, owner / "source.json", owner / "catalog.json") == state
    assert emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000") == state
    assert before == {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}
    g4 = (owner / G4).read_bytes()
    original_editor = (owner / EDITOR).read_bytes().split(b"select_permissions:", 1)[0]
    _narrow_source(owner)
    upgraded = emit(owner, owner / "source.json", owner / "catalog.json", "1800000000001")
    assert check(owner, owner / "source.json", owner / "catalog.json") == upgraded
    assert (owner / G4).read_bytes() == g4
    assert (owner / EDITOR).read_bytes().split(b"select_permissions:", 1)[0] == original_editor
    assert str(EDITOR) not in mapping(upgraded["files"])
    assert str(G4) not in mapping(upgraded["files"])
    assert upgraded["preserved"] == {str(G4): {"table": {"schema": "public", "name": "queries"}}}


@pytest.mark.parametrize("upgrade", [False, True])
def test_only_select_changes_in_hasura_cli_style(owner: Path, upgrade: bool) -> None:
    """Fresh append and middle-block upgrade preserve unowned bytes, comments and nulls."""
    if upgrade:
        emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
        _narrow_source(owner)
    path = owner / EDITOR
    prefix = path.read_bytes().split(b"select_permissions:\n", 1)[0]
    suffix = b"\n# Artifact owner comment\nconfiguration:\n  custom_name: owner_qnrs\n"
    path.write_bytes(path.read_bytes() + suffix)
    version = "1800000000001" if upgrade else "1800000000000"
    emit(owner, owner / "source.json", owner / "catalog.json", version)
    result = path.read_bytes()
    if upgrade:
        assert result.startswith(prefix)
        assert result.endswith(suffix)
    else:
        assert result.startswith(prefix + suffix)
    assert b"  - role: recipient\n    permission:\n      columns:\n        - customer_id\n" in result
    assert result.count(b"insertion_order: null") == prefix.count(b"insertion_order: null")


@pytest.mark.parametrize("failure", ["g4_identity", "missing_g4", "select", "aggregation", "mutation", "unowned_pin", "select_pin", "missing_lock"])
def test_upgrade_still_rejects_owned_drift_without_writing(owner: Path, failure: str) -> None:
    """Relaxed ownership pins never admit identity, SELECT, mutation or provenance drift."""
    state = emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    if failure == "g4_identity":
        path = owner / G4
        path.write_text(path.read_text().replace("name: queries", "name: wrong", 1))
    elif failure == "missing_g4":
        (owner / G4).unlink()
    elif failure in {"select", "mutation", "aggregation"}:
        path = owner / EDITOR
        metadata = load_yaml_file(path)
        if failure == "aggregation":
            value = sequence(json.loads(json.dumps(metadata["select_permissions"])))
            mapping(mapping(value[0])["permission"])["allow_aggregations"] = True
            metadata["select_permissions"] = value
        else:
            metadata["select_permissions" if failure == "select" else "update_permissions"] = [{"role": "attacker"}]
        save_yaml_file(metadata, path)
    elif failure == "unowned_pin":
        mapping(state["files"])[str(G4)] = "forbidden"
        (owner / LOCK).write_bytes(canonical(state))
    elif failure == "select_pin":
        mapping(state["select"])["sha256"] = "forged"
        (owner / LOCK).write_bytes(canonical(state))
    else:
        (owner / LOCK).unlink()
    _narrow_source(owner)
    before = {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}
    for action in [
        lambda: check(owner, owner / "source.json", owner / "catalog.json"),
        lambda: emit(owner, owner / "source.json", owner / "catalog.json", "1800000000001"),
    ]:
        with pytest.raises((ValueError, OSError)):
            action()
    assert before == {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}


@pytest.mark.parametrize("target", [G4, TABLES / "../tables/public_queries.yaml"])
def test_g4_write_set_is_rejected_before_any_write(owner: Path, target: Path) -> None:
    """Even an erroneous compiler write set cannot write G4 or partially publish SQL."""
    before = (owner / G4).read_bytes()
    with pytest.raises(ValueError, match="never"):
        artifacts._write_set(owner, {Path("first.sql"): b"bad", target: b"bad"})
    assert (owner / G4).read_bytes() == before
    assert not (owner / "first.sql").exists()


def test_non_block_select_merge_fails_without_writes(owner: Path) -> None:
    """A flow-style document needs a canonical CLI export before changing its block."""
    emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    path = owner / EDITOR
    path.write_text(json.dumps(load_yaml_file(path)))
    _narrow_source(owner)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="block-style"):
        emit(owner, owner / "source.json", owner / "catalog.json", "1800000000001")
    assert path.read_bytes() == before


def test_merge_guards_unowned_structure(owner: Path) -> None:
    """An invalid block serializer cannot add an unowned key during publication."""
    with (
        patch.object(artifacts, "_select_block", return_value="select_permissions: []\nconfiguration: {}\n"),
        pytest.raises(ValueError, match="separately owned"),
    ):
        emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")


def test_no_final_newline_is_preserved_as_far_as_append_allows(owner: Path) -> None:
    """A valid table export without its final newline still receives a valid block."""
    path = owner / EDITOR
    original = path.read_bytes().rstrip(b"\n")
    path.write_bytes(original)
    emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    assert path.read_bytes().startswith(original + b"\nselect_permissions:\n")


@pytest.mark.parametrize("command", ["doctor", "generate", "emit-migration", "check"])
def test_real_entrypoint_does_not_write_workdir_usage_stats(owner: Path, command: str) -> None:
    """The installed cdc entrypoint runs from a foreign project without _docs writes."""
    common = ["--source", str(owner / "source.json"), "--catalog", str(owner / "catalog.json")]
    args = [command] + ([] if command == "doctor" else common)
    if command in {"emit-migration", "check"}:
        args += ["--hsr", str(owner)]
    if command == "emit-migration":
        args += ["--migration-version", "1800000000000"]
    if command == "check":
        emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    repo = str(Path(__file__).resolve().parent.parent)
    result = subprocess.run(
        [sys.executable, "-m", "cdc_generator.cli.commands", "rbac", *args],
        cwd=owner,
        env={**os.environ, "PYTHONPATH": repo},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    json.loads(result.stdout)
    assert not (owner / "_docs").exists()
