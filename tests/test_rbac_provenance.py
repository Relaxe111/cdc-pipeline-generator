"""Reproducible history, complete inventory and pinned installation contract checks."""

from __future__ import annotations

import base64
import copy
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from cdc_generator.cli.commands import _click_cli
from cdc_generator.core.rbac import provenance
from cdc_generator.core.rbac.artifacts import G4, LOCK, TABLES, check, emit
from cdc_generator.core.rbac.provenance import implementation, restore, seal
from cdc_generator.core.rbac.rendering import render_migration
from cdc_generator.core.rbac.validation import Json, canonical, digest, load_json, mapping, sequence

FIXTURES = Path(__file__).parent / "fixtures/rbac"


@pytest.fixture()
def owner(tmp_path: Path) -> Path:
    """Use a disposable canonical owner with exact prepared source fixtures."""
    root = tmp_path / "owner"
    (root / TABLES).mkdir(parents=True)
    (root / "config.yaml").write_text("version: 3\nmetadata_directory: metadata\n")
    (root / TABLES / "tables.yaml").write_text('- "!include editor_qnrs.yaml"\n- "!include public_queries.yaml"\n')
    for filename in ("editor_qnrs.yaml", "public_queries.yaml"):
        (root / TABLES / filename).write_bytes((FIXTURES / filename).read_bytes())
    (root / "source.json").write_bytes((FIXTURES / "editor.rbac.json").read_bytes())
    (root / "catalog.json").write_bytes((FIXTURES / "editor.catalog.json").read_bytes())
    return root


def _emit(owner: Path, number: int) -> dict[str, Json]:
    """Use explicit deterministic versions, never a clock-derived migration name."""
    return emit(owner, owner / "source.json", owner / "catalog.json", str(1800000000000 + number))


def _upgrade(owner: Path) -> dict[str, Json]:
    """Change a genuine therapist predicate while keeping the first SQL immutable."""
    source = sequence(load_json(owner / "source.json"))
    permissions = sequence(mapping(mapping(source[0])["definition"])["permissions"])
    mapping(permissions[2])["select"] = copy.deepcopy(mapping(permissions[0])["select"])
    (owner / "source.json").write_bytes(canonical(source))
    return _emit(owner, 1)


def test_relocatable_exact_byte_receipts_and_history(owner: Path, tmp_path: Path) -> None:
    """Each emission can be regenerated after inputs move/change, including raw formatting."""
    alternate = tmp_path / "relocated"
    shutil.copytree(owner, alternate)
    first = _emit(owner, 0)
    assert first == _emit(alternate, 0)
    assert (owner / LOCK).read_bytes() == (alternate / LOCK).read_bytes()
    state = _upgrade(owner)
    assert state == _upgrade(alternate)
    assert canonical(state) == (owner / LOCK).read_bytes() == (alternate / LOCK).read_bytes()
    previous = None
    for entry_value in sequence(state["history"]):
        entry = mapping(entry_value)
        current, receipts = restore(entry["inputs"])
        for name in ("source", "catalog"):
            receipt = mapping(receipts[name])
            raw = base64.b64decode(str(receipt["base64"]), validate=True)
            assert digest(raw) == receipt["sha256"]
        up, down = render_migration(current, previous)
        migration = str(entry["migration"])
        assert (owner / migration / "up.sql").read_bytes() == up
        assert (owner / migration / "down.sql").read_bytes() == down
        previous = current
    assert mapping(mapping(mapping(sequence(state["history"])[0])["inputs"])["source"])["sha256"] == digest(
        (FIXTURES / "editor.rbac.json").read_bytes()
    )
    assert check(owner, owner / "source.json", owner / "catalog.json") == state
    assert str(owner) not in canonical(state).decode()
    assert str(G4) not in mapping(state["files"])


def _tamper_history(owner: Path, state: dict[str, Json], failure: str) -> None:
    """Poison input receipts or migration links, including coherently forged SQL hashes."""
    history = sequence(state["history"])
    first = mapping(history[0])
    receipt = mapping(mapping(first["inputs"])["source"])
    if failure == "format":
        state["lock_format"] = 2
    elif failure == "implementation":
        mapping(mapping(state["implementation"])["files"])["core/rbac/rendering.py"] = "0" * 64
    elif failure == "runtime":
        mapping(mapping(state["implementation"])["runtime"])["ruamel.yaml"] = "unreviewed"
    elif failure == "empty_history":
        state["history"] = []
    elif failure == "missing_history":
        del state["history"]
    elif failure == "history_order":
        history.reverse()
    elif failure == "history_duplicate":
        history.append(copy.deepcopy(history[-1]))
    elif failure == "migration_identity":
        first["migration"] = "../unowned"
    elif failure == "receipt_missing":
        del receipt["base64"]
    elif failure == "receipt_base64":
        receipt["base64"] = "bad!"
    elif failure == "receipt_hash":
        receipt["sha256"] = "0" * 64
    elif failure == "receipt_canonical":
        receipt["canonical_sha256"] = "0" * 64
    elif failure == "receipt_extra":
        receipt["unowned"] = True
    elif failure == "predecessor":
        mapping(history[1])["previous_contract_sha256"] = "0" * 64
    elif failure == "old_input":
        raw = (FIXTURES / "editor.catalog.json").read_bytes() + b"\n"
        mapping(mapping(first["inputs"])["catalog"])["base64"] = base64.b64encode(raw).decode()
    elif failure == "old_sql_forged":
        relative = str(first["migration"]) + "/up.sql"
        content = (owner / relative).read_bytes() + b"-- drift\n"
        (owner / relative).write_bytes(content)
        mapping(first["files"])[relative] = digest(content)
        mapping(state["files"])[relative] = digest(content)
    elif failure == "removed_history":
        history.pop(0)


def _tamper_outputs(owner: Path, state: dict[str, Json], failure: str) -> None:
    """Poison convenience fields or the owning filesystem inventory."""
    first = mapping(sequence(state["history"])[0])
    if failure == "g4_identity":
        state["preserved"] = {}
    elif failure == "files_extra":
        mapping(state["files"])[str(G4)] = digest((owner / G4).read_bytes())
    elif failure == "latest_catalog":
        mapping(state["catalog"])["provenance"] = "unreviewed"
    elif failure == "latest_previous":
        state["previous"] = None
    elif failure == "select_path":
        mapping(state["select"])["path"] = str(G4)
    elif failure == "extra_field":
        state["unreviewed"] = True
    elif failure == "orphan_sql":
        destination = owner / "migrations/default/1800000000002_rbac_editor_qnrs"
        shutil.copytree(owner / str(first["migration"]), destination)
    elif failure == "extra_owned_file":
        (owner / str(first["migration"]) / "extra.sql").write_text("-- unrecorded\n")
    elif failure == "missing_sql":
        (owner / str(first["migration"]) / "down.sql").unlink()
    elif failure == "missing_index":
        (owner / TABLES / "tables.yaml").write_text("[]\n")


@pytest.mark.parametrize(
    "failure",
    [
        "format",
        "implementation",
        "runtime",
        "seal",
        "empty_history",
        "missing_history",
        "history_order",
        "history_duplicate",
        "migration_identity",
        "receipt_missing",
        "receipt_base64",
        "receipt_hash",
        "receipt_canonical",
        "receipt_extra",
        "predecessor",
        "old_input",
        "old_sql_forged",
        "removed_history",
        "g4_identity",
        "files_extra",
        "latest_catalog",
        "latest_previous",
        "select_path",
        "extra_field",
        "noncanonical_lock",
        "orphan_sql",
        "extra_owned_file",
        "missing_sql",
        "missing_index",
    ],
)
def test_history_and_provenance_tampering_fails_closed(owner: Path, failure: str) -> None:
    """A rewritten self-hash cannot hide inconsistent or incomplete emission provenance."""
    _emit(owner, 0)
    state = _upgrade(owner)
    _tamper_outputs(owner, state, failure)
    _tamper_history(owner, state, failure)
    state = seal(state)
    if failure == "seal":
        state["sha256"] = "0" * 64
    (owner / LOCK).write_bytes(canonical(state) + (b"\n" if failure == "noncanonical_lock" else b""))
    before = {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}
    runner = CliRunner()
    args = ["--hsr", str(owner), "--source", str(owner / "source.json"), "--catalog", str(owner / "catalog.json")]
    for command in [["check", *args], ["emit-migration", *args, "--migration-version", "1800000000002"]]:
        result = runner.invoke(_click_cli, ["rbac", *command])
        assert result.exit_code == 1 and "Error:" in result.output, result.output
        assert before == {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}


def test_missing_lock_cannot_reclaim_history(owner: Path) -> None:
    """Deleting provenance cannot establish fresh ownership over existing compiler SQL."""
    _emit(owner, 0)
    (owner / LOCK).unlink()
    with pytest.raises(ValueError, match="deleting provenance"):
        _emit(owner, 1)


@pytest.mark.parametrize("variant", ["file", "bad_version", "symlink"])
def test_reserved_migration_inventory_boundaries(owner: Path, tmp_path: Path, variant: str) -> None:
    """Reject noncanonical identities and destinations before a first write."""
    directory = owner / "migrations/default"
    directory.mkdir(parents=True)
    path = directory / f"{'bad' if variant == 'bad_version' else '1800000000000'}_rbac_editor_qnrs"
    if variant == "file":
        path.write_text("not a directory")
    elif variant == "bad_version":
        path.mkdir()
    else:
        path.symlink_to(tmp_path)
    with pytest.raises(ValueError):
        _emit(owner, 1)


def test_implementation_receipt_is_offline_and_packaged(owner: Path) -> None:
    """Version drift blocks reuse; source hash keys contain only packaged relative paths."""
    state = _emit(owner, 0)
    pins = implementation()
    assert pins == state["implementation"]
    assert all(not Path(key).is_absolute() for key in mapping(pins["files"]))
    with patch.object(provenance, "version", return_value="different"), pytest.raises(ValueError, match="runtime provenance"):
        check(owner, owner / "source.json", owner / "catalog.json")


def test_emission_table_boundary(owner: Path) -> None:
    """Canonical history cannot emit a second table under this single-table contract."""
    catalog = mapping(load_json(owner / "catalog.json"))
    catalog["table"] = "another_table"
    (owner / "catalog.json").write_bytes(canonical(catalog))
    with pytest.raises(ValueError, match=r"bounded to editor\.qnrs"):
        _emit(owner, 0)


def test_install_contract_source_pins_and_exact_envelope() -> None:
    """Guard rights remain traceable to the independently owned immutable source contract."""
    supplement = mapping(load_json(FIXTURES / "installation-contract.json"))
    source = mapping(load_json(FIXTURES / "owning-install-contract.json"))
    install_pin = mapping(mapping(mapping(supplement["owningArtifact"])["contracts"])["install"])
    assert digest((FIXTURES / str(install_pin["localReference"])).read_bytes()) == install_pin["sha256"]
    for name, requirement in mapping(supplement["requirements"]).items():
        assert requirement == source[name]
    envelope = mapping(supplement["exactGuardEnvelope"])
    rights = mapping(source["guardRights"])
    assert envelope["updateColumns"] == rights["UPDATE"]
    assert envelope["execute"] == rights["EXECUTE"]
    for table, columns in mapping(envelope["selectColumns"]).items():
        assert columns == [value.strip() for value in str(mapping(rights["SELECT"])[table]).split(":")[-1].split(",")]
    assert envelope["insert"] == envelope["delete"] == []
    assert envelope["revokePermit"] is envelope["tableOwnership"] is envelope["grantOption"] is False


def test_install_oracles_have_fresh_upgrade_crosswalk_and_no_claimed_execution() -> None:
    """No target missing input or unexecuted negative oracle can be reported as qualified."""
    supplement = mapping(load_json(FIXTURES / "installation-contract.json"))
    source = mapping(load_json(FIXTURES / "owning-install-contract.json"))
    assert supplement["targetInputs"] == source["targetInputs"]
    assert all(value is None for value in mapping(supplement["targetInputs"]).values())
    for fact in mapping(supplement["unknownFacts"]).values():
        assert mapping(fact)["value"] is None and mapping(fact)["status"] == "BLOCKED"
        assert mapping(fact)["evidenceRequired"]
    ids = set()
    for value in sequence(supplement["oracles"]):
        oracle = mapping(value)
        assert oracle["status"] == "BLOCKED_NOT_EXECUTED"
        assert oracle["given"] and oracle["when"] and oracle["then"] and oracle["evidence"]
        assert set(sequence(oracle["modes"])) <= {"fresh", "upgrade"} and "upgrade" in sequence(oracle["modes"])
        clause: Json = source
        for key in str(oracle["sourceClause"]).split("/")[1:]:
            clause = mapping(clause)[key]
        assert clause is not None
        ids.add(str(oracle["id"]))
    assert len(ids) == len(sequence(supplement["oracles"])) == 18
    crosswalk = mapping(supplement["existingGateMap"])
    assert {f"L3-T0{number}" for number in range(1, 9)} <= set(crosswalk)
    for value in crosswalk.values():
        for item in sequence(value) if isinstance(value, list) else []:
            if str(item).startswith(("G5-", "G2-", "G1-")):
                assert str(item) in ids
    status = mapping(supplement["status"])
    assert status["targetAdmission"] == "BLOCKED"
    assert status["freshUpgradeGuardOracles"] == "BLOCKED_NOT_EXECUTED"
    assert status["activation"] == "DEFERRED" and status["A6"] == "NOT_DISCHARGED"
    assert json.loads(canonical(supplement)) == supplement


@pytest.mark.parametrize("failure", ["source", "sql", "lock"])
def test_real_entrypoint_drift_returns_nonzero(owner: Path, failure: str) -> None:
    """Actual module/console exit codes make compiler drift usable in offline automation."""
    state = _emit(owner, 0)
    path = owner / {"source": "source.json", "sql": str(state["migration"]) + "/up.sql", "lock": str(LOCK)}[failure]
    path.write_bytes(path.read_bytes() + b"\n")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "cdc_generator.cli.commands",
            "rbac",
            "check",
            "--hsr",
            str(owner),
            "--source",
            str(owner / "source.json"),
            "--catalog",
            str(owner / "catalog.json"),
        ],
        cwd=owner,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parent.parent)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1 and "Error:" in result.stderr
    assert not (owner / "_docs").exists()
