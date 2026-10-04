"""Reviewed recovery of output-identical compiler/dependency provenance changes."""

from __future__ import annotations

import copy
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from cdc_generator.cli.commands import _click_cli
from cdc_generator.core.rbac import provenance, rendering
from cdc_generator.core.rbac.artifacts import LOCK, check, reattest
from cdc_generator.core.rbac.provenance import context, implementation, seal
from cdc_generator.core.rbac.validation import Contract, Json, canonical, digest, load_json, mapping, sequence
from tests.test_rbac_provenance import FIXTURES, _emit, _upgrade
from tests.test_rbac_provenance import owner as prepared_owner

owner = prepared_owner

REVIEW = "test-only ASMA-8350 exact-candidate review"
OLD_LOCK_SHA = "f649cfcd7120e3e60dc7d31e6d91ff6a2a6ecae25abae62870a02a63c40048d7"


def _files(owner: Path) -> dict[Path, bytes]:
    """Capture the entire project so no SQL or other-owner file may be rewritten."""
    return {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}


def _legacy(owner: Path, *, upgrade: bool = False) -> dict[str, Json]:
    """Reproduce the actual reviewed v3 lock with its original six-file receipt."""
    state = _emit(owner, 0)
    if upgrade:
        state = _upgrade(owner)
    reviewed = mapping(load_json(FIXTURES / "reviewed-lock-v3.json"))
    state["implementation"] = copy.deepcopy(reviewed["implementation"])
    del state["environment"]
    del state["reattestations"]
    state = seal(state)
    (owner / LOCK).write_bytes(canonical(state))
    if not upgrade:
        assert digest(canonical(state)) == OLD_LOCK_SHA
        assert (owner / LOCK).read_bytes() == (FIXTURES / "reviewed-lock-v3.json").read_bytes()
    return state


def _propose(owner: Path) -> dict[str, Json]:
    """Require the exact old file hash even for a read-only proposal."""
    return reattest(owner, digest((owner / LOCK).read_bytes()), REVIEW)


def _apply(owner: Path, proposal: dict[str, Json]) -> dict[str, Json]:
    """Use an externally selected candidate hash; there is no implicit approval."""
    return reattest(owner, digest((owner / LOCK).read_bytes()), REVIEW, str(proposal["candidate_sha256"]))


@pytest.mark.parametrize(
    ("dependency", "allowed_version"),
    [("attrs", "25.4.0"), ("rpds-py", "0.27.1"), ("referencing", "0.36.2"), ("jsonschema-specifications", "2025.4.1"), ("ruamel.yaml", "0.18.16")],
)
def test_allowed_dependency_change_checks_and_upgrades(owner: Path, dependency: str, allowed_version: str) -> None:
    """Flexible dependency metadata never vetoes byte-identical generated history."""
    state = _emit(owner, 0)
    before = _files(owner)
    original_version = provenance.version

    def changed_version(name: str) -> str:
        return allowed_version if name == dependency else original_version(name)

    with patch.object(provenance, "version", changed_version):
        assert check(owner, owner / "source.json", owner / "catalog.json") == state
        assert _emit(owner, 0) == state
        assert _files(owner) == before
        upgraded = _upgrade(owner)
        assert check(owner, owner / "source.json", owner / "catalog.json") == upgraded
        assert mapping(upgraded["environment"])[dependency] == allowed_version
        assert upgraded["implementation"] == state["implementation"]
    assert mapping(mapping(state["implementation"])["runtime"]) == {"jsonschema": "4.25.1"}
    for relative in mapping(state["files"]):
        assert (owner / relative).read_bytes() == before[owner / relative]


@pytest.mark.parametrize("shared_file", ["helpers/yaml_loader.py", "core/migration_generator/file_writers.py"])
def test_output_identical_shared_module_edits_are_not_fingerprinted(owner: Path, tmp_path: Path, shared_file: str) -> None:
    """A comment-only shared-module change leaves check and upgrade available."""
    state = _emit(owner, 0)
    package = tmp_path / "packaged-files"
    for name in [*provenance.IMPLEMENTATION_FILES, shared_file]:
        path = package / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((provenance.PACKAGE / name).read_bytes())
    shared = package / shared_file
    shared.write_bytes(shared.read_bytes() + b"\n# Output-identical shared CDC maintenance\n")
    with patch.object(provenance, "PACKAGE", package):
        assert implementation() == state["implementation"]
        assert check(owner, owner / "source.json", owner / "catalog.json") == state
        upgraded = _upgrade(owner)
        assert check(owner, owner / "source.json", owner / "catalog.json") == upgraded
    assert shared_file not in mapping(mapping(state["implementation"])["files"])


@pytest.mark.parametrize("upgrade", [False, True])
def test_reviewed_reattest_recovers_real_old_lock_and_preserves_every_output(owner: Path, upgrade: bool) -> None:
    """Recover reviewed f649cfcd provenance; retain prior implementation and review chain."""
    previous = _legacy(owner, upgrade=upgrade)
    before = _files(owner)
    with pytest.raises(ValueError, match="provenance drift"):
        check(owner, owner / "source.json", owner / "catalog.json")
    proposal = _propose(owner)
    assert proposal["applied"] is False and _files(owner) == before
    candidate = mapping(proposal["candidate"])
    review = mapping(sequence(candidate["reattestations"])[-1])
    assert review["from"] == context(previous)
    assert review["to"] == context(candidate)
    assert review["previous_lock_sha256"] == digest(before[owner / LOCK])
    assert review["history_sha256"] == digest(canonical(previous["history"]))
    assert review["review_reference"] == REVIEW
    assert candidate["implementation"] == implementation()
    assert candidate["history"] == previous["history"]
    result = _apply(owner, proposal)
    assert result["applied"] is True
    assert (owner / LOCK).read_bytes() == canonical(candidate)
    assert {path: data for path, data in _files(owner).items() if path != owner / LOCK} == {
        path: data for path, data in before.items() if path != owner / LOCK
    }
    assert check(owner, owner / "source.json", owner / "catalog.json") == candidate
    # New immutable history may follow a reviewed prefix without erasing its receipt.
    catalog = mapping(load_json(owner / "catalog.json"))
    catalog["provenance"] = "post-review input revision"
    (owner / "catalog.json").write_bytes(canonical(catalog))
    upgraded = _emit(owner, 2)
    assert upgraded["reattestations"] == candidate["reattestations"]
    assert check(owner, owner / "source.json", owner / "catalog.json") == upgraded
    second = _propose(owner)
    _apply(owner, second)
    assert len(sequence(mapping(second["candidate"])["reattestations"])) == 2
    assert check(owner, owner / "source.json", owner / "catalog.json") == second["candidate"]


@pytest.mark.parametrize("changed", ["old_up", "old_down", "new_up", "new_down", "select", "shared_checksum"])
def test_genuine_output_change_refuses_check_upgrade_and_reattest(owner: Path, changed: str) -> None:
    """Every historical direction and canonical SELECT must regenerate byte-identically."""
    _emit(owner, 0)
    _upgrade(owner)
    before = _files(owner)
    render = provenance.render_migration
    permissions = provenance.select_permissions
    checksum = rendering.inject_checksum

    def changed_sql(current: Contract, previous: Contract | None) -> tuple[bytes, bytes]:
        up, down = render(current, previous)
        target = changed.startswith("old") == (previous is None)
        if target and changed.endswith("up"):
            up += b"-- genuine output change\n"
        if target and changed.endswith("down"):
            down += b"-- genuine output change\n"
        return up, down

    def changed_select(contract: Contract) -> list[Json]:
        output = permissions(contract)
        mapping(mapping(output[0])["permission"])["filter"] = {}
        return output

    def changed_checksum(sql: str) -> str:
        return checksum(sql) + "-- changed shared checksum output\n"

    selected = (
        patch.object(provenance, "select_permissions", changed_select)
        if changed == "select"
        else (
            patch.object(rendering, "inject_checksum", changed_checksum)
            if changed == "shared_checksum"
            else patch.object(provenance, "render_migration", changed_sql)
        )
    )
    with selected:
        for action in [lambda: check(owner, owner / "source.json", owner / "catalog.json"), lambda: _emit(owner, 2), lambda: _propose(owner)]:
            with pytest.raises(ValueError, match="Regenerated emission provenance mismatch"):
                action()
        assert _files(owner) == before


@pytest.mark.parametrize("failure", ["old_hash", "old_hash_shape", "review", "candidate", "missing_lock", "artifact_drift"])
def test_review_binding_and_artifact_drift_refuse_without_writes(owner: Path, failure: str) -> None:
    _legacy(owner)
    old_hash = digest((owner / LOCK).read_bytes())
    reviewed_hash = str(_propose(owner)["candidate_sha256"])
    if failure == "missing_lock":
        (owner / LOCK).unlink()
    elif failure == "artifact_drift":
        path = owner / "migrations/default/1800000000000_rbac_editor_qnrs/up.sql"
        path.write_bytes(path.read_bytes() + b"-- unreviewed\n")
    before = _files(owner)
    with pytest.raises(ValueError):
        reattest(
            owner,
            "bad" if failure == "old_hash_shape" else "0" * 64 if failure == "old_hash" else old_hash,
            " " if failure == "review" else REVIEW,
            "0" * 64 if failure == "candidate" else reviewed_hash,
        )
    assert _files(owner) == before


def test_candidate_review_is_bound_to_current_code_and_environment(owner: Path) -> None:
    """A proposal approved before a runtime change cannot silently apply a new receipt."""
    _legacy(owner)
    proposal = _propose(owner)
    before = _files(owner)
    original_version = provenance.version
    with (
        patch.object(provenance, "version", side_effect=lambda name: "25.4.0" if name == "attrs" else original_version(name)),
        pytest.raises(ValueError, match="candidate SHA256 mismatch"),
    ):
        _apply(owner, proposal)
    assert _files(owner) == before


def test_real_cli_proposes_then_applies_only_reviewed_lock(owner: Path) -> None:
    """The registered command prints a reviewable candidate and requires an exact hash."""
    _legacy(owner)
    old_hash = digest((owner / LOCK).read_bytes())
    args = ["rbac", "reattest", "--hsr", str(owner), "--expected-lock-sha256", old_hash, "--review-reference", REVIEW]
    runner = CliRunner()
    before = _files(owner)
    proposed = runner.invoke(_click_cli, args)
    assert proposed.exit_code == 0, proposed.output
    import json

    candidate = json.loads(proposed.output)
    assert candidate["applied"] is False and _files(owner) == before
    refused = runner.invoke(_click_cli, [*args, "--apply-reviewed-sha256", "0" * 64])
    assert refused.exit_code == 1 and _files(owner) == before
    applied = runner.invoke(_click_cli, [*args, "--apply-reviewed-sha256", candidate["candidate_sha256"]])
    assert applied.exit_code == 0, applied.output
    assert json.loads(applied.output)["applied"] is True
    assert check(owner, owner / "source.json", owner / "catalog.json") == candidate["candidate"]


@pytest.mark.parametrize(
    "failure",
    [
        "implementation_hash",
        "implementation_version",
        "context_shape",
        "context_pin",
        "environment_shape",
        "review_shape",
        "review_hash",
        "history_prefix",
        "review_reference",
        "discontinuity",
        "review_head",
    ],
)
def test_reattest_audit_corruption_is_refused(owner: Path, failure: str) -> None:
    """Re-sealing cannot bypass review continuity, input-prefix or typed receipt checks."""
    _legacy(owner)
    state = mapping(_apply(owner, _propose(owner))["candidate"])
    if failure == "discontinuity":
        state = mapping(_apply(owner, _propose(owner))["candidate"])
    state = mapping(load_json(owner / LOCK))
    review = mapping(sequence(state["reattestations"])[-1])
    if failure == "implementation_hash":
        mapping(mapping(state["implementation"])["files"])["core/rbac/rendering.py"] = "invalid"
    elif failure == "implementation_version":
        mapping(mapping(state["implementation"])["runtime"])["jsonschema"] = ""
    elif failure == "context_shape":
        mapping(review["from"])["unreviewed"] = True
    elif failure == "context_pin":
        mapping(mapping(review["from"])["pins"])["jsonschema"] = ""
    elif failure == "environment_shape":
        mapping(state["environment"])["unknown"] = "unreviewed"
    elif failure == "review_shape":
        review["unreviewed"] = True
    elif failure == "review_hash":
        review["previous_lock_sha256"] = "invalid"
    elif failure == "history_prefix":
        review["emissions"] = 99
    elif failure == "review_reference":
        review["review_reference"] = " "
    elif failure == "discontinuity":
        mapping(review["from"])["implementation"] = mapping(load_json(FIXTURES / "reviewed-lock-v3.json"))["implementation"]
    else:
        mapping(mapping(mapping(review["to"])["implementation"])["files"])["core/rbac/rendering.py"] = "0" * 64
    (owner / LOCK).write_bytes(canonical(seal(state)))
    before = _files(owner)
    for action in [lambda: check(owner, owner / "source.json", owner / "catalog.json"), lambda: _propose(owner)]:
        with pytest.raises(ValueError):
            action()
    assert _files(owner) == before


def test_reattest_filesystem_failure_restores_original_lock(owner: Path) -> None:
    """An interrupted lock-only publication keeps the reviewed lock and every output."""
    _legacy(owner)
    proposal = _propose(owner)
    before = _files(owner)
    write = Path.write_bytes
    failed = False

    def fail_once(path: Path, content: bytes) -> int:
        nonlocal failed
        if path == owner / LOCK and not failed:
            failed = True
            write(path, b"interrupted publication")
            raise OSError("simulated lock failure")
        return write(path, content)

    with patch.object(Path, "write_bytes", fail_once), pytest.raises(OSError, match="simulated lock failure"):
        _apply(owner, proposal)
    assert failed and _files(owner) == before
