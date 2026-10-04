"""Source/receipt refusals and immutable history for the approved finite envelope."""

from __future__ import annotations

import base64
import copy
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from cdc_generator.cli.commands import _click_cli
from cdc_generator.core.rbac import provenance
from cdc_generator.core.rbac.artifacts import G4, LOCK, check, emit, reattest
from cdc_generator.core.rbac.composition import Sources
from cdc_generator.core.rbac.rendering import render_migration, select_permissions
from cdc_generator.core.rbac.validation import Json, canonical, compile_contract, digest, load_json, mapping, parse_json, sequence
from tests.rbac_composition_fixture import FIXTURES, catalog
from tests.test_rbac_provenance import owner as prepared_owner

owner = prepared_owner


def compile_fixture(value: Json | None = None) -> tuple[bytes, bytes]:
    """Exercise the actual compiler entry with independently explicit writer authority."""
    return render_migration(compile_contract(load_json(FIXTURES / "writer.opendd.json"), catalog() if value is None else value), None)


def test_qualified_generate_is_policies_only_and_select_only() -> None:
    source, value = load_json(FIXTURES / "writer.opendd.json"), catalog()
    current = compile_contract(source, value)
    up, down = render_migration(current, None)
    assert up == compile_fixture(copy.deepcopy(value))[0]
    assert up.count(b"CREATE POLICY") == 6
    assert b"WITH CHECK" in up and b"exact generated/retained policy definition drift" in up
    for forbidden in (b"GRANT ", b"REVOKE ", b"ENABLE ROW LEVEL", b"FORCE ROW LEVEL", b"CREATE FUNCTION"):
        assert forbidden not in up
    assert b"rollback lacks qualified predecessor writer fences" in down and b"DROP POLICY" not in down
    strict = compile_contract(load_json(FIXTURES / "editor.rbac.json"), load_json(FIXTURES / "editor.catalog.json"))
    assert select_permissions(current) == select_permissions(strict)
    with pytest.raises(ValueError, match="Removing qualified"):
        render_migration(strict, current)


@pytest.mark.parametrize(
    "failure",
    [
        "unknown-property",
        "broad-grant",
        "missing-snapshot",
        "source-bytes",
        "arbitrary-review",
        "handwritten",
        "partial-coverage",
        "receipt-output",
        "compiler-context",
        "worker",
    ],
)
def test_source_and_generated_receipt_refuse_before_emission(owner: Path, failure: str) -> None:
    value = catalog()
    envelope = mapping(value["ownerEnvelope"])
    auth = mapping(envelope["writerAuthorizationSource"])
    snapshots = sequence(value["sourceSnapshots"])
    if failure == "unknown-property":
        envelope["skipPreflight"] = True
    elif failure == "broad-grant":
        envelope["privileges"] = ["ALL"]
    elif failure == "missing-snapshot":
        snapshots.pop()
    elif failure == "source-bytes":
        mapping(snapshots[0])["base64"] = base64.b64encode(b"{}\n").decode()
    elif failure == "arbitrary-review":
        mapping(auth["ownerSourceReview"])["commit"] = "0" * 40
    elif failure == "partial-coverage":
        auth["commands"] = ["INSERT"]
    else:
        ref = (
            auth["ownerDeclarativeSource"]
            if failure == "handwritten"
            else (
                envelope["workerSource"]
                if failure == "worker"
                else auth["compilerSource"] if failure == "compiler-context" else auth["compilerGenerationReceipt"]
            )
        )
        snapshot = next(mapping(item) for item in snapshots if mapping(item)["reference"] == ref)
        data = parse_json(base64.b64decode(str(snapshot["base64"])))
        if failure == "handwritten":
            data = {"sql": "CREATE POLICY handwritten FOR INSERT WITH CHECK (true)"}
        elif failure == "worker":
            mapping(data)["actualWorker"] = "admin"
        elif failure == "compiler-context":
            mapping(data)["compiler"] = "handwritten"
        else:
            mapping(data)["policySetBase64"] = base64.b64encode(b"CREATE POLICY forged").decode()
        # Rehashing a fake/changed receipt is insufficient.
        mapping(ref)["sha256"] = digest(canonical(data))
        snapshot["reference"] = copy.deepcopy(ref)
        snapshot["base64"] = base64.b64encode(canonical(data)).decode()
    (owner / "source.json").write_bytes((FIXTURES / "writer.opendd.json").read_bytes())
    (owner / "catalog.json").write_bytes(canonical(value))
    before = {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}
    with pytest.raises(ValueError):
        emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    assert before == {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}


def test_history_bindings_upgrade_down_and_g4_never_written(owner: Path) -> None:
    value = catalog()
    (owner / "source.json").write_bytes((FIXTURES / "writer.opendd.json").read_bytes())
    (owner / "catalog.json").write_bytes(canonical(value))
    g4 = (owner / G4).read_bytes()
    first = emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    original_up = (owner / "migrations/default/1800000000000_rbac_editor_qnrs/up.sql").read_bytes()
    value["provenance"] = "disclosed fixture upgrade without changed rights"
    (owner / "catalog.json").write_bytes(canonical(value))
    second = emit(owner, owner / "source.json", owner / "catalog.json", "1800000000001")
    assert sequence(second["history"])[0] == sequence(first["history"])[0]
    assert (owner / "migrations/default/1800000000000_rbac_editor_qnrs/up.sql").read_bytes() == original_up
    assert (owner / G4).read_bytes() == g4
    assert check(owner, owner / "source.json", owner / "catalog.json") == second
    assert b"exact generated/retained policy definition drift" in (owner / "migrations/default/1800000000001_rbac_editor_qnrs/down.sql").read_bytes()
    previous_hash = digest((owner / LOCK).read_bytes())
    proposal = reattest(owner, previous_hash, "TEST ONLY exact source candidate")
    reattest(owner, previous_hash, "TEST ONLY exact source candidate", str(proposal["candidate_sha256"]))
    assert sequence(mapping(proposal["candidate"])["history"]) == sequence(second["history"])
    assert (owner / G4).read_bytes() == g4


def test_changed_writer_bytes_refuse_reattest(owner: Path) -> None:
    value = catalog()
    (owner / "source.json").write_bytes((FIXTURES / "writer.opendd.json").read_bytes())
    (owner / "catalog.json").write_bytes(canonical(value))
    emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    original = provenance.render_migration

    def changed(current: provenance.Contract, previous: provenance.Contract | None) -> tuple[bytes, bytes]:
        up, down = original(current, previous)
        return up + b"-- changed writer bytes\n", down

    with patch.object(provenance, "render_migration", changed), pytest.raises(ValueError, match="Regenerated emission"):
        reattest(owner, digest((owner / LOCK).read_bytes()), "TEST ONLY candidate")


def test_real_cli_validates_and_generates_bound_writer_source(owner: Path) -> None:
    source, catalog_path = owner / "source.json", owner / "catalog.json"
    source.write_bytes((FIXTURES / "writer.opendd.json").read_bytes())
    catalog_path.write_bytes(canonical(catalog()))
    runner = CliRunner()
    for command in ("validate", "generate"):
        result = runner.invoke(_click_cli, ["rbac", command, "--source", str(source), "--catalog", str(catalog_path)])
        assert result.exit_code == 0, result.output
        if command == "generate":
            assert mapping(parse_json(result.output.encode()))["up.sql"] == compile_fixture(load_json(catalog_path))[0].decode()


def test_unbound_and_duplicate_source_snapshots_refuse() -> None:
    value = catalog()
    snapshots = sequence(value["sourceSnapshots"])
    with pytest.raises(ValueError, match="Duplicate"):
        Sources([snapshots[0], snapshots[0]])
    mapping(mapping(snapshots[-1])["reference"])["path"] = "unbound"
    with pytest.raises(ValueError, match="Absent"):
        compile_fixture(value)


@pytest.mark.parametrize(
    "mutation",
    [
        "membership-shape",
        "missing-role",
        "role-fields",
        "role-type",
        "privileged",
        "acl-shape",
        "owner",
        "missing-right",
        "grant-option",
        "column",
        "schema-create",
        "broad-default",
    ],
)
def test_source_acl_membership_defaults_cannot_bless_unknown_or_broad_rights(mutation: str) -> None:
    """Even rehashed/generated fixture receipts must obey the finite source envelope."""
    readback = mapping(load_json(FIXTURES / "writer.readback.json"))
    membership = mapping(readback["membership"])
    roles = sequence(membership["roles"])
    acl = mapping(readback["acl"])
    rights = sequence(acl["tableAcl"])
    if mutation == "membership-shape":
        membership["unproved"] = True
    elif mutation == "missing-role":
        roles.clear()
    elif mutation == "role-fields":
        del mapping(roles[0])["inherit"]
    elif mutation == "role-type":
        mapping(roles[0])["login"] = "true"
    elif mutation == "privileged":
        mapping(roles[0])["createRole"] = True
    elif mutation == "acl-shape":
        del acl["columnAcl"]
    elif mutation == "owner":
        acl["owner"] = "editor_app"
    elif mutation == "missing-right":
        rights.pop(0)
    elif mutation == "grant-option":
        mapping(rights[0])["grantOption"] = True
    elif mutation == "column":
        acl["columnAcl"] = [{"grantee": "editor_app", "privilege": "UPDATE", "column": "id"}]
    elif mutation == "schema-create":
        sequence(acl["schemaAcl"]).append({"grantee": "editor_app", "privilege": "CREATE"})
    else:
        readback["defaults"] = [
            {
                "creator": "postgres",
                "schema": None,
                "kind": "r",
                "grantor": "postgres",
                "grantee": "PUBLIC",
                "privilege": "SELECT",
                "grantOption": False,
            }
        ]
    with pytest.raises(ValueError):
        compile_fixture(catalog(readback))


def test_exact_source_bytes_refuse_even_output_identical_format_tampering() -> None:
    """Original source identity must not collapse to canonical meaning under a stale hash."""
    value = catalog()
    snapshot = mapping(sequence(value["sourceSnapshots"])[0])
    raw = base64.b64decode(str(snapshot["base64"]))
    snapshot["base64"] = base64.b64encode(raw + b" ").decode()
    with pytest.raises(ValueError, match="changed source snapshot bytes"):
        compile_fixture(value)


@pytest.mark.parametrize("mutation", ["definition-hash", "source-body", "null-fallback"])
def test_retained_source_identity_is_not_semantic_normalization(mutation: str) -> None:
    """A semantically similar tuple still needs its exact reviewed definition/source bytes."""
    tenant = "customer_id = NULLIF(current_setting('app.customer_id',true),'')::uuid"
    value = catalog(
        retained=[{"name": "other_owner", "command": "ALL", "roles": ["editor_app"], "permissive": False, "using": tenant, "withCheck": tenant}]
    )
    envelope = mapping(value["ownerEnvelope"])
    entry = mapping(sequence(envelope["retainedPolicies"])[0])
    if mutation == "definition-hash":
        entry["definitionSha256"] = "0" * 64
    elif mutation == "source-body":
        entry["using"] = "(" + tenant + ")"
        definition = {key: entry[key] for key in ("name", "command", "roles", "permissive", "using", "withCheck")}
        entry["definitionSha256"] = digest(canonical(definition))
    else:
        entry["withCheck"] = None
    # Rebind the compiler receipt; this mutation must reach the retained-source
    # boundary rather than fail only because the surrounding receipt is stale.
    ref = mapping(mapping(envelope["writerAuthorizationSource"])["compilerGenerationReceipt"])
    snapshot = next(mapping(item) for item in sequence(value["sourceSnapshots"]) if mapping(item)["reference"] == ref)
    receipt = mapping(parse_json(base64.b64decode(str(snapshot["base64"]))))
    receipt["sourceBindings"] = {key: envelope[key] for key in ("membershipSource", "creatorDefaultsSource", "workerSource", "retainedPolicies")}
    ref["sha256"] = digest(canonical(receipt))
    snapshot.update({"reference": copy.deepcopy(ref), "base64": base64.b64encode(canonical(receipt)).decode()})
    with pytest.raises(ValueError, match=r"Retained policy definition/source mismatch|Owner envelope schema clause"):
        compile_fixture(value)


def test_self_consistent_forged_review_is_not_authority() -> None:
    """Arbitrary review bytes + valid compiler regeneration still cannot qualify a source."""
    value = catalog()
    envelope = mapping(value["ownerEnvelope"])
    auth = mapping(envelope["writerAuthorizationSource"])
    snapshots = sequence(value["sourceSnapshots"])
    review_ref = mapping(auth["ownerSourceReview"])
    review_snapshot = next(mapping(item) for item in snapshots if mapping(item)["reference"] == review_ref)
    review = mapping(parse_json(base64.b64decode(str(review_snapshot["base64"]))))
    review["authority"] = "arbitrary syntactically valid self approval"
    review_ref["sha256"] = digest(canonical(review))
    review_snapshot.update({"reference": copy.deepcopy(review_ref), "base64": base64.b64encode(canonical(review)).decode()})
    receipt_ref = mapping(auth["compilerGenerationReceipt"])
    receipt_snapshot = next(mapping(item) for item in snapshots if mapping(item)["reference"] == receipt_ref)
    receipt = mapping(parse_json(base64.b64decode(str(receipt_snapshot["base64"]))))
    receipt["ownerSourceReview"] = copy.deepcopy(review_ref)
    receipt_ref["sha256"] = digest(canonical(receipt))
    receipt_snapshot.update({"reference": copy.deepcopy(receipt_ref), "base64": base64.b64encode(canonical(receipt)).decode()})
    with pytest.raises(ValueError, match="Actual owning declaration/review not admitted"):
        compile_fixture(value)


def test_output_identical_upgrade_reattests_prior_writer_implementation(owner: Path, tmp_path: Path) -> None:
    """Prior writer contexts survive only through the validated byte-identical review chain."""
    (owner / "source.json").write_bytes((FIXTURES / "writer.opendd.json").read_bytes())
    (owner / "catalog.json").write_bytes(canonical(catalog()))
    previous = emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    package = tmp_path / "changed-package"
    for name in provenance.IMPLEMENTATION_FILES:
        target = package / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((provenance.PACKAGE / name).read_bytes())
    implementation_file = package / "core/rbac/writer_rules.py"
    implementation_file.write_bytes(implementation_file.read_bytes() + b"\n# Output-identical reviewed source upgrade\n")
    before = {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}
    with patch.object(provenance, "PACKAGE", package):
        with pytest.raises(ValueError, match="provenance drift"):
            check(owner, owner / "source.json", owner / "catalog.json")
        old_hash = digest((owner / LOCK).read_bytes())
        proposal = reattest(owner, old_hash, "TEST ONLY reviewed output-identical writer upgrade")
        candidate = mapping(proposal["candidate"])
        assert candidate["history"] == previous["history"]
        assert mapping(sequence(candidate["reattestations"])[-1])["from"] == provenance.context(previous)
        reattest(owner, old_hash, "TEST ONLY reviewed output-identical writer upgrade", str(proposal["candidate_sha256"]))
        assert check(owner, owner / "source.json", owner / "catalog.json") == candidate
        new_value = catalog()
        new_value["provenance"] = "TEST ONLY subsequent reviewed writer emission"
        (owner / "catalog.json").write_bytes(canonical(new_value))
        upgraded = emit(owner, owner / "source.json", owner / "catalog.json", "1800000000001")
        assert sequence(upgraded["history"])[0] == sequence(previous["history"])[0]
    for path, content in before.items():
        if path not in {owner / LOCK, owner / "catalog.json"}:
            assert path.read_bytes() == content
