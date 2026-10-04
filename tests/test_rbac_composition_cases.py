"""Keep actual installation cases distinct from executed source/ISO subsets."""

from __future__ import annotations

import ast
from pathlib import Path

from cdc_generator.core.rbac.validation import load_json, mapping, sequence, string

FIXTURES = Path(__file__).parent / "fixtures/rbac"
ORACLES = (
    "G5-IDENTITY-NEG",
    "G5-OWNER-NEG",
    "G5-SEARCHPATH-NEG",
    "G5-FOR-SHARE-NEG",
    "G5-GUARD-ACL-NEG",
    "G5-FUNCTION-ACL-NEG",
    "G5-CREATOR-NEG",
    "G5-CREATOR-ATOMIC-NEG",
    "G5-TRIGGER-NEG",
    "G5-SINGLE-WRITER-NEG",
    "G5-LEGACY-TRANSPORT-NEG",
    "G5-FUNCTION-READ-NEG",
    "G5-PERMIT-NEG",
    "G2-WORKER-NEG",
    "G2-ACTIVATION-GRANT-NEG",
    "G1-REQ001-UPGRADE-NEG",
    "G5-UPGRADE-PRESERVE-NEG",
    "G5-ROLLBACK-NEG",
)
SPECS = (
    "strict-REQ001-refusal-unchanged",
    "strict-retained-policy-refusal-unchanged",
    "qualified-owner-positive",
    "foreign-or-missing-context-negative",
    "acl-missing-each",
    "acl-excess-each",
    "acl-changed-origins",
    "policy-missing-extra-name",
    "policy-changed-definition-each",
    "policy-read-union",
    "policy-writer-positive-closure",
    "tenant-change-negative",
    "reader-remains-select-only",
    "worker-unknown-negative",
    "guard-positive-and-excess",
    "history-golden-strict",
    "history-envelope-byte-binding",
    "history-upgrade-and-down",
    "history-reattest-changed-output",
    "writer-generated-provenance-negative",
    "retained-writer-not-authority",
    "policy-write-union",
)
SOURCE_ONLY = {
    "history-golden-strict",
    "history-envelope-byte-binding",
    "history-reattest-changed-output",
    "writer-generated-provenance-negative",
    "retained-writer-not-authority",
}
TARGETS = {
    "bootstrapConnection",
    "bunConnectionLogin",
    "existingHasuraConnectionLogin",
    "guardOwner",
    "installedArtifactHead",
    "installedConsumerHead",
    "installedMetadataSha256",
    "nonprodIdentity",
    "objectCreators",
    "stagingControls",
}


def test_exact_original_oracle_and_variant_ids_stay_unexecuted() -> None:
    ledger = mapping(load_json(FIXTURES / "writer-composition-cases.json"))
    oracles = [mapping(item) for item in sequence(ledger["originalOracles"])]
    assert tuple(item["id"] for item in oracles) == ORACLES
    expected = []
    for item in oracles:
        modes = ["upgrade"] if item["id"] in {"G1-REQ001-UPGRADE-NEG", "G5-UPGRADE-PRESERVE-NEG"} else ["fresh", "upgrade"]
        assert item["modes"] == modes and item["status"] == "BLOCKED_NOT_EXECUTED"
        expected.extend({"id": item["id"], "mode": mode, "status": "BLOCKED_NOT_EXECUTED", "executed": False} for mode in modes)
    assert ledger["originalVariants"] == expected
    assert len(expected) == 34


def test_exact_runtime_spec_ids_and_test_references_do_not_claim_full_pass() -> None:
    ledger = mapping(load_json(FIXTURES / "writer-composition-cases.json"))
    specs = [mapping(item) for item in sequence(ledger["runtimeSpecifications"])]
    assert tuple(item["id"] for item in specs) == SPECS
    for item in specs:
        assert item["actualStatus"] == "BLOCKED_NOT_EXECUTED" and item["fullCasePass"] is False
        assert item["remaining"]
        status = (
            "BLOCKED_NOT_EXECUTED"
            if item["id"] == "guard-positive-and-excess"
            else "SOURCE_CHECKS_ONLY_RUNTIME_NOT_CLAIMED" if item["id"] in SOURCE_ONLY else "EXECUTED_BOUNDED_ISO_SUBSET"
        )
        assert item["fixtureEvidenceStatus"] == status
        assert bool(item["isoSubsetTests"]) == (status == "EXECUTED_BOUNDED_ISO_SUBSET")
        assert bool(item["sourceChecks"]) == (item["id"] in SOURCE_ONLY | {"history-upgrade-and-down"})
        for reference in sequence(item["isoSubsetTests"]) + sequence(item["sourceChecks"]):
            path, function = string(reference).split("::")
            tree = ast.parse((Path(__file__).parent.parent / path).read_text())
            assert function in {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}


def test_all_actual_target_inputs_and_authority_pins_remain_explicit() -> None:
    ledger = mapping(load_json(FIXTURES / "writer-composition-cases.json"))
    assert ledger["targetInputs"] == dict.fromkeys(TARGETS)
    assert all(ledger[key] is None for key in ("actualWorkerFacts", "actualRevocationFacts", "actualHasuraContext"))
    assert ledger["baseCommit"] == "1a2aab52154dd5ff3836633512972fd6a4ecd728"
    assert ledger["authoritySha256"] == "6badca45f5659342616b1232466bf088fbedaa9fc994f65ce87fc60233b69be1"
    assert ledger["attachmentSha256"] == "4a3eb93017637557b7c5cb37bdbad494b4a3c80b5c2e9aa167363d2835bb8ea5"
    assert ledger["holds"] == [
        "NO_TARGET_INSTALL",
        "NO_ARTIFACT_EMISSION",
        "NO_PRODUCT_RLS_ACTIVATION",
        "NO_ARTIFACT_MASTER_MERGE_AUTO_INSTALL",
        "NO_A6_OR_ASMA_8350_COMPLETION",
        "PG_AND_HASURA_NOT_ONE_TRANSACTION",
    ]
