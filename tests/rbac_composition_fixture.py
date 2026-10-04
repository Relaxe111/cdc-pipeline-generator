"""Disclosed generated ISO snapshots, separate from every actual null targetInput."""

from __future__ import annotations

import base64
import copy
from dataclasses import replace
from pathlib import Path

from cdc_generator.core.rbac.composition import FIXTURE_SOURCE_COMMIT, QUALIFICATION, compiler_context, generation_receipt
from cdc_generator.core.rbac.validation import Json, canonical, compile_contract, digest, load_json, mapping, parse_json
from cdc_generator.core.rbac.writer_rules import CONTEXT, writer_rules

FIXTURES = Path(__file__).parent / "fixtures/rbac"


def catalog(readback: Json | None = None, retained: list[Json] | None = None) -> dict[str, Json]:
    """Embed deterministic receipt bytes; generated: paths denote fixture snapshots, not Git files."""
    values = mapping(load_json(FIXTURES / "writer.readback.json") if readback is None else readback)
    result = mapping(load_json(FIXTURES / "editor.catalog.json"))
    bodies: list[Json] = []

    def snapshot(path: str, body: Json) -> dict[str, Json]:
        raw = canonical(body)
        ref: dict[str, Json] = {"commit": FIXTURE_SOURCE_COMMIT, "path": path, "sha256": digest(raw)}
        bodies.append({"reference": ref, "base64": base64.b64encode(raw).decode("ascii")})
        return ref

    declaration = load_json(FIXTURES / "writer.declaration.json")
    declaration_ref = snapshot("tests/fixtures/rbac/writer.declaration.json", declaration)
    review_ref = snapshot("tests/fixtures/rbac/writer.review.json", load_json(FIXTURES / "writer.review.json"))
    membership_ref = snapshot("generated:ISO/membership", {"qualification": QUALIFICATION, "readback": values["membership"]})
    defaults_ref = snapshot(
        "generated:ISO/creator-defaults-acl",
        {"qualification": QUALIFICATION, "creators": ["postgres"], "defaults": values["defaults"], "acl": values["acl"]},
    )
    worker_ref = snapshot(
        "generated:ISO/context",
        {"qualification": QUALIFICATION, "context": CONTEXT, "actualWorker": None, "unknownWorker": "REFUSE"},
    )
    definitions: list[Json] = []
    for policy in retained or []:
        definition = copy.deepcopy(mapping(policy))
        definitions.append(
            {
                **definition,
                "definitionSha256": digest(canonical(definition)),
                "source": snapshot("generated:ISO/retained/" + str(definition["name"]), definition),
            }
        )
    envelope: dict[str, Json] = {
        "relation": "editor.qnrs",
        "connection": "editor_app",
        "privileges": ["DELETE", "INSERT", "SELECT", "UPDATE"],
        "columnPrivileges": [],
        "grantOption": False,
        "membershipSource": membership_ref,
        "creatorDefaultsSource": defaults_ref,
        "workerSource": worker_ref,
        "retainedPolicies": definitions,
    }
    source = load_json(FIXTURES / "writer.opendd.json")
    reader = compile_contract(load_json(FIXTURES / "editor.rbac.json"), result)
    rules = writer_rules(replace(reader, source=source), declaration)
    bindings: dict[str, Json] = {key: envelope[key] for key in ("membershipSource", "creatorDefaultsSource", "workerSource", "retainedPolicies")}
    receipt = generation_receipt(rules, source, declaration_ref, review_ref, bindings)
    envelope["writerAuthorizationSource"] = {
        "kind": "COMPILER_GENERATED_WRITER_RLS",
        "ownerDeclarativeSource": declaration_ref,
        "ownerSourceReview": review_ref,
        "compilerSource": snapshot("generated:ISO/compiler-context", compiler_context()),
        "compilerGenerationReceipt": snapshot("generated:ISO/writer-receipt", receipt),
        "generatedWriterPolicySetSha256": receipt["policySetSha256"],
        "commands": ["DELETE", "INSERT", "UPDATE"],
    }
    result["ownerEnvelope"] = envelope
    result["sourceSnapshots"] = bodies
    return mapping(parse_json(canonical(result)))
