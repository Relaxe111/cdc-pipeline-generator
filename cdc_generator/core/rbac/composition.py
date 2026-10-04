"""Reviewed finite owner envelope and source-bound generated writer compatibility."""

from __future__ import annotations

import base64
import binascii
import re
from typing import cast

from jsonschema import Draft202012Validator, ValidationError, validate

from cdc_generator.core.rbac.composition_models import Composition, Policy, WriterRule
from cdc_generator.core.rbac.composition_readback import validate_defaults, validate_snapshots
from cdc_generator.core.rbac.policy_semantics import prove_compatible
from cdc_generator.core.rbac.validation import ASSETS, Contract, Json, canonical, digest, identifier, load_json, mapping, parse_json, sequence, string
from cdc_generator.core.rbac.writer_rules import CONTEXT, policies, policy_bytes, predicate, writer_rules

# There is deliberately no admitted actual owner declaration/review. These pins
# admit only the disclosed fixture authored in this source PR, never a target.
FIXTURE_DECLARATION = "0c78d73237c183488b0a6aca183194cee9d67423d68eb8be25654086c554878e"
FIXTURE_REVIEW = "26d056f7bc44d51e0d5913db50eb367f7dde1fc14a5584424be835cfe7222d06"
QUALIFICATION = "TEST_ONLY_NONOWNER_ISO"
ENVELOPE_SCHEMA_SHA256 = "e0254d2536357090b90189045859a334056718b3c476b0b3c6082470f6f5e4f8"
FIXTURE_SOURCE_COMMIT = "cc3aa1c9ab0606a22749eab109699614c6dda145"
FULL_FIXTURE_SOURCE_COMMIT = "2a8cf55de3966d09deb4c07ee6ee91a9c10fddf3"
FULL_FIXTURE_DECLARATION = "7a8da29dc25bf98c9a0984673e43cafe72150e19e0725f3425014bb8d59e4a99"
FULL_FIXTURE_REVIEW = "262d79d29df873b602fd532f95bbfdfeeb61e74cc8600e2ed20b0515ec146422"


class Sources:
    """Resolve only exact offline snapshots; refuse network/path reads and unused sources."""

    def __init__(self, value: Json) -> None:
        self.bodies: dict[bytes, bytes] = {}
        self.used: set[bytes] = set()
        for item in sequence(value):
            snapshot = mapping(item)
            if set(snapshot) != {"reference", "base64"}:
                raise ValueError("Invalid source snapshot receipt")
            ref = mapping(snapshot["reference"])
            if set(ref) != {"commit", "path", "sha256"} or not re.fullmatch(r"[a-f0-9]{40}", string(ref["commit"])):
                raise ValueError("Invalid immutable source reference")
            if not string(ref["path"]).strip() or not re.fullmatch(r"[a-f0-9]{64}", string(ref["sha256"])):
                raise ValueError("Missing exact source path/hash")
            try:
                raw = base64.b64decode(string(snapshot["base64"]), validate=True)
            except binascii.Error as error:
                raise ValueError("Malformed source byte receipt") from error
            key = canonical(ref)
            if key in self.bodies or digest(raw) != ref["sha256"]:
                raise ValueError("Duplicate or changed source snapshot bytes")
            self.bodies[key] = raw

    def resolve(self, reference: Json) -> Json:
        """Hash/body equality establishes identity, not owning review authority."""
        key = canonical(reference)
        if key not in self.bodies:
            raise ValueError("Absent referenced source receipt")
        self.used.add(key)
        return parse_json(self.bodies[key])


def compiler_context() -> dict[str, Json]:
    """Reuse the existing implementation and exact offline validator fingerprints."""
    from cdc_generator.core.rbac.provenance import COMPILER, implementation
    from cdc_generator.core.rbac.validation import doctor

    return {"compiler": COMPILER, "implementation": implementation(), "pins": cast(dict[str, Json], doctor())}


def generation_receipt(rules: tuple[WriterRule, ...], source: Json, declaration_ref: Json, review_ref: Json, bindings: Json) -> dict[str, Json]:
    """The same writer renderer used by cdc rbac generates the whole byte receipt."""
    output = policy_bytes(rules)
    return {
        "kind": "COMPILER_GENERATED_WRITER_RLS",
        "qualification": QUALIFICATION,
        "compilerContext": compiler_context(),
        "openddSha256": digest(canonical(source)),
        "ownerDeclarativeSource": declaration_ref,
        "ownerSourceReview": review_ref,
        "sourceBindings": bindings,
        "context": CONTEXT,
        "commands": ["DELETE", "INSERT", "UPDATE"],
        "policySetSha256": digest(output),
        "policySetBase64": base64.b64encode(output).decode("ascii"),
    }


def _writer_binding(contract: Contract, envelope: dict[str, Json], sources: Sources, reviewed_contexts: tuple[Json, ...]) -> tuple[WriterRule, ...]:
    """C1 refuses arbitrary reviews/SQL witnesses and regenerates every writer byte."""
    authorization = mapping(envelope["writerAuthorizationSource"])
    declaration_ref, review_ref = authorization["ownerDeclarativeSource"], authorization["ownerSourceReview"]
    declaration, review = mapping(sources.resolve(declaration_ref)), mapping(sources.resolve(review_ref))
    admitted = False
    for commit, prefix, declaration_hash, review_hash in [
        (FIXTURE_SOURCE_COMMIT, "writer", FIXTURE_DECLARATION, FIXTURE_REVIEW),
        (FULL_FIXTURE_SOURCE_COMMIT, "writer-full", FULL_FIXTURE_DECLARATION, FULL_FIXTURE_REVIEW),
    ]:
        expected_declaration = {"commit": commit, "path": f"tests/fixtures/rbac/{prefix}.declaration.json", "sha256": declaration_hash}
        expected_review = {"commit": commit, "path": f"tests/fixtures/rbac/{prefix}.review.json", "sha256": review_hash}
        admitted |= declaration_ref == expected_declaration and review_ref == expected_review
    if not admitted:
        raise ValueError("Actual owning declaration/review not admitted; arbitrary references never establish writer authority")
    if declaration.get("qualification") != QUALIFICATION or review.get("qualification") != QUALIFICATION:
        raise ValueError("Fixture admission cannot qualify actual owner/worker rights")
    if review.get("declarationSha256") != digest(canonical(declaration)) or declaration["openddSourceSha256"] != digest(canonical(contract.source)):
        raise ValueError("Owning review/OpenDD declaration binding mismatch")
    rules = writer_rules(contract, declaration)
    context = sources.resolve(authorization["compilerSource"])
    if context not in (compiler_context(), *reviewed_contexts):
        raise ValueError("Unreviewed compiler source context")
    bindings: dict[str, Json] = {key: envelope[key] for key in ("membershipSource", "creatorDefaultsSource", "workerSource")}
    bindings["retainedPolicies"] = envelope["retainedPolicies"]
    expected = generation_receipt(rules, contract.source, declaration_ref, review_ref, bindings)
    expected["compilerContext"] = context
    receipt = sources.resolve(authorization["compilerGenerationReceipt"])
    if receipt != expected or authorization["generatedWriterPolicySetSha256"] != digest(policy_bytes(rules)):
        raise ValueError("Generated writer provenance/byte-identical regeneration mismatch")
    return rules


def _retained(envelope: dict[str, Json], sources: Sources) -> tuple[Policy, ...]:
    """Preserve exact definitions; do not manufacture WITH CHECK fallback or source tuples."""
    result: dict[str, Policy] = {}
    for item in sequence(envelope["retainedPolicies"]):
        entry = mapping(item)
        name = identifier(entry["name"])
        if name.startswith("cdc_rbac_") or name in result:
            raise ValueError("Retained policy overlaps compiler ownership or repeats a name")
        definition = {key: entry[key] for key in ("name", "command", "roles", "permissive", "using", "withCheck")}
        if entry["definitionSha256"] != digest(canonical(definition)) or sources.resolve(entry["source"]) != definition:
            raise ValueError("Retained policy definition/source mismatch")
        result[name] = Policy(name, string(entry["command"]), entry["permissive"] is True, _optional(entry["using"]), _optional(entry["withCheck"]))
    return tuple(result[name] for name in sorted(result))


def _optional(value: Json) -> str | None:
    """Permit only the nullable string already admitted by the exact attachment schema."""
    return None if value is None else string(value)


def compile_composition(contract: Contract, reviewed_contexts: tuple[Json, ...] = ()) -> Composition:
    """All source, ACL and semantic refusals precede any artifact/database write."""
    catalog = mapping(contract.catalog)
    envelope = mapping(catalog["ownerEnvelope"])
    schema_path = ASSETS / "owner-envelope.schema.json"
    if digest(schema_path.read_bytes()) != ENVELOPE_SCHEMA_SHA256:
        raise ValueError("Reviewed owner-envelope schema drift")
    schema = mapping(load_json(schema_path))
    try:
        validate(envelope, schema, cls=Draft202012Validator)
    except ValidationError as error:
        raise ValueError(f"Owner envelope schema clause {error.json_path}: {error.message}") from error
    if (contract.schema, contract.table) != ("editor", "qnrs"):
        raise ValueError("Composition is bounded to editor.qnrs")
    columns = tuple(name for name, _ in contract.column_types)
    if any(rule.columns != columns for rule in contract.rules):
        raise ValueError("Table SELECT envelope cannot enforce a partial reader column projection")
    sources = Sources(catalog.get("sourceSnapshots"))
    writers = _writer_binding(contract, envelope, sources, reviewed_contexts)
    retained = _retained(envelope, sources)
    readers = tuple(Policy(f"cdc_rbac_{rule.role}_select", "SELECT", True, predicate(rule.role, rule.comparisons), None) for rule in contract.rules)
    prove_compatible((*readers, *policies(writers)), retained)
    membership = mapping(sources.resolve(envelope["membershipSource"]))
    defaults = mapping(sources.resolve(envelope["creatorDefaultsSource"]))
    worker = mapping(sources.resolve(envelope["workerSource"]))
    if set(membership) != {"qualification", "readback"} or membership["qualification"] != QUALIFICATION:
        raise ValueError("Unqualified membership source")
    if set(defaults) != {"qualification", "creators", "defaults", "acl"} or defaults["qualification"] != QUALIFICATION:
        raise ValueError("Unqualified ACL/creator-default source")
    creators = tuple(identifier(creator) for creator in sequence(defaults["creators"]))
    if not creators or tuple(sorted(set(creators))) != creators:
        raise ValueError("Exact nonempty creator identities required")
    if worker != {"qualification": QUALIFICATION, "context": CONTEXT, "actualWorker": None, "unknownWorker": "REFUSE"}:
        raise ValueError("Unproved worker/context source")
    validate_snapshots(membership["readback"], defaults["acl"], creators)
    validate_defaults(defaults["defaults"], membership["readback"], creators)
    if sources.used != set(sources.bodies):
        raise ValueError("Unbound source snapshots")
    return Composition(
        writers,
        retained,
        canonical(membership["readback"]).decode(),
        canonical(defaults["defaults"]).decode(),
        canonical(defaults["acl"]).decode(),
        creators,
    )
