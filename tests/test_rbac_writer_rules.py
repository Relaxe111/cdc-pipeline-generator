"""Behavioral proofs of explicit writer declarations and flat policy composition."""

from __future__ import annotations

import copy
from dataclasses import replace
from pathlib import Path

import pytest

from cdc_generator.core.rbac.composition_models import Policy
from cdc_generator.core.rbac.policy_semantics import parse, prove_compatible
from cdc_generator.core.rbac.validation import Contract, Json, compile_contract, load_json, mapping, sequence
from cdc_generator.core.rbac.writer_rules import policies, policy_bytes, writer_rules

FIXTURES = Path(__file__).parent / "fixtures/rbac"
TENANT = "customer_id = NULLIF(current_setting('app.customer_id', true), '')::uuid"
ROLE = "NULLIF(current_setting('app.role', true), '') = 'therapist'"


def inputs() -> tuple[Contract, Json]:
    """Use declared test-only OpenDD opt-ins without promoting read permissions."""
    contract = compile_contract(load_json(FIXTURES / "editor.rbac.json"), load_json(FIXTURES / "editor.catalog.json"))
    return replace(contract, source=load_json(FIXTURES / "writer.opendd.json")), load_json(FIXTURES / "writer.declaration.json")


def generated() -> tuple[Policy, ...]:
    """A generated SELECT and independently declared writer set."""
    contract, source = inputs()
    return (Policy("reader", "SELECT", True, f"{ROLE} AND {TENANT}", None), *policies(writer_rules(contract, source)))


def test_explicit_declaration_controls_command_roles_columns_rows() -> None:
    """Only therapist gets explicit I/U/D; actor checks are distinct from its read rule."""
    contract, source = inputs()
    rules = writer_rules(contract, source)
    assert {rule.role for rule in rules} == {"therapist"}
    assert {rule.command for rule in rules} == {"INSERT", "UPDATE", "DELETE"}
    sql = policy_bytes(rules)
    assert sql == policy_bytes(writer_rules(contract, copy.deepcopy(source)))
    assert sql.count(b"CREATE POLICY") == 3
    assert b"app.user_id" in sql and b"GRANT" not in sql and b"ENABLE" not in sql
    assert b"recipient_insert" not in sql and b"super_user_update" not in sql


@pytest.mark.parametrize("mutation", ["missing-command", "duplicate", "columns", "tenant", "context", "reader-only", "unknown-role"])
def test_writer_declaration_refuses_missing_or_guessed_authority(mutation: str) -> None:
    """Each trust boundary is exercised with a syntactically valid changed source."""
    contract, source = inputs()
    obj = mapping(source)
    rules = sequence(obj["rules"])
    if mutation == "missing-command":
        rules.pop()
    elif mutation == "duplicate":
        rules.append(copy.deepcopy(rules[0]))
    elif mutation == "columns":
        mapping(rules[1])["columns"] = ["id"]
    elif mutation == "tenant":
        mapping(rules[1])["withCheck"] = {"and": []}
    elif mutation == "context":
        mapping(obj["context"])["app.role"] = ["admin"]
    elif mutation == "unknown-role":
        mapping(rules[0])["role"] = "admin"
    else:
        contract = replace(contract, source=load_json(FIXTURES / "editor.rbac.json"))
    with pytest.raises(ValueError):
        writer_rules(contract, source)


@pytest.mark.parametrize("command", ["SELECT", "INSERT", "UPDATE", "DELETE", "ALL"])
@pytest.mark.parametrize("permissive", [True, False])
def test_semantic_union_refuses_widening_or_universal_denial(command: str, permissive: bool) -> None:
    """Schema-valid true/false is never a proof of safe permissive/restrictive composition."""
    expression = "true" if permissive else "false"
    retained = Policy(
        "other_owner", command, permissive, None if command == "INSERT" else expression, None if command in {"SELECT", "DELETE"} else expression
    )
    with pytest.raises(ValueError, match=r"widens|removes authorized"):
        prove_compatible(generated(), (retained,))


def test_semantic_compatible_retained_policies_preserve_positives() -> None:
    """Restrictive tenant AND and redundant permissive writer do not change authorization."""
    owned = generated()
    prove_compatible(owned, (Policy("tenant_fence", "ALL", False, TENANT, TENANT), replace(owned[1], name="other_owner")))


@pytest.mark.parametrize(
    "sql", ["id IS NOT NULL", "true OR evil()", "NOT false", "customer_id = 'uuid'", "NULLIF(current_setting('hasura.user', true), '') = 'therapist'"]
)
def test_unknown_sql_context_and_functions_refuse(sql: str) -> None:
    """The compiler never falls back to a general SQL interpreter or policy hash."""
    with pytest.raises(ValueError):
        parse(sql)


def test_pg_get_expr_text_casts_and_boolean_precedence() -> None:
    """Recognize ordinary PostgreSQL deparse syntax with its exact Boolean meaning."""
    sql = (
        "((customer_id = (NULLIF(current_setting('app.customer_id'::text, true), ''::text))::uuid) AND "
        + "(NULLIF(current_setting('app.role'::text, true), ''::text) = 'therapist'::text))"
    )
    parsed = parse(sql)
    assert parsed.accepts(("therapist", True, False))
    assert not parsed.accepts(("therapist", False, True))
    assert not parsed.accepts((None, True, True))
    assert parse(f"false OR {ROLE} AND {TENANT}").accepts(("therapist", True, False))
    assert not parse(f"false OR {ROLE} AND {TENANT}").accepts(("therapist", False, True))
