"""Generate bounded writer policies exclusively from explicit owning declarations."""

from __future__ import annotations

from cdc_generator.core.rbac.composition_models import Policy, WriterRule
from cdc_generator.core.rbac.validation import MAX_FILTER_DEPTH, ROLES, SESSION, Contract, Json, compile_comparisons, mapping, sequence, string

COMMANDS = {"INSERT": "relationalInsert", "UPDATE": "relationalUpdate", "DELETE": "relationalDelete"}
CONTEXT: dict[str, Json] = {"app.role": list(ROLES), "app.customer_id": "transaction-local UUID", "app.user_id": "transaction-local UUID"}


def writer_rules(contract: Contract, value: Json) -> tuple[WriterRule, ...]:
    """Require explicit role/command/column/old-row/new-row authority and OpenDD opt-ins."""
    source = mapping(value)
    required = {"kind", "qualification", "owner", "relation", "connection", "context", "openddSourceSha256", "rules"}
    if set(source) != required or source["kind"] != "OwnerWriterDeclaration" or source["owner"] != "asma-bunjs-editor":
        raise ValueError("Unqualified owning writer declaration")
    if (contract.schema, contract.table) != ("editor", "qnrs") or source["relation"] != "editor.qnrs" or source["connection"] != "editor_app":
        raise ValueError("Writer declaration must qualify exactly editor.qnrs/editor_app")
    if source["context"] != CONTEXT:
        raise ValueError("Unproved writer context discipline")
    permissions = {
        string(mapping(permission)["role"]): mapping(permission)
        for item in sequence(contract.source)
        if mapping(item)["kind"] == "ModelPermissions"
        for permission in sequence(mapping(mapping(item)["definition"])["permissions"])
    }
    columns = tuple(name for name, _sql_type in contract.column_types)
    result: dict[tuple[str, str], WriterRule] = {}
    for item in sequence(source["rules"]):
        rule = mapping(item)
        if set(rule) != {"role", "command", "columns", "using", "withCheck"}:
            raise ValueError("Every writer command requires explicit columns/USING/WITH CHECK")
        role, command = string(rule["role"]), string(rule["command"])
        if role not in ROLES or command not in COMMANDS or (role, command) in result:
            raise ValueError("Unknown/duplicate writer role/command")
        if permissions.get(role, {}).get(COMMANDS[command]) != {}:
            raise ValueError("Writer rule lacks explicit OpenDD relational command opt-in")
        fields = tuple(sorted(string(field) for field in sequence(rule["columns"])))
        # PostgreSQL policies cannot qualify role-dependent changed-column rights.
        # Refuse instead of pretending that a row policy enforces that authority.
        if fields != (() if command == "DELETE" else columns):
            raise ValueError("Supported writer column envelope requires explicit exact relation columns (DELETE: none)")
        using = _predicate(contract, rule["using"]) if command != "INSERT" else None
        check = _predicate(contract, rule["withCheck"]) if command != "DELETE" else None
        if (command == "INSERT" and rule["using"] is not None) or (command == "DELETE" and rule["withCheck"] is not None):
            raise ValueError("Writer command has inapplicable predicate")
        result[role, command] = WriterRule(role, command, fields, using, check)
    enabled = {(role, command) for role, permission in permissions.items() for command, key in COMMANDS.items() if permission.get(key) == {}}
    if set(result) != enabled or {command for _role, command in result} != set(COMMANDS):
        raise ValueError("Incomplete independently generated writer command coverage")
    return tuple(result[key] for key in sorted(result))


def _predicate(contract: Contract, value: Json) -> tuple[tuple[str, str], ...]:
    """Reuse the existing flat OpenDD tenant/actor comparison compiler."""
    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > MAX_FILTER_DEPTH:
            raise ValueError("Writer filter exceeds flat depth limit")
        obj = mapping(item)
        if set(obj) == {"and"}:
            pending.extend((term, depth + 1) for term in sequence(obj["and"]))
        elif set(obj) != {"fieldComparison"} or set(mapping(obj["fieldComparison"])) != {"field", "operator", "value"}:
            raise ValueError("Unsupported owning writer predicate definition")
    pairs = compile_comparisons(value)
    if ("customer_id", "x-hasura-customer-id") not in pairs or any(dict(contract.column_types).get(field) != "uuid" for field, _ in pairs):
        raise ValueError("Writer predicate requires qualified UUID tenant equality")
    return pairs


def predicate(role: str, pairs: tuple[tuple[str, str], ...]) -> str:
    """Render a trusted role plus declared field/session conjunction."""
    terms = [f"NULLIF(current_setting('app.role', true), '') = '{role}'"]
    terms.extend(f"\"{field}\" = NULLIF(current_setting('{SESSION[variable][1]}', true), '')::uuid" for field, variable in pairs)
    return " AND ".join(terms)


def policies(rules: tuple[WriterRule, ...]) -> tuple[Policy, ...]:
    """Create only compiler-owned writer definitions, never retained witnesses."""
    return tuple(
        Policy(
            f"cdc_rbac_{rule.role}_{rule.command.lower()}",
            rule.command,
            True,
            predicate(rule.role, rule.using) if rule.using is not None else None,
            predicate(rule.role, rule.check) if rule.check is not None else None,
        )
        for rule in rules
    )


def policy_sql(policy: Policy) -> str:
    """Emit a policy without grants, activation, functions or metadata writes."""
    statement = f'CREATE POLICY "{policy.name}" ON "editor"."qnrs" FOR {policy.command} TO "editor_app"'
    if policy.using is not None:
        statement += f" USING ({policy.using})"
    if policy.check is not None:
        statement += f" WITH CHECK ({policy.check})"
    return statement + ";"


def policy_bytes(rules: tuple[WriterRule, ...]) -> bytes:
    """Bind the byte-identical complete generated writer policy set in C1 receipts."""
    return ("\n".join(policy_sql(policy) for policy in policies(rules)) + "\n").encode()
