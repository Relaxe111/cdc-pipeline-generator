"""Deterministic SQL and Hasura SELECT projections of the same flat rules."""

from __future__ import annotations

from cdc_generator.core.migration_generator.file_writers import inject_checksum
from cdc_generator.core.rbac.validation import SESSION, Contract, Json, Rule, canonical, digest

CONNECTION_ROLE = "editor_app"


def policy_name(rule: Rule) -> str:
    """Name a policy in the compiler-owned namespace on this one table."""
    return f"cdc_rbac_{rule.role}_select"


def _relation(contract: Contract) -> str:
    """Quote catalog-validated PostgreSQL relation names."""
    return f'"{contract.schema}"."{contract.table}"'


def select_permissions(contract: Contract) -> list[Json]:
    """Emit only SELECT, with the identical session comparison conjunction."""
    return [
        {
            "role": rule.role,
            "permission": {
                "columns": list(rule.columns),
                "filter": {"_and": [{field: {"_eq": variable}} for field, variable in rule.comparisons]},
            },
        }
        for rule in contract.rules
    ]


def _drop(contract: Contract) -> list[str]:
    """Remove only compiler-owned policies; leave separately owned ACLs intact."""
    relation = _relation(contract)
    names = [policy_name(rule) for rule in contract.rules]
    if contract.composition:
        from cdc_generator.core.rbac.writer_rules import policies

        names.extend(policy.name for policy in policies(contract.composition.writers))
    return [f'DROP POLICY "{name}" ON {relation};' for name in names]


def _install(contract: Contract) -> list[str]:
    """Prepare SELECT policies without changing ACLs or RLS activation."""
    relation = _relation(contract)
    sql: list[str] = []
    types = dict(contract.column_types)
    for rule in contract.rules:
        terms = [f"NULLIF(current_setting('app.role', true), '') = '{rule.role}'"]
        for field, variable in rule.comparisons:
            guc = SESSION[variable][1]
            terms.append(f"\"{field}\" = NULLIF(current_setting('{guc}', true), '')::{types[field]}")
        predicate = " AND ".join(terms)
        sql.append(f'CREATE POLICY "{policy_name(rule)}" ON {relation} FOR SELECT TO "{CONNECTION_ROLE}" USING ({predicate});')
    if contract.composition:
        from cdc_generator.core.rbac.writer_rules import policies, policy_sql

        sql.extend(policy_sql(policy) for policy in policies(contract.composition.writers))
    return sql


def _preflight(current: Contract, previous: Contract | None) -> str:
    """Refuse absent/privileged roles, schema drift and unsafe upgrade ACLs."""
    relation = _relation(current)
    role = CONNECTION_ROLE
    policies = [policy_name(rule) for rule in previous.rules] if previous else []
    if current.composition:
        from cdc_generator.core.rbac.composition_sql import owned_definitions

        policies = sorted([policy.name for policy in owned_definitions(previous)] + [policy.name for policy in current.composition.retained])
    names = ", ".join(f"'{name}'" for name in policies)
    expected = f"ARRAY[{names}]::text[]"
    columns = sorted(set(current.rules[0].columns) | (set(previous.rules[0].columns) if previous else set()))
    permitted = "ARRAY[" + ", ".join(f"'{col}'" for col in columns) + "]::text[]"
    checks: list[str] = []
    for name, sql_type in current.column_types:
        checks.append(f"""  IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = rel AND attname = '{name}'
      AND NOT attisdropped AND atttypid = '{sql_type}'::regtype) THEN
    RAISE EXCEPTION 'RBAC catalog column/type mismatch: {name}';
  END IF;""")
    privilege_checks = f"""  IF has_table_privilege(app, rel, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
      OR has_any_column_privilege(app, rel, 'INSERT,UPDATE,REFERENCES')
      OR EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = rel AND attnum > 0 AND NOT attisdropped
        AND attname <> ALL({permitted}) AND has_column_privilege(app, rel, attname, 'SELECT')) THEN
    RAISE EXCEPTION 'RBAC unexpected broad or write privileges';
  END IF;"""
    if current.composition:
        from cdc_generator.core.rbac.composition_sql import qualified_checks

        privilege_checks = qualified_checks(current, previous)
    return f"""DO $rbac$
DECLARE
  rel oid := to_regclass('{relation}');
  app oid := (SELECT oid FROM pg_roles WHERE rolname = '{role}');
  existing text[];
BEGIN
  IF rel IS NULL THEN RAISE EXCEPTION 'RBAC table missing: {relation}'; END IF;
  IF app IS NULL THEN RAISE EXCEPTION 'RBAC role missing: {role}'; END IF;
  IF EXISTS (SELECT 1 FROM pg_roles r WHERE pg_has_role(app, r.oid, 'MEMBER')
      AND (r.rolsuper OR r.rolbypassrls)) THEN
    RAISE EXCEPTION 'RBAC connection role has privileged membership';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_class WHERE oid = rel AND (relkind <> 'r' OR pg_has_role(app, relowner, 'MEMBER'))) THEN
    RAISE EXCEPTION 'RBAC connection must be a nonowner on an ordinary table';
  END IF;
{chr(10).join(checks)}
  SELECT COALESCE(array_agg(polname::text ORDER BY polname), ARRAY[]::text[]) INTO existing
    FROM pg_policy WHERE polrelid = rel;
  IF existing <> {expected} THEN
    RAISE EXCEPTION 'RBAC unmanaged or missing policy';
  END IF;
{privilege_checks}
END
$rbac$;"""


def render_migration(current: Contract, previous: Contract | None) -> tuple[bytes, bytes]:
    """Create immutable upgrade and conservative rollback SQL with checksums."""
    if previous and (previous.schema, previous.table) != (current.schema, current.table):
        raise ValueError("Changing table identity requires a separate compiler unit")
    if previous and previous.composition and current.composition is None:
        raise ValueError("Removing qualified generated writer fences is prohibited")
    fingerprint = digest(canonical({"source": current.source, "catalog": current.catalog}))
    header = f"-- Generated by cdc rbac; contract sha256:{fingerprint}\n"
    lock = ['LOCK TABLE "editor"."qnrs" IN ACCESS EXCLUSIVE MODE;'] if current.composition else []
    up = ["BEGIN;", *lock, _preflight(current, previous)]
    if previous:
        up.extend(_drop(previous))
    up.extend(_install(current))
    up.append("COMMIT;")
    down = ["BEGIN;"]
    if current.composition:
        if previous is None or previous.composition is None:
            down.append("DO $rbac$ BEGIN RAISE EXCEPTION 'RBAC rollback lacks qualified predecessor writer fences; no writes'; END $rbac$;")
        else:
            down.extend([*lock, _preflight(previous, current), *_drop(current), *_install(previous)])
    else:
        down.extend(_drop(current))
        if previous:
            down.extend(_install(previous))
    down.append("COMMIT;")
    return (inject_checksum(header + "\n".join(up) + "\n").encode(), inject_checksum(header + "\n".join(down) + "\n").encode())
