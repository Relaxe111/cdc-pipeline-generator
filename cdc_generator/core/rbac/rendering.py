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
                "allow_aggregations": False,
            },
        }
        for rule in contract.rules
    ]


def _drop(contract: Contract) -> list[str]:
    """Remove only compiler-owned policies and column-level SELECT grants."""
    relation = _relation(contract)
    columns = ", ".join(f'"{col}"' for col in contract.rules[0].columns)
    return [f'DROP POLICY "{policy_name(rule)}" ON {relation};' for rule in contract.rules] + [
        f'REVOKE SELECT ({columns}) ON TABLE {relation} FROM "{CONNECTION_ROLE}";'
    ]


def _install(contract: Contract) -> list[str]:
    """Render least-privilege SELECT policies; never grant table writes."""
    relation = _relation(contract)
    sql = [f"ALTER TABLE {relation} ENABLE ROW LEVEL SECURITY;", f"ALTER TABLE {relation} FORCE ROW LEVEL SECURITY;"]
    types = dict(contract.column_types)
    for rule in contract.rules:
        terms = [f"NULLIF(current_setting('app.role', true), '') = '{rule.role}'"]
        for field, variable in rule.comparisons:
            guc = SESSION[variable][1]
            terms.append(f"\"{field}\" = NULLIF(current_setting('{guc}', true), '')::{types[field]}")
        predicate = " AND ".join(terms)
        sql.append(f'CREATE POLICY "{policy_name(rule)}" ON {relation} FOR SELECT TO "{CONNECTION_ROLE}" USING ({predicate});')
    columns = ", ".join(f'"{col}"' for col in contract.rules[0].columns)
    sql.extend(
        [
            f'GRANT USAGE ON SCHEMA "{contract.schema}" TO "{CONNECTION_ROLE}";',
            f'GRANT SELECT ({columns}) ON TABLE {relation} TO "{CONNECTION_ROLE}";',
        ]
    )
    return sql


def _preflight(current: Contract, previous: Contract | None) -> str:
    """Refuse absent/privileged roles, schema drift and unsafe upgrade ACLs."""
    relation = _relation(current)
    role = CONNECTION_ROLE
    policies = [policy_name(rule) for rule in previous.rules] if previous else []
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
  IF has_table_privilege(app, rel, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
      OR has_any_column_privilege(app, rel, 'INSERT,UPDATE,REFERENCES')
      OR EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = rel AND attnum > 0 AND NOT attisdropped
        AND attname <> ALL({permitted}) AND has_column_privilege(app, rel, attname, 'SELECT')) THEN
    RAISE EXCEPTION 'RBAC unexpected broad or write privileges';
  END IF;
END
$rbac$;"""


def render_migration(current: Contract, previous: Contract | None) -> tuple[bytes, bytes]:
    """Create immutable upgrade and conservative rollback SQL with checksums."""
    if previous and (previous.schema, previous.table) != (current.schema, current.table):
        raise ValueError("Changing table identity requires a separate compiler unit")
    fingerprint = digest(canonical({"source": current.source, "catalog": current.catalog}))
    header = f"-- Generated by cdc rbac; contract sha256:{fingerprint}\n"
    up = ["BEGIN;", _preflight(current, previous)]
    if previous:
        up.extend(_drop(previous))
    up.extend(_install(current))
    up.append("COMMIT;")
    down = ["BEGIN;", *_drop(current)]
    if previous:
        down.extend(_install(previous))
    # Keep RLS forced after a fresh rollback: removing a grant must not open rows.
    down.append("COMMIT;")
    return (inject_checksum(header + "\n".join(up) + "\n").encode(), inject_checksum(header + "\n".join(down) + "\n").encode())
