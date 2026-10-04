"""Exact qualified PostgreSQL preflight; retained policies/ACLs are read-only inputs."""

from __future__ import annotations

from cdc_generator.core.rbac.composition_models import Policy
from cdc_generator.core.rbac.composition_readback import ACL_SQL, MEMBERSHIP_SQL, defaults_sql
from cdc_generator.core.rbac.validation import SESSION, Contract, Json, canonical

COMMAND_CODES = {"SELECT": "r", "INSERT": "a", "UPDATE": "w", "DELETE": "d", "ALL": "*"}


def literal(value: str) -> str:
    """Quote exact receipt strings, including separately owned predicate text."""
    return "'" + value.replace("'", "''") + "'"


def deparse(role: str, comparisons: tuple[tuple[str, str], ...]) -> str:
    """The finite PostgreSQL 17 deparse profile is independently checked in ISO tests."""
    terms = [f"(NULLIF(current_setting('app.role'::text, true), ''::text) = '{role}'::text)"]
    terms.extend(f"({field} = (NULLIF(current_setting('{SESSION[variable][1]}'::text, true), ''::text))::uuid)" for field, variable in comparisons)
    return "(" + " AND ".join(terms) + ")"


def owned_definitions(contract: Contract | None) -> tuple[Policy, ...]:
    """Expected old compiler policies are regenerated, never accepted by name alone."""
    if contract is None:
        return ()
    result = [Policy(f"cdc_rbac_{rule.role}_select", "SELECT", True, deparse(rule.role, rule.comparisons), None) for rule in contract.rules]
    if contract.composition:
        result.extend(
            Policy(
                f"cdc_rbac_{rule.role}_{rule.command.lower()}",
                rule.command,
                True,
                deparse(rule.role, rule.using) if rule.using is not None else None,
                deparse(rule.role, rule.check) if rule.check is not None else None,
            )
            for rule in contract.composition.writers
        )
    return tuple(result)


def policy_tuple(policy: Policy) -> dict[str, Json]:
    """Match the exact pg_policy readback keys and roles."""
    return {
        "name": policy.name,
        "command": COMMAND_CODES[policy.command],
        "roles": ["editor_app"],
        "permissive": policy.permissive,
        "using": policy.using,
        "withCheck": policy.check,
    }


def qualified_checks(current: Contract, previous: Contract | None) -> str:
    """Check complete originated/effective rights and definitions before touching owned policies."""
    composition = current.composition
    if composition is None:
        raise ValueError("Qualified checks require a verified composition")
    expected = sorted((*owned_definitions(previous), *composition.retained), key=lambda policy: policy.name)
    definitions = canonical([policy_tuple(policy) for policy in expected]).decode()
    columns = ", ".join(literal(name) for name, _ in current.column_types)
    creators = ", ".join(literal(name) for name in composition.creators)
    return f"""  IF current_database() !~ '^asma8350_writer_[a-f0-9]{{32}}$' THEN
    RAISE EXCEPTION 'RBAC TEST_ONLY_NONOWNER_ISO source has no actual target installation admission';
  END IF;
  IF current_setting('server_version_num')::int / 10000 <> 17 THEN
    RAISE EXCEPTION 'RBAC qualified composition requires the supported PostgreSQL 17 readback profile';
  END IF;
  IF (SELECT jsonb_agg(attname::text ORDER BY attname) FROM pg_attribute
      WHERE attrelid=rel AND attnum>0 AND NOT attisdropped) <> to_jsonb(ARRAY[{columns}]::text[]) THEN
    RAISE EXCEPTION 'RBAC complete writer column catalog mismatch';
  END IF;
  IF ({MEMBERSHIP_SQL}) IS DISTINCT FROM {literal(composition.membership)}::jsonb THEN
    RAISE EXCEPTION 'RBAC membership/role attributes/origins changed';
  END IF;
  IF ({ACL_SQL}) IS DISTINCT FROM {literal(composition.acl)}::jsonb THEN
    RAISE EXCEPTION 'RBAC ACL owner/origins/rights changed';
  END IF;
  IF ({defaults_sql(composition.creators)}) IS DISTINCT FROM {literal(composition.defaults)}::jsonb THEN
    RAISE EXCEPTION 'RBAC creator global/schema default privileges changed';
  END IF;
  IF ARRAY(SELECT rolname::text FROM pg_roles WHERE rolname = ANY(ARRAY[{creators}]::text[]) ORDER BY rolname)
      <> ARRAY[{creators}]::text[] THEN
    RAISE EXCEPTION 'RBAC creator identity missing';
  END IF;
  IF NOT has_schema_privilege(app, 'editor', 'USAGE') OR has_schema_privilege(app, 'editor', 'CREATE')
      OR NOT has_table_privilege(app, rel, 'SELECT') OR NOT has_table_privilege(app, rel, 'INSERT')
      OR NOT has_table_privilege(app, rel, 'UPDATE') OR NOT has_table_privilege(app, rel, 'DELETE')
      OR has_table_privilege(app, rel, 'TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') THEN
    RAISE EXCEPTION 'RBAC effective privileges differ from the finite owner envelope';
  END IF;
  IF (SELECT COALESCE(jsonb_agg(jsonb_build_object('name', p.polname::text, 'command', p.polcmd::text,
      'permissive', p.polpermissive,
      'roles', (SELECT jsonb_agg(CASE WHEN x=0 THEN 'PUBLIC' ELSE pg_get_userbyid(x) END ORDER BY x)
        FROM unnest(p.polroles) x), 'using', pg_get_expr(p.polqual,p.polrelid),
      'withCheck', pg_get_expr(p.polwithcheck,p.polrelid)) ORDER BY p.polname), '[]'::jsonb)
      FROM pg_policy p WHERE p.polrelid=rel) IS DISTINCT FROM {literal(definitions)}::jsonb THEN
    RAISE EXCEPTION 'RBAC exact generated/retained policy definition drift';
  END IF;"""
