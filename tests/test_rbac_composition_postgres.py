"""Disclosed fixture ISO: actual nonowner login, active RLS, positive-backed writer oracles."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from typing import cast

import psycopg2
import pytest
from psycopg2.extensions import connection, parse_dsn

from cdc_generator.core.rbac.composition_readback import ACL_SQL, MEMBERSHIP_SQL, defaults_sql
from cdc_generator.core.rbac.rendering import render_migration
from cdc_generator.core.rbac.validation import Contract, Json, compile_contract, load_json, mapping, sequence
from tests.rbac_composition_fixture import FIXTURES, catalog
from tests.test_rbac_postgres import ACTOR_A, ACTOR_B, TENANT_A, TENANT_B

DDL = f"""CREATE SCHEMA editor;
CREATE TABLE editor.qnrs (id uuid PRIMARY KEY, customer_id uuid NOT NULL, user_id uuid, lifecycle_status text NOT NULL);
INSERT INTO editor.qnrs VALUES
 ('00000000-0000-0000-0000-000000000001','{TENANT_A}','{ACTOR_A}','draft'),
 ('00000000-0000-0000-0000-000000000002','{TENANT_A}','{ACTOR_B}','draft'),
 ('00000000-0000-0000-0000-000000000003','{TENANT_B}','{ACTOR_B}','draft');
-- Test-only fixture activation precedes ACL exposure; neither comes from compiler output.
ALTER TABLE editor.qnrs ENABLE ROW LEVEL SECURITY;
ALTER TABLE editor.qnrs FORCE ROW LEVEL SECURITY;
GRANT USAGE ON SCHEMA editor TO editor_app;
GRANT SELECT,INSERT,UPDATE,DELETE ON editor.qnrs TO editor_app;
"""


@pytest.fixture()
def isolated(request: pytest.FixtureRequest) -> Iterator[tuple[connection, connection]]:
    """Create/drop only owned disposable databases; independently authenticate editor_app."""
    dsn = os.environ.get("RBAC_TEST_DSN")
    if not dsn:
        pytest.skip("Set RBAC_TEST_DSN to the explicitly owned disposable fixture")
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    prefix = "asma8350_unqualified_" if getattr(request, "param", None) == "unqualified" else "asma8350_writer_"
    name = prefix + uuid.uuid4().hex
    with admin.cursor() as cursor:
        cursor.execute("ALTER ROLE editor_app LOGIN PASSWORD 'asma8350-fixture-only' NOSUPERUSER NOBYPASSRLS NOINHERIT")
        cursor.execute("CREATE DATABASE " + name)
    config = parse_dsn(dsn)
    config.update({"dbname": name})
    config.pop("options", None)
    owner = psycopg2.connect(**config)
    owner.autocommit = True
    with owner.cursor() as cursor:
        cursor.execute(DDL)
    config.update({"user": "editor_app", "password": "asma8350-fixture-only"})
    config.pop("options", None)
    app = psycopg2.connect(**config)
    app.autocommit = True
    try:
        yield owner, app
    finally:
        app.close()
        owner.close()
        with admin.cursor() as cursor:
            cursor.execute("REVOKE postgres FROM editor_app")
            cursor.execute("ALTER ROLE editor_app LOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT NOCREATEDB")
            cursor.execute("DROP DATABASE " + name + " WITH (FORCE)")
        admin.close()


def readback(db: connection) -> dict[str, Json]:
    """Read actual ACLs/defaults/membership using the same bounded native observations."""
    result: dict[str, Json] = {}
    with db.cursor() as cursor:
        for key, query in [("membership", MEMBERSHIP_SQL), ("acl", ACL_SQL), ("defaults", defaults_sql(("postgres",)))]:
            cursor.execute(query)
            row = cursor.fetchone()
            assert row is not None
            result[key] = cast(Json, row[0])
    return result


def contract(db: connection, *, upgrade: bool = False) -> Contract:
    """Compiler-generated policies from explicit fixture declaration; no handwritten fallback."""
    value = catalog(readback(db))
    if upgrade:
        value["provenance"] = "TEST ONLY immutable upgrade of the same qualified declaration"
    return compile_contract(load_json(FIXTURES / "writer.opendd.json"), value)


def apply(db: connection, sql: bytes) -> None:
    """A failed preflight rolls back before any owned policy changes."""
    with db.cursor() as cursor:
        try:
            cursor.execute(sql.decode())
        except psycopg2.Error:
            cursor.execute("ROLLBACK")
            raise


def inventory(db: connection) -> list[tuple[object, ...]]:
    """Capture all policy definitions and owners' rights to prove failures do not write."""
    with db.cursor() as cursor:
        cursor.execute("""SELECT polname,polcmd,polpermissive,polroles::text,pg_get_expr(polqual,polrelid),pg_get_expr(polwithcheck,polrelid)
            FROM pg_policy WHERE polrelid='editor.qnrs'::regclass ORDER BY polname""")
        return [tuple(row) for row in cursor.fetchall()]


def assert_nonowner(app: connection) -> None:
    """Positive evidence requires the effective/session caller and active RLS, not bootstrap."""
    with app.cursor() as cursor:
        cursor.execute("""SELECT session_user,current_user,r.rolsuper,r.rolbypassrls,pg_has_role(current_user,c.relowner,'MEMBER'),
        c.relrowsecurity,c.relforcerowsecurity FROM pg_roles r CROSS JOIN pg_class c
        WHERE r.rolname=current_user AND c.oid='editor.qnrs'::regclass""")
        assert cursor.fetchone() == ("editor_app", "editor_app", False, False, False, True, True)


def probe(app: connection, query: str, *, role: str | None = "therapist", tenant: str | None = TENANT_A, actor: str | None = ACTOR_A) -> int:
    """Run real unfiltered/filtered commands with transaction-local context and rollback."""
    with app.cursor() as cursor:
        cursor.execute("BEGIN")
        try:
            for key, value in [("app.role", role), ("app.customer_id", tenant), ("app.user_id", actor)]:
                cursor.execute("SELECT set_config(%s,%s,true)", (key, value or ""))
            cursor.execute(query)
            return cursor.rowcount
        finally:
            cursor.execute("ROLLBACK")


def statements(command: str, returning: bool, *, tenant: str = TENANT_A) -> str:
    """Include statements without WHERE/RETURNING; filtered probes are separate cases."""
    if command == "INSERT":
        sql = f"""INSERT INTO editor.qnrs(id,customer_id,user_id,lifecycle_status)
        VALUES ('00000000-0000-0000-0000-000000000099','{tenant}','{ACTOR_A}','new')"""
    elif command == "UPDATE":
        sql = "UPDATE editor.qnrs SET lifecycle_status='written'"
    else:
        sql = "DELETE FROM editor.qnrs"
    if returning:
        if command != "INSERT":
            sql += " WHERE id='00000000-0000-0000-0000-000000000001'"
        sql += " RETURNING id"
    return sql


@pytest.mark.parametrize("upgrade", [False, True])
@pytest.mark.parametrize("command", ["INSERT", "UPDATE", "DELETE"])
@pytest.mark.parametrize("returning", [False, True])
def test_nonowner_active_rls_positive_and_foreign_denial(
    isolated: tuple[connection, connection], upgrade: bool, command: str, returning: bool
) -> None:
    db, app = isolated
    baseline = contract(db)
    before = readback(db)
    apply(db, render_migration(baseline, None)[0])
    if upgrade:
        apply(db, render_migration(contract(db, upgrade=True), baseline)[0])
    assert readback(db) == before
    assert_nonowner(app)
    assert probe(app, statements(command, returning)) == 1
    if command == "INSERT":
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            probe(app, statements(command, returning, tenant=TENANT_B))
    else:
        assert probe(app, statements(command, returning), tenant=TENANT_B, actor=ACTOR_A) == 0


@pytest.mark.parametrize("command", ["INSERT", "UPDATE", "DELETE"])
@pytest.mark.parametrize(
    "context",
    [
        "missing",
        "missing-role",
        "missing-tenant",
        "missing-actor",
        "foreign-actor",
        "unknown-worker",
        "reader-role",
        "super-user-reader",
        "malformed",
        "malformed-actor",
    ],
)
def test_missing_malformed_and_unqualified_context_denials(isolated: tuple[connection, connection], command: str, context: str) -> None:
    db, app = isolated
    apply(db, render_migration(contract(db), None)[0])
    roles = {"missing": None, "missing-role": None, "unknown-worker": "admin", "reader-role": "recipient", "super-user-reader": "super_user"}
    role = roles.get(context, "therapist")
    tenant = None if context in {"missing", "missing-tenant"} else "malformed" if context == "malformed" else TENANT_A
    actor = (
        None if context == "missing-actor" else "malformed" if context == "malformed-actor" else ACTOR_B if context == "foreign-actor" else ACTOR_A
    )
    query = statements(command, context == "foreign-actor")
    if context in {"malformed", "malformed-actor"}:
        with pytest.raises(psycopg2.errors.InvalidTextRepresentation):
            probe(app, query, role=role, tenant=tenant, actor=actor)
    elif command == "INSERT":
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            probe(app, query, role=role, tenant=tenant, actor=actor)
    else:
        assert probe(app, query, role=role, tenant=tenant, actor=actor) == 0


@pytest.mark.parametrize("upgrade", [False, True])
@pytest.mark.parametrize(
    "mutation",
    [
        "missing-SELECT",
        "missing-INSERT",
        "missing-UPDATE",
        "missing-DELETE",
        "TRUNCATE",
        "REFERENCES",
        "TRIGGER",
        "MAINTAIN",
        "grant-option",
        "column",
        "public-origin",
        "defaults",
        "membership",
        "owner",
        "role-inherit",
        "role-createdb",
        "schema-usage",
        "schema-create",
    ],
)
def test_exact_acl_creator_membership_refusal_before_writes(isolated: tuple[connection, connection], upgrade: bool, mutation: str) -> None:
    db, _app = isolated
    baseline = contract(db)
    previous = baseline if upgrade else None
    if previous:
        apply(db, render_migration(baseline, None)[0])
    current = contract(db, upgrade=True)
    with db.cursor() as cursor:
        if mutation.startswith("missing-"):
            cursor.execute(f"REVOKE {mutation[8:]} ON editor.qnrs FROM editor_app")
        elif mutation in {"TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"}:
            cursor.execute(f"GRANT {mutation} ON editor.qnrs TO editor_app")
        elif mutation == "grant-option":
            cursor.execute("GRANT UPDATE ON editor.qnrs TO editor_app WITH GRANT OPTION")
        elif mutation == "column":
            cursor.execute("GRANT UPDATE(id) ON editor.qnrs TO editor_app")
        elif mutation == "public-origin":
            cursor.execute("REVOKE INSERT ON editor.qnrs FROM editor_app; GRANT INSERT ON editor.qnrs TO PUBLIC")
        elif mutation == "defaults":
            cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA editor GRANT SELECT ON TABLES TO editor_app")
        elif mutation == "membership":
            cursor.execute("GRANT postgres TO editor_app")
        elif mutation == "role-inherit":
            cursor.execute("ALTER ROLE editor_app INHERIT")
        elif mutation == "role-createdb":
            cursor.execute("ALTER ROLE editor_app CREATEDB")
        elif mutation == "schema-usage":
            cursor.execute("REVOKE USAGE ON SCHEMA editor FROM editor_app")
        elif mutation == "schema-create":
            cursor.execute("GRANT CREATE ON SCHEMA editor TO editor_app")
        else:
            cursor.execute("ALTER TABLE editor.qnrs OWNER TO editor_app")
    before = inventory(db), readback(db)
    with pytest.raises(psycopg2.Error, match=r"RBAC.*(changed|privileged|nonowner)"):
        apply(db, render_migration(current, previous)[0])
    assert (inventory(db), readback(db)) == before


@pytest.mark.parametrize("command", ["SELECT", "INSERT", "UPDATE", "DELETE", "ALL"])
def test_retained_permissive_union_refuses_and_unfiltered_probe_exposes_hazard(isolated: tuple[connection, connection], command: str) -> None:
    db, app = isolated
    apply(db, render_migration(contract(db), None)[0])
    using = " USING (true)" if command != "INSERT" else ""
    check = " WITH CHECK (true)" if command not in {"SELECT", "DELETE"} else ""
    with db.cursor() as cursor:
        # Seeded hostile other-owner policy, never compiler product output.
        cursor.execute(f"CREATE POLICY other_owner ON editor.qnrs FOR {command} TO editor_app{using}{check}")
        cursor.execute("""SELECT pg_get_expr(polqual,polrelid),pg_get_expr(polwithcheck,polrelid) FROM pg_policy
            WHERE polname='other_owner' AND polrelid='editor.qnrs'::regclass""")
        definition = cursor.fetchone()
        assert definition is not None
    retained: list[Json] = [
        {"name": "other_owner", "command": command, "roles": ["editor_app"], "permissive": True, "using": definition[0], "withCheck": definition[1]}
    ]
    before = inventory(db)
    with pytest.raises(ValueError, match="widens generated authorization"):
        compile_contract(load_json(FIXTURES / "writer.opendd.json"), catalog(readback(db), retained))
    assert inventory(db) == before
    # WHERE/RETURNING can consult SELECT and mask the write widening.
    if command in {"UPDATE", "DELETE"}:
        assert probe(app, statements(command, False), role=None, tenant=None, actor=None) == 3
        assert probe(app, statements(command, True), role=None, tenant=None, actor=None) == 0
    elif command == "INSERT":
        assert probe(app, statements(command, False, tenant=TENANT_B), role=None, tenant=None, actor=None) == 1


def test_compatible_retained_policy_preserved_and_upgrade_down(isolated: tuple[connection, connection]) -> None:
    db, app = isolated
    with db.cursor() as cursor:
        cursor.execute("""CREATE POLICY other_owner ON editor.qnrs AS RESTRICTIVE FOR ALL TO editor_app
            USING (customer_id = NULLIF(current_setting('app.customer_id',true),'')::uuid)
            WITH CHECK (customer_id = NULLIF(current_setting('app.customer_id',true),'')::uuid)""")
        cursor.execute("""SELECT pg_get_expr(polqual,polrelid),pg_get_expr(polwithcheck,polrelid) FROM pg_policy
            WHERE polname='other_owner' AND polrelid='editor.qnrs'::regclass""")
        expressions = cursor.fetchone()
        assert expressions is not None
    retained: list[Json] = [
        {"name": "other_owner", "command": "ALL", "roles": ["editor_app"], "permissive": False, "using": expressions[0], "withCheck": expressions[1]}
    ]
    value = catalog(readback(db), retained)
    source = load_json(FIXTURES / "writer.opendd.json")
    baseline = compile_contract(source, value)
    original = inventory(db)
    apply(db, render_migration(baseline, None)[0])
    assert original[0] in inventory(db)
    assert probe(app, statements("UPDATE", False)) == 1
    value["provenance"] = "TEST ONLY upgrade"
    upgraded = compile_contract(source, value)
    up, down = render_migration(upgraded, baseline)
    before = inventory(db), readback(db)
    apply(db, up)
    apply(db, down)
    assert (inventory(db), readback(db)) == before
    assert probe(app, statements("UPDATE", False)) == 1
    with pytest.raises(psycopg2.Error, match="lacks qualified predecessor"):
        apply(db, render_migration(baseline, None)[1])
    assert (inventory(db), readback(db)) == before


@pytest.mark.parametrize("returning", [False, True])
def test_tenant_change_and_trigger_final_row_fence(isolated: tuple[connection, connection], returning: bool) -> None:
    db, app = isolated
    apply(db, render_migration(contract(db), None)[0])
    with db.cursor() as cursor:
        # Test-only invoker trigger, no guard-owner, definer or extra ACL substitute.
        cursor.execute(f"""CREATE FUNCTION editor.fixture_change() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN NEW.customer_id := '{TENANT_B}'::uuid; RETURN NEW; END $$;
            CREATE TRIGGER fixture_change BEFORE UPDATE ON editor.qnrs FOR EACH ROW EXECUTE FUNCTION editor.fixture_change()""")
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        probe(app, statements("UPDATE", returning))
    with db.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM editor.qnrs WHERE lifecycle_status='draft'")
        assert cursor.fetchone() == (3,)


def test_committed_nonowner_write_and_local_context_cleanup(isolated: tuple[connection, connection]) -> None:
    db, app = isolated
    apply(db, render_migration(contract(db), None)[0])
    with app.cursor() as cursor:
        cursor.execute("BEGIN")
        for key, value in [("app.role", "therapist"), ("app.customer_id", TENANT_A), ("app.user_id", ACTOR_A)]:
            cursor.execute("SELECT set_config(%s,%s,true)", (key, value))
        cursor.execute("UPDATE editor.qnrs SET lifecycle_status='committed'")
        assert cursor.rowcount == 1
        cursor.execute("COMMIT")
        cursor.execute("SELECT current_setting('app.role',true),current_setting('app.customer_id',true),current_setting('app.user_id',true)")
        assert cursor.fetchone() == ("", "", "")
        cursor.execute("SELECT count(*) FROM editor.qnrs")
        assert cursor.fetchone() == (0,)
    with db.cursor() as cursor:
        cursor.execute("SELECT customer_id::text,user_id::text FROM editor.qnrs WHERE lifecycle_status='committed'")
        assert cursor.fetchall() == [(TENANT_A, ACTOR_A)]


@pytest.mark.parametrize("upgrade", [False, True])
@pytest.mark.parametrize("mutation", ["missing", "extra", "role", "command", "permissive", "using", "check", "null-check"])
def test_retained_exact_definition_each_field_refuses_before_writes(isolated: tuple[connection, connection], upgrade: bool, mutation: str) -> None:
    db, _app = isolated
    with db.cursor() as cursor:
        cursor.execute("""CREATE POLICY other_owner ON editor.qnrs AS RESTRICTIVE FOR ALL TO editor_app
        USING (customer_id = NULLIF(current_setting('app.customer_id',true),'')::uuid)
        WITH CHECK (customer_id = NULLIF(current_setting('app.customer_id',true),'')::uuid)""")
        cursor.execute("""SELECT pg_get_expr(polqual,polrelid),pg_get_expr(polwithcheck,polrelid)
        FROM pg_policy WHERE polname='other_owner' AND polrelid='editor.qnrs'::regclass""")
        expressions = cursor.fetchone()
        assert expressions is not None
    retained: list[Json] = [
        {"name": "other_owner", "command": "ALL", "roles": ["editor_app"], "permissive": False, "using": expressions[0], "withCheck": expressions[1]}
    ]
    source = load_json(FIXTURES / "writer.opendd.json")
    current = compile_contract(source, catalog(readback(db), retained))
    previous = current if upgrade else None
    if previous:
        apply(db, render_migration(previous, None)[0])
    with db.cursor() as cursor:
        if mutation == "missing":
            cursor.execute("DROP POLICY other_owner ON editor.qnrs")
        elif mutation == "extra":
            cursor.execute("CREATE POLICY surprise ON editor.qnrs FOR SELECT TO editor_app USING (true)")
        elif mutation == "role":
            cursor.execute("ALTER POLICY other_owner ON editor.qnrs TO PUBLIC")
        elif mutation == "using":
            cursor.execute("ALTER POLICY other_owner ON editor.qnrs USING (true)")
        elif mutation == "check":
            cursor.execute("ALTER POLICY other_owner ON editor.qnrs WITH CHECK (true)")
        else:
            cursor.execute("DROP POLICY other_owner ON editor.qnrs")
            mode = "AS PERMISSIVE" if mutation == "permissive" else "AS RESTRICTIVE"
            command = "FOR SELECT" if mutation == "command" else "FOR ALL"
            check = "" if mutation in {"command", "null-check"} else " WITH CHECK (true)"
            cursor.execute(f"CREATE POLICY other_owner ON editor.qnrs {mode} {command} TO editor_app USING (true){check}")
    before = inventory(db), readback(db)
    with pytest.raises(psycopg2.Error, match=r"RBAC.*policy"):
        apply(db, render_migration(current, previous)[0])
    assert (inventory(db), readback(db)) == before


@pytest.mark.parametrize("mutation", ["reader-using", "writer-using", "writer-check", "writer-role"])
def test_owned_policy_changed_definitions_refuse_upgrade(isolated: tuple[connection, connection], mutation: str) -> None:
    db, _app = isolated
    previous = contract(db)
    apply(db, render_migration(previous, None)[0])
    current = contract(db, upgrade=True)
    sql = {
        "reader-using": "ALTER POLICY cdc_rbac_therapist_select ON editor.qnrs USING (true)",
        "writer-using": "ALTER POLICY cdc_rbac_therapist_update ON editor.qnrs USING (true)",
        "writer-check": "ALTER POLICY cdc_rbac_therapist_update ON editor.qnrs WITH CHECK (true)",
        "writer-role": "ALTER POLICY cdc_rbac_therapist_insert ON editor.qnrs TO PUBLIC",
    }[mutation]
    with db.cursor() as cursor:
        cursor.execute(sql)
    before = inventory(db), readback(db)
    with pytest.raises(psycopg2.Error, match="exact generated/retained policy definition drift"):
        apply(db, render_migration(current, previous)[0])
    assert (inventory(db), readback(db)) == before


def full_table(db: connection) -> None:
    """Test-only 19-column catalog shape; not the 1177-migration guard/constraint chain."""
    columns = sequence(mapping(load_json(FIXTURES / "writer-full.catalog.json"))["columns"])
    defaults = {"uuid": f"'{ACTOR_A}'", "text": "''", "int4": "1", "date": "'2026-10-04'", "timestamptz": "'2026-10-04 00:00:00+00'"}
    declarations: list[str] = []
    for item in columns:
        column = mapping(item)
        name, sql_type = str(column["name"]), str(column["type"])
        definition = f'"{name}" {sql_type}'
        if column["nullable"] is False:
            definition += " NOT NULL"
        if name not in {"id", "customer_id", "user_id"}:
            definition += " DEFAULT " + defaults[sql_type]
        if name == "id":
            definition += " PRIMARY KEY"
        declarations.append(definition)
    with db.cursor() as cursor:
        cursor.execute('DROP TABLE "editor"."qnrs"')
        cursor.execute('CREATE TABLE "editor"."qnrs" (' + ",".join(declarations) + ")")
        cursor.execute(f"""INSERT INTO editor.qnrs(id,customer_id,user_id,lifecycle_status) VALUES
        ('00000000-0000-0000-0000-000000000001','{TENANT_A}','{ACTOR_A}','draft'),
        ('00000000-0000-0000-0000-000000000002','{TENANT_A}','{ACTOR_B}','draft'),
        ('00000000-0000-0000-0000-000000000003','{TENANT_B}','{ACTOR_B}','draft');
        ALTER TABLE editor.qnrs ENABLE ROW LEVEL SECURITY; ALTER TABLE editor.qnrs FORCE ROW LEVEL SECURITY;
        GRANT SELECT,INSERT,UPDATE,DELETE ON editor.qnrs TO editor_app;""")


@pytest.mark.parametrize("upgrade", [False, True])
@pytest.mark.parametrize("command", ["INSERT", "UPDATE", "DELETE"])
@pytest.mark.parametrize("returning", [False, True])
def test_full_19_column_source_nonowner_positive_and_negative(
    isolated: tuple[connection, connection], upgrade: bool, command: str, returning: bool
) -> None:
    db, app = isolated
    full_table(db)
    source = load_json(FIXTURES / "writer-full.opendd.json")
    value = catalog(readback(db), full=True)
    baseline = compile_contract(source, value)
    assert len(baseline.column_types) == 19
    before = readback(db)
    apply(db, render_migration(baseline, None)[0])
    if upgrade:
        value["provenance"] = "TEST ONLY full-table upgrade"
        apply(db, render_migration(compile_contract(source, value), baseline)[0])
    assert before == readback(db)
    assert_nonowner(app)
    assert probe(app, "SELECT template_version,valid_from,created_at FROM editor.qnrs") == 2
    assert probe(app, statements(command, returning)) == 1
    if command == "INSERT":
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            probe(app, statements(command, returning, tenant=TENANT_B))
    else:
        assert probe(app, statements(command, returning), tenant=TENANT_B) == 0


@pytest.mark.parametrize("column", ["customer_id", "user_id"])
@pytest.mark.parametrize("returning", [False, True])
def test_direct_tenant_actor_change_denied_without_select_mask(isolated: tuple[connection, connection], column: str, returning: bool) -> None:
    db, app = isolated
    apply(db, render_migration(contract(db), None)[0])
    changed = TENANT_B if column == "customer_id" else ACTOR_B
    sql = f"UPDATE editor.qnrs SET {column}='{changed}'"
    if returning:
        sql += " WHERE id='00000000-0000-0000-0000-000000000001' RETURNING id"
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        probe(app, sql)
    assert probe(app, statements("UPDATE", returning)) == 1


@pytest.mark.parametrize("isolated", ["unqualified"], indirect=True)
def test_test_only_source_never_admits_other_database_installation(isolated: tuple[connection, connection]) -> None:
    """Exercise the refusal in another owned disposable database, never an actual target."""
    db, app = isolated
    current = contract(db)
    before = inventory(db), readback(db)
    with pytest.raises(psycopg2.Error, match="no actual target installation admission"):
        apply(db, render_migration(current, None)[0])
    assert (inventory(db), readback(db)) == before
    assert_nonowner(app)
    assert probe(app, "SELECT * FROM editor.qnrs") == 0
