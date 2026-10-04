"""Execute generated fresh/upgrade SQL against isolated PostgreSQL and Hasura."""

from __future__ import annotations

import copy
import json
import os
import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path
from urllib.request import Request, urlopen

import pytest

try:
    import psycopg2
    from psycopg2.extensions import connection
except ImportError:
    pytest.skip("Install the database extra to run PostgreSQL tests", allow_module_level=True)

from cdc_generator.core.rbac.artifacts import G4, TABLES, check, emit
from cdc_generator.core.rbac.rendering import render_migration, select_permissions
from cdc_generator.core.rbac.validation import Contract, compile_contract, load_json, mapping, sequence

FIXTURES = Path(__file__).parent / "fixtures/rbac"
TENANT_A = "00000000-0000-0000-0000-000000000011"
TENANT_B = "00000000-0000-0000-0000-000000000022"
ACTOR_A = "00000000-0000-0000-0000-000000000033"
ACTOR_B = "00000000-0000-0000-0000-000000000044"
DDL = f"""
CREATE SCHEMA editor;
CREATE TABLE editor.qnrs (id uuid PRIMARY KEY, customer_id uuid NOT NULL,
  user_id uuid, lifecycle_status text NOT NULL, unprojected text);
INSERT INTO editor.qnrs VALUES
 ('00000000-0000-0000-0000-000000000001','{TENANT_A}','{ACTOR_A}','draft','private'),
 ('00000000-0000-0000-0000-000000000002','{TENANT_A}','{ACTOR_B}','draft','private'),
 ('00000000-0000-0000-0000-000000000003','{TENANT_B}','{ACTOR_B}','draft','private');
"""


def _baseline() -> Contract:
    """Compile the real artifact admission source rather than handwritten RLS."""
    return compile_contract(load_json(FIXTURES / "editor.rbac.json"), load_json(FIXTURES / "editor.catalog.json"))


@pytest.fixture()
def database() -> Iterator[connection]:
    """Use an explicitly supplied local disposable cluster; never target config."""
    dsn = os.environ.get("RBAC_TEST_DSN")
    if not dsn:
        pytest.skip("Set RBAC_TEST_DSN to an isolated PostgreSQL cluster")
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    name = "rbac_" + uuid.uuid4().hex
    with admin.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname='editor_app'")
        if not cursor.fetchone():
            cursor.execute("CREATE ROLE editor_app NOLOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT")
        cursor.execute("ALTER ROLE editor_app NOSUPERUSER NOBYPASSRLS NOINHERIT")
        cursor.execute("CREATE DATABASE " + name)
    config = admin.get_dsn_parameters()
    config["dbname"] = name
    config.pop("options", None)
    db = psycopg2.connect(**config)
    db.autocommit = True
    with db.cursor() as cursor:
        cursor.execute(DDL)
    try:
        yield db
    finally:
        db.close()
        with admin.cursor() as cursor:
            cursor.execute("REVOKE postgres FROM editor_app")
            cursor.execute("DROP DATABASE " + name + " WITH (FORCE)")
            cursor.execute("ALTER ROLE editor_app NOSUPERUSER NOBYPASSRLS NOINHERIT")
        admin.close()


def _apply(db: connection, sql: bytes) -> None:
    """Execute actual compiler output; failed transactions must be rolled back."""
    with db.cursor() as cursor:
        try:
            cursor.execute(sql.decode())
        except psycopg2.Error:
            cursor.execute("ROLLBACK")
            raise


def _read(db: connection, role: str | None, tenant: str | None, actor: str | None) -> list[str]:
    """Exercise the trusted per-transaction GUC contract as editor_app."""
    with db.cursor() as cursor:
        cursor.execute("BEGIN; SET LOCAL ROLE editor_app")
        try:
            for key, value in [("app.role", role), ("app.customer_id", tenant), ("app.user_id", actor)]:
                if value is not None:
                    cursor.execute("SELECT set_config(%s,%s,true)", (key, value))
            cursor.execute("SELECT id::text FROM editor.qnrs ORDER BY id")
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.execute("ROLLBACK")


def _activate_test_rls(db: connection) -> None:
    """Activate only disposable test tables; activation is outside compiler scope."""
    with db.cursor() as cursor:
        cursor.execute("ALTER TABLE editor.qnrs ENABLE ROW LEVEL SECURITY; ALTER TABLE editor.qnrs FORCE ROW LEVEL SECURITY")


@pytest.mark.parametrize(
    ("role", "tenant", "actor", "count"),
    [
        ("recipient", TENANT_A, ACTOR_A, 1),
        ("recipient", TENANT_A, ACTOR_B, 1),
        ("recipient", TENANT_B, ACTOR_A, 0),
        ("recipient", TENANT_B, ACTOR_B, 1),
        ("super_user", TENANT_A, ACTOR_A, 2),
        ("therapist", TENANT_B, ACTOR_B, 1),
        ("therpist", TENANT_A, ACTOR_A, 0),
        ("qnr_projection_writer", TENANT_A, ACTOR_A, 0),
        (None, TENANT_A, ACTOR_A, 0),
        ("recipient", None, ACTOR_A, 0),
        ("recipient", TENANT_A, None, 0),
    ],
)
def test_fresh_two_tenant_session_matrix(database: connection, role: str | None, tenant: str | None, actor: str | None, count: int) -> None:
    """Tenant, actor, platform role and missing-context negatives run on real RLS."""
    _apply(database, render_migration(_baseline(), None)[0])
    _activate_test_rls(database)
    assert len(_read(database, role, tenant, actor)) == count
    assert _read(database, None, None, None) == []  # reused connection never inherits prior transaction context


@pytest.mark.parametrize("operation", ["INSERT", "UPDATE", "DELETE", "TRUNCATE", "unprojected", "malformed_context"])
def test_forbidden_privileges(database: connection, operation: str) -> None:
    """No writes, unprojected columns, or malformed tenant can bypass the subset."""
    _apply(database, render_migration(_baseline(), None)[0])
    _activate_test_rls(database)
    statements = {
        "INSERT": "INSERT INTO editor.qnrs(id) VALUES (gen_random_uuid())",
        "UPDATE": "UPDATE editor.qnrs SET lifecycle_status='active'",
        "DELETE": "DELETE FROM editor.qnrs",
        "TRUNCATE": "TRUNCATE editor.qnrs",
        "unprojected": "SELECT unprojected FROM editor.qnrs",
    }
    with pytest.raises(psycopg2.Error):
        if operation == "malformed_context":
            _read(database, "therapist", "malformed-uuid", ACTOR_A)
        else:
            _apply(database, ("BEGIN; SET LOCAL ROLE editor_app; " + statements[operation] + "; COMMIT;").encode())


@pytest.mark.parametrize("upgrade", [False, True])
@pytest.mark.parametrize(
    "failure",
    [
        "superuser",
        "bypassrls",
        "privileged_member",
        "owner",
        "broad_select",
        "write",
        "column_write",
        "extra_column",
        "public_grant",
        "unmanaged_policy",
        "catalog_type",
        "missing_column",
    ],
)
def test_installation_failure_is_transactional(database: connection, upgrade: bool, failure: str) -> None:
    """Fresh and upgrade safety failures leave the preexisting DB state intact."""
    baseline = _baseline()
    if upgrade:
        _apply(database, render_migration(baseline, None)[0])
    poison = {
        "superuser": "ALTER ROLE editor_app SUPERUSER",
        "bypassrls": "ALTER ROLE editor_app BYPASSRLS",
        "privileged_member": "GRANT postgres TO editor_app",
        "owner": "ALTER TABLE editor.qnrs OWNER TO editor_app",
        "broad_select": "GRANT SELECT ON editor.qnrs TO editor_app",
        "write": "GRANT UPDATE ON editor.qnrs TO editor_app",
        "column_write": "GRANT UPDATE(lifecycle_status) ON editor.qnrs TO editor_app",
        "extra_column": "GRANT SELECT(unprojected) ON editor.qnrs TO editor_app",
        "public_grant": "GRANT SELECT ON editor.qnrs TO PUBLIC",
        "unmanaged_policy": "CREATE POLICY surprise ON editor.qnrs USING (true)",
        "catalog_type": "ALTER TABLE editor.qnrs ALTER COLUMN id TYPE text",
        "missing_column": "ALTER TABLE editor.qnrs DROP COLUMN lifecycle_status",
    }
    with database.cursor() as cursor:
        cursor.execute(poison[failure])
        cursor.execute("SELECT polname,pg_get_expr(polqual,polrelid) FROM pg_policy ORDER BY polname")
        prior = cursor.fetchall()
    with pytest.raises(psycopg2.Error, match="RBAC"):
        _apply(database, render_migration(baseline, baseline if upgrade else None)[0])
    with database.cursor() as cursor:
        cursor.execute("SELECT polname,pg_get_expr(polqual,polrelid) FROM pg_policy ORDER BY polname")
        assert cursor.fetchall() == prior


def test_missing_role_and_table(database: connection) -> None:
    """Missing installation prerequisites report an exact failure before grants."""
    with database.cursor() as cursor:
        cursor.execute("DROP ROLE editor_app")
    with pytest.raises(psycopg2.Error, match="role missing"):
        _apply(database, render_migration(_baseline(), None)[0])
    with database.cursor() as cursor:
        cursor.execute("CREATE ROLE editor_app NOLOGIN; DROP TABLE editor.qnrs")
    with pytest.raises(psycopg2.Error, match="table missing"):
        _apply(database, render_migration(_baseline(), None)[0])


def test_upgrade_and_rollback_authorization(database: connection) -> None:
    """A real narrower upgrade takes effect; down restores the previous rules."""
    baseline = _baseline()
    source = sequence(copy.deepcopy(baseline.source))
    permissions = sequence(mapping(mapping(source[0])["definition"])["permissions"])
    mapping(permissions[2])["select"] = copy.deepcopy(mapping(permissions[0])["select"])
    current = compile_contract(source, baseline.catalog)
    _apply(database, render_migration(baseline, None)[0])
    _activate_test_rls(database)
    assert len(_read(database, "therapist", TENANT_A, ACTOR_A)) == 2
    up, down = render_migration(current, baseline)
    _apply(database, up)
    assert len(_read(database, "therapist", TENANT_A, ACTOR_A)) == 1
    assert len(_read(database, "therapist", TENANT_B, ACTOR_A)) == 0
    _apply(database, down)
    assert len(_read(database, "therapist", TENANT_A, ACTOR_A)) == 2
    _apply(database, render_migration(baseline, None)[1])
    with pytest.raises(psycopg2.Error, match="permission denied"):
        _read(database, "therapist", TENANT_A, ACTOR_A)


@pytest.mark.parametrize(("enabled", "forced"), [(False, False), (True, False), (True, True), (False, True)])
def test_preparation_and_rollbacks_preserve_activation_flags(database: connection, enabled: bool, forced: bool) -> None:
    """Fresh, upgrade and both downs never enable, force, disable or unforce RLS."""
    with database.cursor() as cursor:
        cursor.execute("ALTER TABLE editor.qnrs " + ("ENABLE" if enabled else "DISABLE") + " ROW LEVEL SECURITY")
        cursor.execute("ALTER TABLE editor.qnrs " + ("FORCE" if forced else "NO FORCE") + " ROW LEVEL SECURITY")
    baseline = _baseline()
    source = sequence(copy.deepcopy(baseline.source))
    permissions = sequence(mapping(mapping(source[0])["definition"])["permissions"])
    mapping(permissions[2])["select"] = copy.deepcopy(mapping(permissions[0])["select"])
    current = compile_contract(source, baseline.catalog)
    fresh_up, fresh_down = render_migration(baseline, None)
    up, down = render_migration(current, baseline)
    for sql, policy_count in [(fresh_up, 3), (up, 3), (down, 3), (fresh_down, 0)]:
        _apply(database, sql)
        with database.cursor() as cursor:
            cursor.execute("SELECT relrowsecurity,relforcerowsecurity FROM pg_class WHERE oid='editor.qnrs'::regclass")
            assert cursor.fetchone() == (enabled, forced)
            cursor.execute("SELECT count(*) FROM pg_policy WHERE polrelid='editor.qnrs'::regclass")
            assert cursor.fetchone() == (policy_count,)


def test_inactive_preparation_preserves_owner_writes_and_admin_worker_reads(database: connection) -> None:
    """Policy preparation leaves the existing non-RLS owner and worker contexts intact."""
    owner = "rbac_owner_" + uuid.uuid4().hex
    worker = "rbac_worker_" + uuid.uuid4().hex
    with database.cursor() as cursor:
        cursor.execute(f'CREATE ROLE "{owner}" NOLOGIN; CREATE ROLE "{worker}" NOLOGIN')
        cursor.execute(f'GRANT USAGE ON SCHEMA editor TO "{owner}","{worker}"')
        cursor.execute(f'ALTER TABLE editor.qnrs OWNER TO "{owner}"; GRANT SELECT(id) ON editor.qnrs TO "{worker}"')
    baseline = _baseline()
    source = sequence(copy.deepcopy(baseline.source))
    permissions = sequence(mapping(mapping(source[0])["definition"])["permissions"])
    mapping(permissions[2])["select"] = copy.deepcopy(mapping(permissions[0])["select"])
    current = compile_contract(source, baseline.catalog)
    fresh_up, fresh_down = render_migration(baseline, None)
    up, down = render_migration(current, baseline)
    try:
        for sql, app_select in [(fresh_up, True), (up, True), (down, True), (fresh_down, False)]:
            _apply(database, sql)
            if app_select:
                assert len(_read(database, "admin", None, None)) == 3
            with database.cursor() as cursor:
                cursor.execute(f"BEGIN; SET LOCAL ROLE \"{worker}\"; SET LOCAL app.role = 'admin'")
                cursor.execute("SELECT id FROM editor.qnrs")
                assert len(cursor.fetchall()) == 3
                cursor.execute("ROLLBACK")
                cursor.execute(f'BEGIN; SET LOCAL ROLE "{owner}"')
                cursor.execute("UPDATE editor.qnrs SET lifecycle_status='active'")
                assert cursor.rowcount == 3
                cursor.execute("INSERT INTO editor.qnrs(id,customer_id,lifecycle_status) VALUES(gen_random_uuid(),%s,'draft')", (TENANT_A,))
                assert cursor.rowcount == 1
                cursor.execute("DELETE FROM editor.qnrs WHERE lifecycle_status='draft'")
                assert cursor.rowcount == 1
                cursor.execute("ROLLBACK")
    finally:
        with database.cursor() as cursor:
            cursor.execute("ROLLBACK")
            cursor.execute("ALTER TABLE editor.qnrs OWNER TO postgres")
            cursor.execute(f'DROP OWNED BY "{owner}","{worker}"; DROP ROLE "{owner}","{worker}"')


def _http(endpoint: str, value: object, headers: dict[str, str] | None = None) -> dict[str, object]:
    """Call the explicitly supplied disposable Hasura instance, without secrets."""
    base = os.environ["RBAC_TEST_HASURA_URL"]
    request = Request(base + endpoint, data=json.dumps(value).encode(), headers={"Content-Type": "application/json", **(headers or {})})
    with urlopen(request, timeout=20) as response:
        return json.load(response)


def _hasura_upgrade(db: connection) -> None:
    """Narrow both enforcers and require identical tenant/actor results."""
    baseline = _baseline()
    source = sequence(copy.deepcopy(baseline.source))
    permissions = sequence(mapping(mapping(source[0])["definition"])["permissions"])
    mapping(permissions[2])["select"] = copy.deepcopy(mapping(permissions[0])["select"])
    upgraded = compile_contract(source, baseline.catalog)
    _apply(db, render_migration(upgraded, baseline)[0])
    table = {"schema": "editor", "name": "qnrs"}
    _http("/v1/metadata", {"type": "pg_drop_select_permission", "args": {"source": "default", "table": table, "role": "therapist"}})
    permission = mapping(select_permissions(upgraded)[2])["permission"]
    _http(
        "/v1/metadata",
        {"type": "pg_create_select_permission", "args": {"source": "default", "table": table, "role": "therapist", "permission": permission}},
    )
    for tenant, count in [(TENANT_A, 1), (TENANT_B, 0)]:
        headers = {"x-hasura-role": "therapist", "x-hasura-customer-id": tenant, "x-hasura-user-id": ACTOR_A}
        result = _http("/v1/graphql", {"query": "{ editor_qnrs { id } }"}, headers)
        assert len(result["data"]["editor_qnrs"]) == count
        assert sorted(row["id"] for row in result["data"]["editor_qnrs"]) == _read(db, "therapist", tenant, ACTOR_A)


def test_hasura_two_tenants_and_no_mutations() -> None:
    """Execute real Hasura filters and inspect every role's mutation exclusion."""
    if not os.environ.get("RBAC_TEST_HASURA_URL") or not os.environ.get("RBAC_TEST_DSN"):
        pytest.skip("Set isolated RBAC_TEST_HASURA_URL and RBAC_TEST_DSN")
    db = psycopg2.connect(os.environ["RBAC_TEST_DSN"])
    db.autocommit = True
    _http("/v1/metadata", {"type": "clear_metadata", "args": {}})
    with db.cursor() as cursor:
        cursor.execute("DROP SCHEMA IF EXISTS editor CASCADE")
        cursor.execute(DDL)
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = 'editor_app'")
        if not cursor.fetchone():
            cursor.execute("CREATE ROLE editor_app NOLOGIN NOSUPERUSER NOBYPASSRLS")
    _apply(db, render_migration(_baseline(), None)[0])
    _activate_test_rls(db)
    try:
        _http(
            "/v1/metadata",
            {
                "type": "replace_metadata",
                "args": {
                    "version": 3,
                    "sources": [
                        {
                            "name": "default",
                            "kind": "postgres",
                            "configuration": {"connection_info": {"database_url": {"from_env": "HASURA_GRAPHQL_DATABASE_URL"}}},
                            "tables": [{"table": {"schema": "editor", "name": "qnrs"}, "select_permissions": select_permissions(_baseline())}],
                        }
                    ],
                },
            },
        )
        for role in ["recipient", "therapist", "super_user"]:
            for tenant, actor, count in [(TENANT_A, ACTOR_A, 1 if role == "recipient" else 2), (TENANT_B, ACTOR_A, 0 if role == "recipient" else 1)]:
                headers = {"x-hasura-role": role, "x-hasura-customer-id": tenant, "x-hasura-user-id": actor}
                result = _http("/v1/graphql", {"query": "{ editor_qnrs { id customer_id user_id lifecycle_status } }"}, headers)
                assert len(result["data"]["editor_qnrs"]) == count, result
                assert sorted(row["id"] for row in result["data"]["editor_qnrs"]) == _read(db, role, tenant, actor)
                introspection = _http("/v1/graphql", {"query": "{ __schema { mutationType { name } } }"}, headers)
                assert introspection["data"]["__schema"]["mutationType"] is None
                mutation = _http("/v1/graphql", {"query": "mutation { delete_editor_qnrs(where: {}) { affected_rows } }"}, headers)
                assert "errors" in mutation
                hidden = _http("/v1/graphql", {"query": "{ editor_qnrs { unprojected } }"}, headers)
                assert "errors" in hidden
        _hasura_upgrade(db)
        missing = _http("/v1/graphql", {"query": "{ editor_qnrs { id } }"}, {"x-hasura-role": "recipient"})
        assert "errors" in missing
        internal = _http("/v1/graphql", {"query": "{ editor_qnrs { id } }"}, {"x-hasura-role": "qnr_projection_writer"})
        assert "errors" in internal
    finally:
        _http("/v1/metadata", {"type": "clear_metadata", "args": {}})
        with db.cursor() as cursor:
            cursor.execute("DROP SCHEMA editor CASCADE")
        db.close()


def _cli_export(project: Path) -> None:
    """Use the explicitly supplied real Hasura CLI against disposable localhost."""
    result = subprocess.run(
        [os.environ["RBAC_TEST_HASURA_CLI"], "metadata", "export", "--project", str(project), "--skip-update-check", "--envfile", "/dev/null"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_real_hasura_cli_export_only_changes_select(tmp_path: Path) -> None:
    """Canonical CLI re-exports match fresh SELECT bytes and remain upgradeable."""
    if not all(os.environ.get(key) for key in ("RBAC_TEST_DSN", "RBAC_TEST_HASURA_URL", "RBAC_TEST_HASURA_CLI")):
        pytest.skip("Set disposable PostgreSQL/Hasura endpoints and a real RBAC_TEST_HASURA_CLI")
    db = psycopg2.connect(os.environ["RBAC_TEST_DSN"])
    db.autocommit = True
    _http("/v1/metadata", {"type": "clear_metadata", "args": {}})
    with db.cursor() as cursor:
        cursor.execute(DDL + "CREATE TABLE public.queries(id uuid PRIMARY KEY)")
    editor = {
        "table": {"schema": "editor", "name": "qnrs"},
        "object_relationships": [
            {
                "name": "owner_relationship",
                "using": {
                    "manual_configuration": {
                        "column_mapping": {"id": "id"},
                        "insertion_order": None,
                        "remote_table": {"schema": "public", "name": "queries"},
                    }
                },
            },
        ],
    }
    legacy = {
        "table": {"schema": "public", "name": "queries"},
        "update_permissions": [
            {"role": "legacy_owner", "permission": {"columns": ["id"], "filter": {}, "check": {}}},
        ],
    }
    tables = [editor, legacy]
    metadata = {
        "version": 3,
        "sources": [
            {
                "name": "default",
                "kind": "postgres",
                "configuration": {"connection_info": {"database_url": {"from_env": "HASURA_GRAPHQL_DATABASE_URL"}}},
                "tables": tables,
            }
        ],
    }
    try:
        _http("/v1/metadata", {"type": "replace_metadata", "args": metadata})
        (tmp_path / "config.yaml").write_text(
            "version: 3\nendpoint: "
            + os.environ["RBAC_TEST_HASURA_URL"]
            + "\nmetadata_directory: metadata\nmigrations_directory: migrations\nenable_telemetry: false\n"
        )
        _cli_export(tmp_path)
        path = tmp_path / TABLES / "editor_qnrs.yaml"
        original = path.read_bytes()
        g4 = (tmp_path / G4).read_bytes()
        source = tmp_path / "source.json"
        source.write_bytes((FIXTURES / "editor.rbac.json").read_bytes())
        catalog = FIXTURES / "editor.catalog.json"
        emit(tmp_path, source, catalog, "1800000000000")
        emitted = path.read_bytes()
        assert emitted.startswith(original)
        assert (tmp_path / G4).read_bytes() == g4
        editor["select_permissions"] = select_permissions(_baseline())
        _http("/v1/metadata", {"type": "replace_metadata", "args": metadata})
        _cli_export(tmp_path)
        assert path.read_bytes() == emitted
        check(tmp_path, source, catalog)
        permissions = sequence(load_json(source))
        roles = sequence(mapping(mapping(permissions[0])["definition"])["permissions"])
        mapping(roles[2])["select"] = copy.deepcopy(mapping(roles[0])["select"])
        source.write_bytes(json.dumps(permissions).encode())
        emit(tmp_path, source, catalog, "1800000000001")
        editor["select_permissions"] = select_permissions(compile_contract(permissions, load_json(catalog)))
        upgraded = path.read_bytes()
        _http("/v1/metadata", {"type": "replace_metadata", "args": metadata})
        _cli_export(tmp_path)
        assert path.read_bytes() == upgraded
        check(tmp_path, source, catalog)
    finally:
        _http("/v1/metadata", {"type": "clear_metadata", "args": {}})
        with db.cursor() as cursor:
            cursor.execute("DROP SCHEMA editor CASCADE; DROP TABLE public.queries")
        db.close()
