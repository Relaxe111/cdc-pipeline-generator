"""Finite PostgreSQL 17 ACL/origin/default/membership observations used before policy writes."""

from __future__ import annotations

from cdc_generator.core.rbac.validation import Json, identifier, mapping, sequence, string

PRIVILEGES = {"SELECT", "INSERT", "UPDATE", "DELETE"}

MEMBERSHIP_SQL = """WITH RECURSIVE reachable(oid) AS (
  SELECT oid FROM pg_roles WHERE rolname = 'editor_app'
  UNION SELECT m.roleid FROM pg_auth_members m JOIN reachable r ON m.member = r.oid
)
SELECT jsonb_build_object(
 'roles', COALESCE((SELECT jsonb_agg(jsonb_build_object('name', rolname, 'superuser', rolsuper,
   'bypassrls', rolbypassrls, 'inherit', rolinherit, 'createRole', rolcreaterole,
   'createDb', rolcreatedb, 'replication', rolreplication, 'login', rolcanlogin) ORDER BY rolname)
   FROM pg_roles WHERE oid IN (SELECT oid FROM reachable)), '[]'::jsonb),
 'memberships', COALESCE((SELECT jsonb_agg(jsonb_build_object('role', r.rolname, 'member', u.rolname,
   'grantor', g.rolname, 'admin', m.admin_option, 'inherit', m.inherit_option, 'set', m.set_option)
   ORDER BY r.rolname, u.rolname, g.rolname) FROM pg_auth_members m
   JOIN pg_roles r ON r.oid=m.roleid JOIN pg_roles u ON u.oid=m.member JOIN pg_roles g ON g.oid=m.grantor
   WHERE m.member IN (SELECT oid FROM reachable)), '[]'::jsonb))"""

ACL_SQL = """SELECT jsonb_build_object(
 'owner', pg_get_userbyid(c.relowner),
 'tableAcl', COALESCE((SELECT jsonb_agg(jsonb_build_object('grantor', pg_get_userbyid(x.grantor),
   'grantee', CASE WHEN x.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(x.grantee) END,
   'privilege', x.privilege_type, 'grantOption', x.is_grantable)
   ORDER BY pg_get_userbyid(x.grantor), CASE WHEN x.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(x.grantee) END,
     x.privilege_type) FROM aclexplode(COALESCE(c.relacl, acldefault('r',c.relowner))) x), '[]'::jsonb),
 'columnAcl', COALESCE((SELECT jsonb_agg(jsonb_build_object('column', a.attname,
   'grantor', pg_get_userbyid(x.grantor), 'grantee', CASE WHEN x.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(x.grantee) END,
   'privilege', x.privilege_type, 'grantOption', x.is_grantable) ORDER BY a.attname,pg_get_userbyid(x.grantor),
     CASE WHEN x.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(x.grantee) END,x.privilege_type)
   FROM pg_attribute a CROSS JOIN LATERAL aclexplode(a.attacl) x WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped), '[]'::jsonb),
 'schemaAcl', COALESCE((SELECT jsonb_agg(jsonb_build_object('grantor', pg_get_userbyid(x.grantor),
   'grantee', CASE WHEN x.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(x.grantee) END,
   'privilege', x.privilege_type, 'grantOption', x.is_grantable)
   ORDER BY pg_get_userbyid(x.grantor),CASE WHEN x.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(x.grantee) END,
     x.privilege_type) FROM aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) x), '[]'::jsonb))
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid='"editor"."qnrs"'::regclass"""


def defaults_sql(creators: tuple[str, ...]) -> str:
    """Observe global and every schema default for each explicitly qualified creator."""
    names = ", ".join(f"'{identifier(name)}'" for name in creators)
    return f"""SELECT COALESCE(jsonb_agg(jsonb_build_object('creator', pg_get_userbyid(d.defaclrole),
 'schema', CASE WHEN d.defaclnamespace=0 THEN NULL ELSE n.nspname END, 'kind', d.defaclobjtype,
 'grantor', pg_get_userbyid(x.grantor), 'grantee', CASE WHEN x.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(x.grantee) END,
 'privilege', x.privilege_type, 'grantOption', x.is_grantable)
 ORDER BY pg_get_userbyid(d.defaclrole),n.nspname,d.defaclobjtype,pg_get_userbyid(x.grantor),
   CASE WHEN x.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(x.grantee) END,x.privilege_type), '[]'::jsonb)
 FROM pg_default_acl d LEFT JOIN pg_namespace n ON n.oid=d.defaclnamespace
 CROSS JOIN LATERAL aclexplode(d.defaclacl) x WHERE pg_get_userbyid(d.defaclrole) IN ({names})"""


def validate_snapshots(membership: Json, acl: Json, creators: tuple[str, ...]) -> None:
    """Refuse privileged identities and source snapshots that invent a broader ACL envelope."""
    obj = mapping(membership)
    if set(obj) != {"roles", "memberships"}:
        raise ValueError("Unproved membership readback")
    roles = [mapping(role) for role in sequence(obj["roles"])]
    if not roles or sum(role.get("name") == "editor_app" for role in roles) != 1:
        raise ValueError("Missing connection membership identity")
    for role in roles:
        if set(role) != {"name", "superuser", "bypassrls", "inherit", "createRole", "createDb", "replication", "login"}:
            raise ValueError("Incomplete role attributes")
        identifier(role["name"])
        if any(not isinstance(role[key], bool) for key in set(role) - {"name"}):
            raise ValueError("Unknown role attributes")
        if any(role[key] is not False for key in ("superuser", "bypassrls", "createRole", "createDb", "replication")):
            raise ValueError("Privileged connection membership source")
    rights = mapping(acl)
    if set(rights) != {"owner", "tableAcl", "columnAcl", "schemaAcl"}:
        raise ValueError("Incomplete ACL/default-creator source")
    owner = identifier(rights["owner"])
    if owner not in creators or any(role["name"] == owner for role in roles):
        raise ValueError("Unqualified creator or owner membership")
    applicable = {"editor_app", "PUBLIC", *(string(role["name"]) for role in roles)}
    own_rights = [mapping(right) for right in sequence(rights["tableAcl"]) if string(mapping(right).get("grantee")) in applicable]
    if len(own_rights) != len(PRIVILEGES) or {string(right.get("privilege")) for right in own_rights} != PRIVILEGES:
        raise ValueError("Owner envelope requires exactly independently originated table CRUD")
    if any(right.get("grantOption") is not False or right.get("grantee") != "editor_app" for right in own_rights):
        raise ValueError("Unsupported grant option or ACL origin")
    if any(string(mapping(right).get("grantee")) in applicable for right in sequence(rights["columnAcl"])):
        raise ValueError("Unexpected originated column privileges")
    schema = [mapping(right) for right in sequence(rights["schemaAcl"]) if string(mapping(right).get("grantee")) in applicable]
    if (
        len(schema) != 1
        or schema[0].get("grantee") != "editor_app"
        or schema[0].get("privilege") != "USAGE"
        or schema[0].get("grantOption") is not False
    ):
        raise ValueError("Schema source must qualify exact nongrantable USAGE, without CREATE")


def validate_defaults(value: Json, membership: Json, creators: tuple[str, ...]) -> None:
    """No fixture declaration can bless new broad creator defaults for app/PUBLIC/inherited roles."""
    roles = {"PUBLIC", *(string(mapping(role)["name"]) for role in sequence(mapping(membership)["roles"]))}
    for item in sequence(value):
        entry = mapping(item)
        if set(entry) != {"creator", "schema", "kind", "grantor", "grantee", "privilege", "grantOption"}:
            raise ValueError("Unproved global/schema default privilege definition")
        if string(entry["creator"]) not in creators or string(entry["grantee"]) in roles:
            raise ValueError("Unqualified creator or broad app/PUBLIC/inherited default privileges")
