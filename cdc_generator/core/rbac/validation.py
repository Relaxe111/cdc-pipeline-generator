"""Offline OpenDD shape and bounded, real-name compiler validation."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import NoReturn, cast

from jsonschema import Draft7Validator, ValidationError, validate
from referencing import Registry
from referencing.exceptions import NoSuchResource
from referencing.jsonschema import Schema

Json = str | int | float | bool | None | list["Json"] | dict[str, "Json"]
ROLES = ("recipient", "super_user", "therapist")
UPSTREAM = "94915fe51d6d21bd7f6d4452dc16221bef8cfefd"
UPSTREAM_HASH = "3ff0d2a5680d57c8a042c0c577b7dd68aab7f71ab1b6dd9b81e6569702cf7b20"
SLICE_HASH = "162bcba04f5e01580f0067a4e2f74b47c1fade2db46cd97a570c4a77fc8fa7d3"
VALIDATOR = "4.25.1"
MAX_FILTER_DEPTH = 16
ASSETS = Path(__file__).resolve().parents[2] / "templates" / "rbac"
SESSION = {"x-hasura-customer-id": ("customer_id", "app.customer_id"), "x-hasura-user-id": ("user_id", "app.user_id")}


@dataclass(frozen=True)
class Rule:
    """One role's column allow-list and canonical flat session comparisons."""

    role: str
    columns: tuple[str, ...]
    comparisons: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class Contract:
    """Validated single-table admission subset, never a full service policy."""

    schema: str
    table: str
    column_types: tuple[tuple[str, str], ...]
    rules: tuple[Rule, ...]
    source: Json
    catalog: Json


def digest(data: bytes) -> str:
    """Hash exact bytes, including formatting of owner-authored inputs."""
    return hashlib.sha256(data).hexdigest()


def canonical(value: object) -> bytes:
    """Encode compiler state reproducibly, without timestamps or host paths."""
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def mapping(value: Json) -> dict[str, Json]:
    """Require an object at a JSON trust boundary."""
    if not isinstance(value, dict):
        raise ValueError("Expected JSON object")
    return value


def sequence(value: Json) -> list[Json]:
    """Require a list at a JSON trust boundary."""
    if not isinstance(value, list):
        raise ValueError("Expected JSON array")
    return value


def string(value: Json) -> str:
    """Require a string; never coerce untrusted SQL names or values."""
    if not isinstance(value, str):
        raise ValueError("Expected JSON string")
    return value


def identifier(value: Json) -> str:
    """Accept only bounded PostgreSQL identifiers, avoiding truncation."""
    name = string(value)
    if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", name):
        raise ValueError(f"Unsupported identifier: {name}")
    return name


def _unique_pairs(pairs: list[tuple[str, Json]]) -> dict[str, Json]:
    """Reject duplicate JSON keys instead of silently accepting the last."""
    result: dict[str, Json] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def load_json(path: Path) -> Json:
    """Load a JSON input without duplicate keys or nonfinite constants."""

    def reject_constant(value: str) -> None:
        raise ValueError(f"Invalid JSON constant: {value}")

    return cast(Json, json.loads(path.read_bytes(), object_pairs_hook=_unique_pairs, parse_constant=reject_constant))


def _offline_resource(uri: str) -> NoReturn:
    """Deny all external retrieval, including schema URLs and secrets."""
    raise NoSuchResource(ref=uri)


def doctor() -> dict[str, str]:
    """Verify the exact official schema slice and local validator pin."""
    if version("jsonschema") != VALIDATOR:
        raise ValueError(f"Required local jsonschema version is {VALIDATOR}")
    for filename, expected in [("metadata.jsonschema", UPSTREAM_HASH), ("opendd-permissions.schema.json", SLICE_HASH)]:
        if digest((ASSETS / filename).read_bytes()) != expected:
            raise ValueError(f"Pinned OpenDD schema drift: {filename}")
    return {"opendd_commit": UPSTREAM, "upstream_sha256": UPSTREAM_HASH, "slice_sha256": SLICE_HASH, "jsonschema": VALIDATOR}


def _permissions(source: Json) -> dict[str, dict[str, Json]]:
    """Validate official v1 shapes and require exactly the two supported kinds."""
    doctor()
    schema = mapping(load_json(ASSETS / "opendd-permissions.schema.json"))
    try:
        validate(source, schema, cls=Draft7Validator, registry=Registry[Schema](retrieve=_offline_resource))
    except ValidationError as error:
        raise ValueError(f"OpenDD schema clause {error.json_path}: {error.message}") from error
    definitions: dict[str, dict[str, Json]] = {}
    for item in sequence(source):
        obj = mapping(item)
        kind = string(obj["kind"])
        if kind in definitions:
            raise ValueError(f"Duplicate OpenDD kind: {kind}")
        definitions[kind] = mapping(obj["definition"])
    if set(definitions) != {"ModelPermissions", "TypePermissions"}:
        raise ValueError("Require one ModelPermissions v1 and one TypePermissions v1")
    return definitions


def _role_map(definition: dict[str, Json]) -> dict[str, dict[str, Json]]:
    """Validate platform vocabulary independently of permissive upstream Role."""
    roles: dict[str, dict[str, Json]] = {}
    for item in sequence(definition["permissions"]):
        permission = mapping(item)
        role = string(permission["role"])
        if role not in ROLES or role in roles:
            raise ValueError(f"Unknown or duplicate platform role: {role}")
        roles[role] = permission
    return roles


def _comparisons(predicate: Json, depth: int = 0) -> tuple[tuple[str, str], ...]:
    """Compile only nonempty AND and typed field-to-session equality."""
    if depth > MAX_FILTER_DEPTH:
        raise ValueError("Flat filter exceeds depth limit")
    obj = mapping(predicate)
    if set(obj) == {"and"}:
        terms = sequence(obj["and"])
        if not terms:
            raise ValueError("Empty AND would grant all rows")
        pairs = [pair for term in terms for pair in _comparisons(term, depth + 1)]
        return tuple(sorted(set(pairs)))
    if set(obj) != {"fieldComparison"}:
        raise ValueError("Unsupported filter: only flat _eq and and are admitted")
    comparison = mapping(obj["fieldComparison"])
    value = mapping(comparison["value"])
    if comparison["operator"] != "_eq" or set(value) != {"sessionVariable"}:
        raise ValueError("Only _eq to a declared sessionVariable is admitted")
    variable = string(value["sessionVariable"])
    if variable not in SESSION or comparison["field"] != SESSION[variable][0]:
        raise ValueError(f"Unqualified field/session mapping: {variable}")
    return ((string(comparison["field"]), variable),)


def compile_contract(source: Json, catalog_value: Json) -> Contract:
    """Validate pinned catalog names and local OpenDD semantics before emission."""
    definitions = _permissions(source)
    catalog = mapping(catalog_value)
    schema, table = identifier(catalog["schema"]), identifier(catalog["table"])
    if schema != "editor":
        raise ValueError("This bounded compiler admits only owner-exclusive editor")
    if sorted(string(role) for role in sequence(catalog["acceptedRoles"])) != list(ROLES):
        raise ValueError("Catalog role vocabulary must match recipient/super_user/therapist")
    model, types = definitions["ModelPermissions"], definitions["TypePermissions"]
    if model["modelName"] != catalog["modelName"] or types["typeName"] != catalog["typeName"]:
        raise ValueError("OpenDD model/type name does not match catalog")
    columns: dict[str, str] = {}
    for item in sequence(catalog["columns"]):
        column = mapping(item)
        name, sql_type = identifier(column["name"]), string(column["type"])
        if name in columns or sql_type not in {"uuid", "text"}:
            raise ValueError(f"Unsupported or duplicate catalog column: {name}")
        columns[name] = sql_type
    model_roles, type_roles = _role_map(model), _role_map(types)
    if not model_roles or model_roles.keys() != type_roles.keys():
        raise ValueError("Model and type role sets must agree and be nonempty")
    rules: list[Rule] = []
    for role in sorted(model_roles):
        permission, type_permission = model_roles[role], type_roles[role]
        if set(permission) != {"role", "select"} or set(type_permission) != {"role", "output"}:
            raise ValueError("Unsupported permissions: only SELECT/output is admitted")
        select = mapping(permission["select"])
        if set(select) != {"filter"}:
            raise ValueError("Argument presets/subscriptions are outside the bounded subset")
        fields = tuple(sorted(string(field) for field in sequence(mapping(type_permission["output"])["allowedFields"])))
        if not fields or any(field not in columns for field in fields):
            raise ValueError("Unknown or empty allowedFields")
        pairs = _comparisons(select["filter"])
        if ("customer_id", "x-hasura-customer-id") not in pairs:
            raise ValueError("Every role requires a positive tenant equality")
        if any(field not in columns or columns[field] != "uuid" for field, _variable in pairs):
            raise ValueError("Tenant/actor comparison requires real UUID catalog columns")
        rules.append(Rule(role, fields, pairs))
    if len({rule.columns for rule in rules}) != 1:
        raise ValueError("Role-specific column envelopes require a separate compiler capability")
    return Contract(schema, table, tuple(sorted(columns.items())), tuple(rules), source, catalog_value)


def generate_schema(catalog_value: Json) -> Json:
    """Project real catalog names and platform role enums into the official slice."""
    doctor()
    catalog = mapping(catalog_value)
    result = mapping(load_json(ASSETS / "opendd-permissions.schema.json"))
    definitions = mapping(result["definitions"])
    for name, values in [
        ("Role", list(ROLES)),
        ("ModelName", [string(catalog["modelName"])]),
        ("CustomTypeName", [string(catalog["typeName"])]),
        ("FieldName", [identifier(mapping(col)["name"]) for col in sequence(catalog["columns"])]),
    ]:
        mapping(definitions[name])["enum"] = cast(list[Json], values)
    return result
