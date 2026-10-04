"""Bounded compiler admission, canonical merge and negative drift tests."""

from __future__ import annotations

import copy
import json
import socket
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from cdc_generator.cli.commands import _click_cli
from cdc_generator.core.rbac.artifacts import G4, LOCK, TABLES, check, emit
from cdc_generator.core.rbac.rendering import render_migration, select_permissions
from cdc_generator.core.rbac.validation import (
    ASSETS,
    Contract,
    Json,
    canonical,
    compile_contract,
    doctor,
    generate_schema,
    load_json,
    mapping,
    sequence,
)
from cdc_generator.helpers.yaml_loader import load_yaml_file

FIXTURES = Path(__file__).parent / "fixtures/rbac"


@pytest.fixture()
def inputs() -> tuple[Json, Json]:
    """Use the artifact author's exact pinned official input and real catalog."""
    return load_json(FIXTURES / "editor.rbac.json"), load_json(FIXTURES / "editor.catalog.json")


@pytest.fixture()
def contract(inputs: tuple[Json, Json]) -> Contract:
    """Compile genuine admission inputs for both renderers."""
    return compile_contract(*inputs)


@pytest.fixture()
def owner(tmp_path: Path) -> Path:
    """Create a disposable canonical Hasura project with the real G4 metadata."""
    (tmp_path / TABLES).mkdir(parents=True)
    (tmp_path / "config.yaml").write_text("version: 3\nmetadata_directory: metadata\n")
    (tmp_path / TABLES / "tables.yaml").write_text('- "!include editor_qnrs.yaml"\n- "!include public_queries.yaml"\n')
    for filename in ("editor_qnrs.yaml", "public_queries.yaml"):
        (tmp_path / TABLES / filename).write_bytes((FIXTURES / filename).read_bytes())
    (tmp_path / "source.json").write_bytes((FIXTURES / "editor.rbac.json").read_bytes())
    (tmp_path / "catalog.json").write_bytes((FIXTURES / "editor.catalog.json").read_bytes())
    return tmp_path


def test_offline_validation_and_both_targets(contract: Contract) -> None:
    """No DNS/socket access is possible during actual validation or compilation."""
    with patch.object(socket, "socket", side_effect=AssertionError("network forbidden")):
        assert doctor()["jsonschema"] == "4.25.1"
        assert compile_contract(contract.source, contract.catalog) == contract
        up, down = render_migration(contract, None)
    assert b'FOR SELECT TO "editor_app"' in up
    assert b"GRANT " not in up + down
    assert b"REVOKE " not in up + down
    assert b"ENABLE ROW LEVEL SECURITY" not in up
    assert b"FORCE ROW LEVEL SECURITY" not in up
    assert b"CREATE ROLE" not in up and b"GRANT ALL" not in up
    assert b"DISABLE ROW LEVEL SECURITY" not in down
    for rule, permission in zip(contract.rules, select_permissions(contract), strict=True):
        assert mapping(permission)["role"] == rule.role
        assert mapping(mapping(permission)["permission"])["columns"] == list(rule.columns)
        for field, variable in rule.comparisons:
            assert {field: {"_eq": variable}} in sequence(mapping(mapping(permission)["permission"])["filter"]["_and"])


def test_exact_upstream_slice() -> None:
    """Every vendored permission keyword comes from the pinned official schema."""
    upstream = mapping(load_json(ASSETS / "metadata.jsonschema"))
    sliced = mapping(load_json(ASSETS / "opendd-permissions.schema.json"))

    def without_ids(value: Json) -> Json:
        if isinstance(value, dict):
            return {key: without_ids(item) for key, item in value.items() if key != "$id"}
        if isinstance(value, list):
            return [without_ids(item) for item in value]
        return value

    definitions = mapping(upstream["definitions"])
    for name, definition in mapping(sliced["definitions"]).items():
        assert definition == without_ids(definitions[name])
    variants = sequence(mapping(definitions["OpenDdSubgraphObject"])["oneOf"])
    for variant in sequence(mapping(sliced["items"])["oneOf"]):
        kind = sequence(mapping(mapping(mapping(variant)["properties"])["kind"])["enum"])[0]
        official = next(mapping(item) for item in variants if mapping(item).get("title") == kind)
        assert variant == without_ids(sequence(official["oneOf"])[0])


@pytest.mark.parametrize(
    "failure",
    [
        "unknown_role",
        "internal_role",
        "wrong_model",
        "wrong_type",
        "unknown_field",
        "literal",
        "or",
        "empty_and",
        "no_tenant",
        "relationship",
        "unknown_session",
        "wrong_session_field",
        "mutation",
        "preset",
        "different_columns",
        "duplicate_role",
        "duplicate_kind",
        "version",
        "unknown_column_type",
        "wrong_schema",
        "wrong_roles",
    ],
)
def test_invalid_input_fails_closed(inputs: tuple[Json, Json], failure: str) -> None:
    """Unsupported official shapes/semantics cannot silently broaden access."""
    source, catalog = copy.deepcopy(inputs)
    objects, catalog_obj = sequence(source), mapping(catalog)
    model = mapping(mapping(objects[0])["definition"])
    types = mapping(mapping(objects[1])["definition"])
    permission = mapping(sequence(model["permissions"])[0])
    select = mapping(permission["select"])
    terms = sequence(mapping(select["filter"])["and"])
    comparison = mapping(mapping(terms[0])["fieldComparison"])
    output = mapping(mapping(sequence(types["permissions"])[0])["output"])
    if failure in {"unknown_role", "internal_role"}:
        permission["role"] = "therpist" if failure == "unknown_role" else "qnr_projection_writer"
    elif failure == "wrong_model":
        model["modelName"] = "misspelled"
    elif failure == "wrong_type":
        types["typeName"] = "misspelled"
    elif failure == "unknown_field":
        output["allowedFields"] = ["not_a_column"]
    elif failure == "literal":
        comparison["value"] = {"literal": "tenant"}
    elif failure == "or":
        select["filter"] = {"or": terms}
    elif failure == "empty_and":
        select["filter"] = {"and": []}
    elif failure == "no_tenant":
        select["filter"] = {"and": [terms[1]]}
    elif failure == "relationship":
        select["filter"] = {"relationship": {"name": "other", "predicate": None}}
    elif failure == "unknown_session":
        comparison["value"] = {"sessionVariable": "x-hasura-secret"}
    elif failure == "wrong_session_field":
        comparison["field"] = "id"
    elif failure == "mutation":
        permission["relationalUpdate"] = {}
    elif failure == "preset":
        select["argumentPresets"] = []
    elif failure == "different_columns":
        output["allowedFields"] = ["id"]
    elif failure == "duplicate_role":
        sequence(model["permissions"]).append(permission)
    elif failure == "duplicate_kind":
        objects.append(objects[0])
    elif failure == "version":
        mapping(objects[0])["version"] = "v2"
    elif failure == "unknown_column_type":
        mapping(sequence(catalog_obj["columns"])[0])["type"] = "uuid; DROP SCHEMA editor"
    elif failure == "wrong_schema":
        catalog_obj["schema"] = "public"
    elif failure == "wrong_roles":
        sequence(catalog_obj["acceptedRoles"]).append("qnr_projection_writer")
    with pytest.raises(ValueError):
        compile_contract(source, catalog)


def test_schema_enums_and_duplicate_json(inputs: tuple[Json, Json], tmp_path: Path) -> None:
    """Expose catalog names for authoring, and reject ambiguous JSON keys."""
    schema = mapping(generate_schema(inputs[1]))
    assert mapping(mapping(schema["definitions"])["Role"])["enum"] == ["recipient", "super_user", "therapist"]
    bad = tmp_path / "bad.json"
    bad.write_text('{"role":"recipient","role":"therapist"}')
    with pytest.raises(ValueError, match="Duplicate"):
        load_json(bad)


def test_structured_merge_and_determinism(owner: Path, contract: Contract) -> None:
    """Keep actual G4 bytes/identity and relationships; second emission is inert."""
    original = load_yaml_file(owner / TABLES / "editor_qnrs.yaml")
    g4 = (owner / G4).read_bytes()
    state = emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    after = load_yaml_file(owner / TABLES / "editor_qnrs.yaml")
    assert after.pop("select_permissions") == select_permissions(contract)
    assert after == original
    assert (owner / G4).read_bytes() == g4
    assert load_yaml_file(owner / G4)["table"] == {"name": "queries", "schema": "public"}
    before = {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}
    assert emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000") == state
    assert before == {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}
    assert check(owner, owner / "source.json", owner / "catalog.json") == state


@pytest.mark.parametrize(
    "target",
    [
        "source.json",
        "catalog.json",
        "rbac/.rbac-lock.json",
        "migrations/default/1800000000000_rbac_editor_qnrs/up.sql",
        "migrations/default/1800000000000_rbac_editor_qnrs/down.sql",
    ],
)
def test_drift_detected(owner: Path, target: str) -> None:
    """Exact compiler input, migration and provenance edits fail drift checks."""
    emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    path = owner / target
    path.write_bytes(path.read_bytes() + b"\n")
    if target == str(LOCK):
        data = mapping(load_json(path))
        data["compiler"] = "unqualified"
        path.write_bytes(canonical(data))
    with pytest.raises(ValueError):
        check(owner, owner / "source.json", owner / "catalog.json")


def test_upgrade_preserves_history_and_g4(owner: Path) -> None:
    """An admitted policy change creates a new migration; prior SQL never moves."""
    first = emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    old_sql = (owner / str(first["migration"]) / "up.sql").read_bytes()
    source = sequence(load_json(owner / "source.json"))
    permission = mapping(sequence(mapping(mapping(source[0])["definition"])["permissions"])[2])
    mapping(permission["select"])["filter"] = mapping(sequence(mapping(mapping(source[0])["definition"])["permissions"])[0])["select"]["filter"]
    (owner / "source.json").write_bytes(canonical(source))
    with pytest.raises(ValueError, match="Source/catalog drift"):
        check(owner, owner / "source.json", owner / "catalog.json")
    second = emit(owner, owner / "source.json", owner / "catalog.json", "1800000000001")
    assert first["migration"] != second["migration"]
    assert (owner / str(first["migration"]) / "up.sql").read_bytes() == old_sql
    assert first["preserved"] == second["preserved"]
    assert check(owner, owner / "source.json", owner / "catalog.json") == second


@pytest.mark.parametrize(
    "failure", ["existing_select", "wrong_identity", "editor_mutation", "old_timestamp", "missing_index", "symlink", "missing_g4"]
)
def test_emission_failures_leave_owner_unchanged(owner: Path, failure: str) -> None:
    """Refuse unowned metadata, unsafe destinations and migration collisions."""
    path = owner / TABLES / "editor_qnrs.yaml"
    if failure == "existing_select":
        path.write_text(path.read_text() + "select_permissions: []\n")
    elif failure == "wrong_identity":
        path.write_text(path.read_text().replace("schema: editor", "schema: public", 1))
    elif failure == "editor_mutation":
        path.write_text(path.read_text() + "update_permissions:\n- role: therapist\n")
    elif failure == "old_timestamp":
        (owner / "migrations/default/1800000000001_existing").mkdir(parents=True)
    elif failure == "missing_index":
        (owner / TABLES / "tables.yaml").write_text("[]")
    elif failure == "symlink":
        path.unlink()
        path.symlink_to(FIXTURES / "editor_qnrs.yaml")
    elif failure == "missing_g4":
        (owner / G4).unlink()
    before = {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}
    with pytest.raises((ValueError, OSError)):
        emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    assert before == {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}


def test_filesystem_failure_restores_write_set(owner: Path) -> None:
    """A failed lock publication restores metadata and removes new SQL files."""
    original_write = Path.write_bytes

    def fail_lock(path: Path, data: bytes) -> int:
        if path == owner / LOCK:
            raise OSError("simulated disk failure")
        return original_write(path, data)

    before = {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}
    with patch.object(Path, "write_bytes", fail_lock), pytest.raises(OSError):
        emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    assert before == {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}


def test_real_cdc_cli_registration(owner: Path) -> None:
    """Invoke the real cdc Click registry through every bounded command."""
    runner = CliRunner()
    common = ["--source", str(owner / "source.json"), "--catalog", str(owner / "catalog.json")]
    for args in [
        ["doctor"],
        ["schema", "--catalog", str(owner / "catalog.json")],
        ["validate", *common],
        ["generate", *common],
        ["emit-migration", *common, "--hsr", str(owner), "--migration-version", "1800000000000"],
        ["check", *common, "--hsr", str(owner)],
    ]:
        result = runner.invoke(_click_cli, ["rbac", *args])
        assert result.exit_code == 0, result.output
        json.loads(result.output)
    (owner / "source.json").write_text("[]")
    result = runner.invoke(_click_cli, ["rbac", "check", *common, "--hsr", str(owner)])
    assert result.exit_code != 0 and "drift" in result.output


def test_unknown_kind(inputs: tuple[Json, Json]) -> None:
    """Reject commands, which cannot enter a SELECT-only permission compiler."""
    source, catalog = copy.deepcopy(inputs)
    mapping(sequence(source)[0])["kind"] = "CommandPermissions"
    with pytest.raises(ValueError):
        compile_contract(source, catalog)


@pytest.mark.parametrize("value", [None, [], 12, True])
def test_malformed_source_reports_validation_error(value: Json, inputs: tuple[Json, Json]) -> None:
    """Bad JSON shapes fail at the documented local validation boundary."""
    with pytest.raises(ValueError):
        compile_contract(value, inputs[1])


@pytest.mark.parametrize(
    "failure", ["missing_kind", "empty_roles", "mismatched_roles", "duplicate_column", "bad_identifier", "unqualified_column", "deep_filter"]
)
def test_catalog_and_shape_boundaries(inputs: tuple[Json, Json], failure: str) -> None:
    """Reject missing identities, ambiguous catalog names and unbounded filters."""
    source, catalog = copy.deepcopy(inputs)
    objects, catalog_obj = sequence(source), mapping(catalog)
    models = mapping(mapping(objects[0])["definition"])
    types = mapping(mapping(objects[1])["definition"])
    if failure == "missing_kind":
        objects.pop()
    elif failure == "empty_roles":
        models["permissions"] = []
        types["permissions"] = []
    elif failure == "mismatched_roles":
        sequence(types["permissions"]).pop()
    elif failure == "duplicate_column":
        sequence(catalog_obj["columns"]).append(sequence(catalog_obj["columns"])[0])
    elif failure == "bad_identifier":
        catalog_obj["table"] = "qnrs; drop schema editor"
    elif failure == "unqualified_column":
        mapping(sequence(catalog_obj["columns"])[1])["type"] = "text"
    elif failure == "deep_filter":
        select = mapping(mapping(sequence(models["permissions"])[0])["select"])
        for _index in range(18):
            select["filter"] = {"and": [select["filter"]]}
    with pytest.raises(ValueError):
        compile_contract(source, catalog)


def test_validator_dependency_and_schema_pins(tmp_path: Path) -> None:
    """Missing/revised pins cannot silently select a different validator schema."""
    from cdc_generator.core.rbac import validation

    with patch.object(validation, "version", return_value="unqualified"), pytest.raises(ValueError, match="version"):
        doctor()
    with patch.object(validation, "ASSETS", tmp_path):
        for filename in ["metadata.jsonschema", "opendd-permissions.schema.json"]:
            (tmp_path / filename).write_bytes(b"{}")
        with pytest.raises(ValueError, match="schema drift"):
            doctor()
    # Any external URI is refused instead of retrieving a resource.
    from referencing.exceptions import NoSuchResource
    from referencing.jsonschema import DRAFT7

    registry = validation.Registry[validation.Schema](retrieve=validation._offline_resource)
    with pytest.raises(NoSuchResource):
        registry.get_or_retrieve("https://example.invalid/secret")
    assert DRAFT7.id_of({"$id": "test"}) == "test"


def test_invalid_constants_and_json_types(tmp_path: Path) -> None:
    """No nonfinite constants or coerced values cross JSON/SQL trust boundaries."""
    from cdc_generator.core.rbac.validation import identifier, string

    path = tmp_path / "invalid.json"
    path.write_text("[NaN]")
    with pytest.raises(ValueError, match="constant"):
        load_json(path)
    for function in [mapping, sequence, string, identifier]:
        with pytest.raises(ValueError):
            function(None)


@pytest.mark.parametrize("failure", ["config", "g4_identity", "missing_lock", "timestamp", "commented_include"])
def test_owner_layout_boundaries(owner: Path, failure: str) -> None:
    """Canonical layout and include validation precede any artifact writes."""
    if failure == "config":
        (owner / "config.yaml").write_text("version: 2\n")
    elif failure == "g4_identity":
        path = owner / G4
        path.write_text(path.read_text().replace("name: queries", "name: other", 1))
    elif failure == "commented_include":
        (owner / TABLES / "tables.yaml").write_text("# !include editor_qnrs.yaml\n[]\n")
    before = {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}
    with pytest.raises(ValueError):
        if failure == "missing_lock":
            check(owner, owner / "source.json", owner / "catalog.json")
        else:
            emit(owner, owner / "source.json", owner / "catalog.json", "invalid" if failure == "timestamp" else "1800000000000")
    assert before == {path: path.read_bytes() for path in owner.rglob("*") if path.is_file()}


@pytest.mark.parametrize("failure", ["g4_identity", "sql_hash", "select", "mutation", "input_state"])
def test_forged_hash_cannot_hide_generated_drift(owner: Path, failure: str) -> None:
    """Recomputed contract outputs catch tampering even after a hash is rewritten."""
    from cdc_generator.core.rbac.validation import digest
    from cdc_generator.helpers.yaml_loader import save_yaml_file

    state = emit(owner, owner / "source.json", owner / "catalog.json", "1800000000000")
    if failure == "g4_identity":
        state["preserved"] = {str(G4): {"table": {"schema": "public", "name": "other"}}}
    elif failure == "sql_hash":
        relative = str(state["migration"]) + "/up.sql"
        (owner / relative).write_bytes(b"-- attacker update\n")
        mapping(state["files"])[relative] = digest((owner / relative).read_bytes())
    elif failure in {"select", "mutation"}:
        relative = str(TABLES / "editor_qnrs.yaml")
        metadata = load_yaml_file(owner / relative)
        if failure == "select":
            metadata["select_permissions"] = []
            mapping(state["select"])["sha256"] = digest(canonical([]))
        else:
            metadata["update_permissions"] = [{"role": "therapist"}]
        save_yaml_file(metadata, owner / relative)
    elif failure == "input_state":
        # This provenance-only edit changes neither SQL nor metadata, so input-state equality catches it.
        mapping(state["catalog"])["provenance"] = "tampered"
        baseline = compile_contract(state["source"], state["catalog"])
        up, down = render_migration(baseline, None)
        for filename, content in [("up.sql", up), ("down.sql", down)]:
            relative = str(state["migration"]) + "/" + filename
            (owner / relative).write_bytes(content)
            mapping(state["files"])[relative] = digest(content)
    (owner / LOCK).write_bytes(canonical(state))
    with pytest.raises(ValueError):
        check(owner, owner / "source.json", owner / "catalog.json")


def test_table_identity_upgrade_refused(contract: Contract) -> None:
    """Immutable provenance cannot reinterpret a migration as another table."""
    from dataclasses import replace

    with pytest.raises(ValueError, match="table identity"):
        render_migration(replace(contract, table="different"), contract)


def test_pinned_generation_and_source_hashes(contract: Contract) -> None:
    """A source or compiler change deliberately invalidates the reviewed hash pins."""
    from cdc_generator.core.rbac.validation import digest

    up, down = render_migration(contract, None)
    actual = {"up.sql": up.decode(), "down.sql": down.decode(), "select_permissions": select_permissions(contract), "pins": doctor()}
    assert canonical(actual) == (FIXTURES / "expected-generation.json").read_bytes()
    provenance = mapping(load_json(FIXTURES / "provenance.json"))
    for filename, expected in mapping(provenance["fixtures"]).items():
        assert digest((FIXTURES / filename).read_bytes()) == expected
