from cdc_generator.helpers.topology_runtime import (
    resolve_broker_topology,
    resolve_runtime_mode,
    resolve_topology,
    resolve_runtime_engine,
    resolve_topology_kind,
    supported_topologies_for_source_type,
    topology_supported_for_source_type,
)


def test_resolve_broker_topology_reads_broker_key() -> None:
    config = {"broker_topology": "per-server"}

    assert resolve_broker_topology(config) == "per-server"


def test_resolve_broker_topology_returns_none_when_missing() -> None:
    config: dict[str, str] = {}

    assert resolve_broker_topology(config) is None


def test_resolve_broker_topology_returns_none_for_fdw() -> None:
    config = {"topology": "fdw", "broker_topology": "per-server"}

    assert resolve_broker_topology(config, topology="fdw") is None


def test_resolve_topology_kind_defaults_brokered_from_broker_config() -> None:
    config = {"broker_topology": "shared"}

    assert resolve_topology_kind(config) == "brokered_redpanda"


def test_resolve_topology_kind_native_mssql_is_fdw_pull() -> None:
    assert resolve_topology_kind({}, runtime_mode="native", source_type="mssql") == "mssql_fdw_pull"


def test_resolve_topology_reads_explicit_topology() -> None:
    config = {"topology": "fdw"}

    assert resolve_topology(config, source_type="mssql") == "fdw"


def test_resolve_topology_native_postgres_defaults_pg_native() -> None:
    assert resolve_topology({}, runtime_mode="native", source_type="postgres") == "pg_native"


def test_resolve_runtime_mode_uses_topology_mapping() -> None:
    assert resolve_runtime_mode({}, topology="fdw", source_type="mssql") == "native"


def test_supported_topologies_for_mssql_are_redpanda_and_fdw() -> None:
    assert supported_topologies_for_source_type("mssql") == ("redpanda", "fdw")


def test_topology_supported_for_source_type_rejects_invalid_combo() -> None:
    assert topology_supported_for_source_type("fdw", "postgres") is False


def test_resolve_runtime_engine_defaults_bento_for_brokered_redpanda() -> None:
    assert resolve_runtime_engine({}, topology_kind="brokered_redpanda") == "bento"


def test_resolve_runtime_engine_defaults_postgres_native_for_native_runtime() -> None:
    assert resolve_runtime_engine({}, runtime_mode="native") == "postgres_native"