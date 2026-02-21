"""Tests for source-group-driven customer environment migration."""

from pathlib import Path

from pytest import MonkeyPatch

from cdc_generator.helpers.service_config import (
    get_all_customers,
    load_service_config,
    merge_customer_config,
)


def _write_yaml(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_merge_customer_config_derives_environments_from_source_groups(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    services_dir = tmp_path / "services"
    services_dir.mkdir(parents=True)

    _write_yaml(
        tmp_path / "source-groups.yaml",
        """
adopus:
  server_group_type: db-per-tenant
  servers:
    default:
      host: mssql-host
      port: 1433
      user: sa
      password: secret
      kafka_bootstrap_servers: kafka:9092
  sources:
    AvProd:
      prod:
        server: default
        database: AdOpusAVProd
""".strip()
        + "\n",
    )

    monkeypatch.chdir(tmp_path)

    service_config: dict[str, object] = {
        "service": "adopus",
        "server_group": "adopus",
        "shared": {"source_tables": [], "ignore_tables": []},
    }

    merged = merge_customer_config(service_config, "avprod")

    environments = merged["environments"]
    assert isinstance(environments, dict)
    assert "prod" in environments

    prod = environments["prod"]
    assert isinstance(prod, dict)
    assert prod["database"] == {"name": "AdOpusAVProd"}
    assert prod["topic_prefix"] == "prod.avprod.AdOpusAVProd"
    assert prod["existing_mssql"] is True

    mssql = prod["mssql"]
    assert isinstance(mssql, dict)
    assert mssql["host"] == "mssql-host"
    assert mssql["port"] == 1433
    assert mssql["user"] == "sa"
    assert mssql["password"] == "secret"

    kafka = prod["kafka"]
    assert isinstance(kafka, dict)
    assert kafka["bootstrap_servers"] == "kafka:9092"


def test_merge_customer_config_without_source_entry_returns_empty_environments(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    services_dir = tmp_path / "services"
    services_dir.mkdir(parents=True)

    _write_yaml(
        tmp_path / "source-groups.yaml",
        """
adopus:
  server_group_type: db-per-tenant
  sources:
    AVProd:
      schemas:
      - dbo
""".strip()
        + "\n",
    )

    monkeypatch.chdir(tmp_path)

    service_config: dict[str, object] = {
        "service": "adopus",
        "server_group": "adopus",
        "shared": {"source_tables": [], "ignore_tables": []},
    }

    merged = merge_customer_config(service_config, "avprod")
    assert merged["environments"] == {}


def test_load_service_config_derives_customers_from_source_groups(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    services_dir = tmp_path / "services"
    services_dir.mkdir(parents=True)

    _write_yaml(
        services_dir / "adopus.yaml",
        """
adopus:
  server_group: adopus
  shared:
    source_tables: []
""".strip()
        + "\n",
    )

    _write_yaml(
        tmp_path / "source-groups.yaml",
        """
adopus:
  pattern: db-per-tenant
  sources:
    AVProd:
      default:
        server: default
        database: AdOpusAVProd
        customer_id: 3b2afe16-cf52-4c86-9825-9de9e31768f6
    Avansas:
      default:
        server: default
        database: AdOpusTest
""".strip()
        + "\n",
    )

    monkeypatch.chdir(tmp_path)

    config = load_service_config("adopus")
    customers = config.get("customers")
    assert isinstance(customers, list)
    assert customers == [
        {
            "name": "avansas",
            "schema": "avansas",
            "customer_id": None,
        },
        {
            "name": "avprod",
            "schema": "avprod",
            "customer_id": "3b2afe16-cf52-4c86-9825-9de9e31768f6",
        },
    ]


def test_get_all_customers_reads_derived_customers(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    services_dir = tmp_path / "services"
    services_dir.mkdir(parents=True)

    _write_yaml(
        services_dir / "adopus.yaml",
        """
adopus:
  server_group: adopus
  shared:
    source_tables: []
""".strip()
        + "\n",
    )

    _write_yaml(
        tmp_path / "source-groups.yaml",
        """
adopus:
  server_group_type: db-per-tenant
  sources:
    AVProd:
      default:
        server: default
        database: AdOpusAVProd
    Avansas:
      default:
        server: default
        database: AdOpusTest
""".strip()
        + "\n",
    )

    monkeypatch.chdir(tmp_path)

    assert get_all_customers() == ["avansas", "avprod"]
