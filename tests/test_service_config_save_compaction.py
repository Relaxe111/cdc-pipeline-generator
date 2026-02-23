"""Tests for sink table YAML compaction during service config save."""

from pathlib import Path

from pytest import MonkeyPatch

from cdc_generator.validators.manage_service import config as config_module


def test_save_service_config_compacts_repetitive_sink_table_clones(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    services_dir = tmp_path / "services"
    services_dir.mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_module, "SERVICES_DIR", services_dir)
    monkeypatch.setattr(config_module, "SERVICE_SCHEMAS_DIR", services_dir)

    config: dict[str, object] = {
        "service": "adopus",
        "sinks": {
            "sink_asma.directory": {
                "tables": {
                    "adopus.Actor": {
                        "target_exists": False,
                        "from": "dbo.Actor",
                        "replicate_structure": True,
                        "column_templates": [{"template": "customer_id"}],
                    },
                    "adopus.Address": {
                        "target_exists": False,
                        "from": "dbo.Address",
                        "replicate_structure": True,
                        "column_templates": [{"template": "customer_id"}],
                    },
                }
            }
        },
    }

    assert config_module.save_service_config("adopus", config) is True

    rendered = (services_dir / "adopus.yaml").read_text(encoding="utf-8")

    assert "&shared_defaults_" in rendered
    assert "<<: *shared_defaults_" in rendered
    assert "from: dbo.Actor" in rendered
    assert "from: dbo.Address" in rendered


def test_save_service_config_compacts_generic_similar_blocks(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    services_dir = tmp_path / "services"
    services_dir.mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_module, "SERVICES_DIR", services_dir)
    monkeypatch.setattr(config_module, "SERVICE_SCHEMAS_DIR", services_dir)

    config: dict[str, object] = {
        "service": "demo",
        "sync_profiles": {
            "profile_a": {
                "enabled": True,
                "retry_count": 3,
                "source": "alpha",
            },
            "profile_b": {
                "enabled": True,
                "retry_count": 3,
                "source": "beta",
            },
        },
    }

    assert config_module.save_service_config("demo", config) is True

    rendered = (services_dir / "demo.yaml").read_text(encoding="utf-8")

    assert "&shared_defaults_" in rendered
    assert "<<: *shared_defaults_" in rendered
    assert "source: alpha" in rendered
    assert "source: beta" in rendered
