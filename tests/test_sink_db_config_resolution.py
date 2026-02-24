"""Regression tests for sink DB config resolution.

Ensures sink inspect resolves target databases/schemas from
sink-groups sources (not only source-groups sources).
"""

from unittest.mock import patch

from cdc_generator.validators.manage_service.db_inspector_common import (
    get_sink_db_config,
)


def test_get_sink_db_config_uses_sink_group_sources_for_database_and_schemas() -> None:
    """Resolves sink target from sink-groups sources for nonprod alias env."""
    sink_groups = {
        "sink_asma": {
            "source_group": "adopus",
            "type": "postgres",
            "servers": {
                "nonprod": {
                    "host": "localhost",
                    "port": 5432,
                    "user": "postgres",
                    "password": "postgres",
                },
            },
            "sources": {
                "directory": {
                    "schemas": ["public", "logs"],
                    "dev": {
                        "server": "nonprod",
                        "database": "directory_dev",
                    },
                },
            },
        },
    }

    with patch(
        "cdc_generator.validators.manage_service.db_inspector_common.get_available_sinks",
        return_value=["sink_asma.directory"],
    ), patch(
        "cdc_generator.validators.manage_service.db_inspector_common._load_sink_groups",
        return_value=sink_groups,
    ), patch(
        "cdc_generator.validators.manage_service.db_inspector_common.load_server_groups",
        return_value={
            "adopus": {
                "sources": {
                    "adopus": {
                        "nonprod": {"database": "adopus_nonprod"},
                    },
                },
            },
        },
    ):
        config = get_sink_db_config("adopus", "sink_asma.directory", "nonprod")

    assert config is not None
    assert config["env_config"]["database_name"] == "directory_dev"
    assert config["schemas"] == ["public", "logs"]
