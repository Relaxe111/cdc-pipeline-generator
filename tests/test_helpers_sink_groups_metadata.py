"""Tests for sink-groups metadata comment generation."""

from pathlib import Path
from typing import Any

from cdc_generator.helpers.helpers_sink_groups import (
    load_sink_groups,
    save_sink_groups,
)


def test_save_sink_groups_writes_header_services_and_warnings(tmp_path: Path) -> None:
    sink_file = tmp_path / "sink-groups.yaml"
    sink_groups: dict[str, dict[str, Any]] = {
        "sink_test": {
            "source_group": "asma",
            "pattern": "db-shared",
            "type": "postgres",
            "servers": {},
            "sources": {},
        },
    }

    save_sink_groups(sink_groups, sink_file)

    content = sink_file.read_text(encoding="utf-8")
    assert "AUTO-GENERATED FILE - DO NOT EDIT DIRECTLY" in content
    assert "Use 'cdc manage-sink-groups' commands to modify this file" in content
    assert "Updated at:" in content
    assert "# Sink Group: sink_test" in content
    assert "# Type: postgres | Pattern: db-shared | Servers: 0 | Services: 0" in content
    assert "# ! Warning:" in content

    loaded = load_sink_groups(sink_file)
    assert "sink_test" in loaded


def test_save_sink_groups_includes_inherited_service_summary(tmp_path: Path) -> None:
    sink_file = tmp_path / "sink-groups.yaml"
    sink_groups: dict[str, dict[str, Any]] = {
        "sink_asma": {
            "inherits": True,
            "source_group": "asma",
            "pattern": "db-shared",
            "type": "postgres",
            "servers": {
                "default": {
                    "source_ref": "default",
                },
            },
            "inherited_sources": ["directory", "chat"],
        },
    }

    save_sink_groups(sink_groups, sink_file)

    content = sink_file.read_text(encoding="utf-8")
    assert "# Sink Group: sink_asma" in content
    assert "# Type: postgres | Pattern: db-shared | Servers: 1 | Services: 2" in content
    assert "# Services (2): chat, directory" in content
    assert "# * Warnings: none" in content
