"""Dataclasses for organizing function parameters."""

from dataclasses import dataclass
from typing import Any


@dataclass
class MetadataParams:
    """Parameters for metadata generation."""

    databases: list[dict[str, Any]]
    total_dbs: int
    total_tables: int
    avg_tables: int
    service_list: str
    num_services: int
    env_stats_line: str
    db_list_lines: list[str]
