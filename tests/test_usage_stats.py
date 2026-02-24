from __future__ import annotations

from pathlib import Path

import pytest

from cdc_generator.cli import usage_stats


def test_normalize_usage_key_ignores_flag_order() -> None:
    left = usage_stats._normalize_usage_key(
        [
            "manage-services",
            "config",
            "--service",
            "adopus",
            "--inspect",
            "--schema",
            "dbo",
        ]
    )
    right = usage_stats._normalize_usage_key(
        [
            "manage-services",
            "config",
            "--schema",
            "dbo",
            "--inspect",
            "--service",
            "adopus",
        ]
    )

    assert left is not None
    assert right is not None
    assert left == right


def test_normalize_usage_key_ignores_flag_values() -> None:
    left = usage_stats._normalize_usage_key(
        [
            "msr",
            "custom-tables",
            "--add-custom-table=customer_id",
            "--service=directory",
        ]
    )
    right = usage_stats._normalize_usage_key(
        [
            "msr",
            "custom-tables",
            "--service",
            "directory",
            "--add-custom-table",
            "tenant_id",
        ]
    )

    assert left is not None
    assert right is not None
    assert left == right
    assert "--service=directory" not in left
    assert "--add-custom-table=customer_id" not in left
    assert "--service" in left
    assert "--add-custom-table" in left


def test_read_usage_file_merges_legacy_flag_values(tmp_path: Path) -> None:
    stats_file = tmp_path / "igor.efrem.txt"
    stats_file.write_text(
        (
            "cdc mss custom-tables | flags: --add-custom-table=customer_id, --service=directory\t1\n"
            "cdc msr custom-tables | flags: --service, --add-custom-table\t2\n"
        ),
        encoding="utf-8",
    )

    counts = usage_stats._read_usage_file(stats_file)
    assert counts == {
        "cdc msr custom-tables | flags: --add-custom-table, --service": 3,
    }


def test_track_usage_increments_per_user_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        usage_stats,
        "_git_user_name",
        lambda _root: "igor.efrem",
    )

    usage_stats.track_usage(
        tmp_path,
        ["manage-services", "config", "--service", "adopus", "--inspect"],
    )
    usage_stats.track_usage(
        tmp_path,
        ["manage-services", "config", "--inspect", "--service", "adopus"],
    )

    user_file = tmp_path / "_docs" / "_stats" / "igor.efrem.txt"
    content = user_file.read_text(encoding="utf-8")
    lines = [line for line in content.splitlines() if line.strip()]

    assert len(lines) == 1
    assert lines[0].endswith("\t2")


def test_generate_usage_stats_aggregates_users(tmp_path: Path) -> None:
    stats_dir = tmp_path / "_docs" / "_stats"
    stats_dir.mkdir(parents=True, exist_ok=True)

    (stats_dir / "igor.efrem.txt").write_text(
        "cdc generate\t4\ncdc manage-services config\t2\n",
        encoding="utf-8",
    )
    (stats_dir / "other.user.txt").write_text(
        "cdc generate\t1\n",
        encoding="utf-8",
    )

    report_path = usage_stats.generate_usage_stats(tmp_path)
    report = report_path.read_text(encoding="utf-8")

    assert report_path == stats_dir / "stats.md"
    assert "### igor.efrem" in report
    assert "### other.user" in report
    assert "### total" in report
    assert "| cdc generate | 5 |" in report
    assert "```toon" in report
    assert "```json" not in report


def test_generate_usage_stats_rewrites_user_file_canonical(tmp_path: Path) -> None:
    stats_dir = tmp_path / "_docs" / "_stats"
    stats_dir.mkdir(parents=True, exist_ok=True)

    user_file = stats_dir / "igor.efrem.txt"
    user_file.write_text(
        (
            "cdc mss custom-tables | flags: --add-custom-table=customer_id, --service=directory\t2\n"
            "cdc msr custom-tables | flags: --service, --add-custom-table\t1\n"
        ),
        encoding="utf-8",
    )

    usage_stats.generate_usage_stats(tmp_path)
    rewritten = user_file.read_text(encoding="utf-8")

    assert rewritten == "cdc msr custom-tables | flags: --add-custom-table, --service\t3\n"
