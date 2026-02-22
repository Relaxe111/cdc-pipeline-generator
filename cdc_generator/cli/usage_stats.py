#!/usr/bin/env python3
"""Runtime CDC command usage tracking and report generation."""

from __future__ import annotations

import argparse
import re
import shlex
import subprocess
from collections import defaultdict
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path

_STATS_DIR_PARTS = ("_docs", "_stats")
_STATS_FILE_EXT = ".txt"
_STATS_REPORT_NAME = "stats.md"
_CDC = "cdc"
_MIN_COMMAND_PATH_PARTS = 2

_VALID_COMMAND_TOKEN_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$", re.IGNORECASE)


def _utc_now_iso() -> str:
    """Return current UTC timestamp in ISO format across Python versions."""
    utc_attr = getattr(datetime, "UTC", None)
    if isinstance(utc_attr, tzinfo):
        return datetime.now(utc_attr).isoformat(timespec="seconds")
    fallback_utc = timezone(timedelta(0))
    return datetime.now(fallback_utc).isoformat(timespec="seconds")


def _stats_dir(workspace_root: Path) -> Path:
    """Return stats directory under implementation docs."""
    return workspace_root / _STATS_DIR_PARTS[0] / _STATS_DIR_PARTS[1]


def _flag_name(token: str) -> str:
    """Return canonical flag name (drop any assigned value)."""
    if "=" in token:
        return token.split("=", 1)[0]
    return token


def _sanitize_user_name(raw_name: str) -> str:
    """Convert git user name/email into safe stats filename stem."""
    lowered = raw_name.strip().lower()
    normalized = re.sub(r"\s+", ".", lowered)
    safe = re.sub(r"[^a-z0-9._-]", "", normalized)
    return safe or "unknown-user"


def _git_user_name(workspace_root: Path) -> str:
    """Resolve user key from git config, preferring user.email local-part."""
    commands = [
        ["git", "-C", str(workspace_root), "config", "user.email"],
        ["git", "-C", str(workspace_root), "config", "user.name"],
    ]

    for command in commands:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            continue
        value = result.stdout.strip()
        if not value:
            continue
        if "@" in value:
            value = value.split("@", 1)[0]
        return _sanitize_user_name(value)

    return "unknown-user"


def _normalize_usage_key(argv_tokens: list[str]) -> str | None:
    """Normalize command + options so option order does not matter."""
    if not argv_tokens:
        return None

    tokens = [_CDC, *argv_tokens]
    try:
        joined = " ".join(shlex.quote(token) for token in tokens)
        parsed_tokens = shlex.split(joined, posix=True)
    except ValueError:
        parsed_tokens = tokens

    command_path: list[str] = []
    cursor = 0
    while cursor < len(parsed_tokens):
        token = parsed_tokens[cursor]
        if token.startswith("-") and token != "-":
            break
        command_path.append(token)
        cursor += 1

    if len(command_path) < _MIN_COMMAND_PATH_PARTS:
        return None

    for token in command_path[1:]:
        if _VALID_COMMAND_TOKEN_RE.match(token) is None:
            return None

    flags: list[str] = []
    positional_args: list[str] = []

    while cursor < len(parsed_tokens):
        token = parsed_tokens[cursor]
        if token in {"", "\\"}:
            cursor += 1
            continue

        if token.startswith("-"):
            flags.append(_flag_name(token))
            if "=" in token:
                cursor += 1
                continue

            next_index = cursor + 1
            if next_index < len(parsed_tokens):
                next_token = parsed_tokens[next_index]
                if not next_token.startswith("-") and next_token not in {"", "\\"}:
                    cursor += 2
                    continue

            cursor += 1
            continue

        positional_args.append(token)
        cursor += 1

    normalized = " ".join(command_path)
    if positional_args:
        normalized += " | args: " + ", ".join(sorted(positional_args))
    if flags:
        normalized += " | flags: " + ", ".join(sorted(set(flags)))
    return normalized


def _canonicalize_stored_key(command: str) -> str:
    """Canonicalize persisted command key to current normalization rules."""
    if " | " not in command:
        return command

    parts = command.split(" | ")
    base = parts[0]
    args_part = ""
    flags_part = ""

    for part in parts[1:]:
        if part.startswith("args: "):
            args_part = part.removeprefix("args: ")
        elif part.startswith("flags: "):
            flags_part = part.removeprefix("flags: ")

    normalized = base
    if args_part:
        args_tokens = [token.strip() for token in args_part.split(",") if token.strip()]
        if args_tokens:
            normalized += " | args: " + ", ".join(sorted(args_tokens))

    if flags_part:
        flags_tokens = [token.strip() for token in flags_part.split(",") if token.strip()]
        normalized_flags = sorted({_flag_name(token) for token in flags_tokens})
        if normalized_flags:
            normalized += " | flags: " + ", ".join(normalized_flags)

    return normalized


def _read_usage_file(file_path: Path) -> dict[str, int]:
    """Read per-user usage counts from tab-separated file."""
    if not file_path.exists():
        return {}

    results: dict[str, int] = {}
    for line in file_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if "\t" not in stripped:
            continue
        command, count_raw = stripped.rsplit("\t", 1)
        try:
            count = int(count_raw)
        except ValueError:
            continue
        if count <= 0:
            continue
        canonical_command = _canonicalize_stored_key(command)
        results[canonical_command] = results.get(canonical_command, 0) + count
    return results


def _write_usage_file(file_path: Path, counts: dict[str, int]) -> None:
    """Persist per-user usage counts as tab-separated file."""
    lines = [
        command + "\t" + str(count)
        for command, count in sorted(
            counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]
    file_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def track_usage(workspace_root: Path, argv_tokens: list[str]) -> None:
    """Track one command invocation for the current git user."""
    command_key = _normalize_usage_key(argv_tokens)
    if command_key is None:
        return

    stats_dir = _stats_dir(workspace_root)
    stats_dir.mkdir(parents=True, exist_ok=True)

    user_name = _git_user_name(workspace_root)
    file_path = stats_dir / (user_name + _STATS_FILE_EXT)

    counts = _read_usage_file(file_path)
    counts[command_key] = counts.get(command_key, 0) + 1
    _write_usage_file(file_path, counts)


def _load_all_user_counts(
    stats_dir: Path,
    *,
    rewrite_canonical: bool = False,
) -> dict[str, dict[str, int]]:
    """Load command counts from all user stats files."""
    users: dict[str, dict[str, int]] = {}
    if not stats_dir.exists():
        return users

    for file_path in sorted(stats_dir.glob("*" + _STATS_FILE_EXT)):
        user_name = file_path.stem
        counts = _read_usage_file(file_path)
        users[user_name] = counts
        if rewrite_canonical and counts:
            _write_usage_file(file_path, counts)

    return users


def _render_stats_markdown(users: dict[str, dict[str, int]]) -> str:
    """Render AI + human readable usage stats markdown."""
    totals: dict[str, int] = defaultdict(int)
    for command_counts in users.values():
        for command, count in command_counts.items():
            totals[command] += count

    generated_at = _utc_now_iso()

    lines: list[str] = [
        "# Command Usage Stats",
        "",
        "Generated from runtime counters in `_docs/_stats/{user}.txt`.",
        "",
        "## Machine Readable",
        "",
        "```json",
        "{",
        f'  "generated_at_utc": "{generated_at}",',
        '  "users": {',
    ]

    user_names = sorted(users)
    for index, user_name in enumerate(user_names):
        suffix = "," if index < len(user_names) - 1 else ""
        lines.append(f'    "{user_name}": {{')
        user_items = sorted(users[user_name].items(), key=lambda item: (-item[1], item[0]))
        for command_index, (command, count) in enumerate(user_items):
            command_suffix = "," if command_index < len(user_items) - 1 else ""
            escaped = command.replace("\\", "\\\\").replace('"', '\\"')
            lines.append(f'      "{escaped}": {count}{command_suffix}')
        lines.append("    }" + suffix)

    lines.extend(
        [
            "  },",
            '  "total": {',
        ]
    )

    total_items = sorted(totals.items(), key=lambda item: (-item[1], item[0]))
    for index, (command, count) in enumerate(total_items):
        suffix = "," if index < len(total_items) - 1 else ""
        escaped = command.replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f'    "{escaped}": {count}{suffix}')

    lines.extend(
        [
            "  }",
            "}",
            "```",
            "",
            "## Human Readable",
            "",
            "Generated: " + generated_at,
            "",
        ]
    )

    for user_name in user_names:
        lines.append("### " + user_name)
        lines.append("")
        lines.append("| command | count |")
        lines.append("| --- | ---: |")
        for command, count in sorted(users[user_name].items(), key=lambda item: (-item[1], item[0])):
            lines.append("| " + command.replace("|", "\\|") + " | " + str(count) + " |")
        lines.append("")

    lines.append("### total")
    lines.append("")
    lines.append("| command | count |")
    lines.append("| --- | ---: |")
    for command, count in total_items:
        lines.append("| " + command.replace("|", "\\|") + " | " + str(count) + " |")
    lines.append("")

    return "\n".join(lines)


def generate_usage_stats(workspace_root: Path) -> Path:
    """Generate stats markdown from per-user usage files."""
    stats_dir = _stats_dir(workspace_root)
    users = _load_all_user_counts(
        stats_dir,
        rewrite_canonical=True,
    )
    report = _render_stats_markdown(users)

    output_file = stats_dir / _STATS_REPORT_NAME
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(report, encoding="utf-8")
    return output_file


def main() -> int:
    """Standalone CLI for generating usage stats markdown."""
    parser = argparse.ArgumentParser(description="Generate usage stats markdown from _docs/_stats")
    parser.add_argument("--workspace-root", type=Path, default=Path.cwd())
    args = parser.parse_args()

    output = generate_usage_stats(args.workspace_root.resolve())
    print("Generated usage stats: " + str(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
