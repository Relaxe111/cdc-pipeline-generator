#!/usr/bin/env python3
"""Build per-user CDC command usage stats from git history.

The report is generated from added lines in git patches and grouped by git user.
Commands are normalized so flag/argument order does not affect grouping.
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

_LOG_PREFIX = "@@@"
_LOG_SEPARATOR = "\x1f"
_CDC_WORD = "cdc"
_LINE_SPLIT_RE = re.compile(r"\s*(?:&&|\|\||;)\s*")
_COMMAND_LINE_RE = re.compile(
    r"^\s*(?:[-*]\s+)?(?:[`\"'])?(?:\$\s+)?(?:fish>\s+)?cdc\b",
    re.IGNORECASE,
)
_AUTHOR_PARTS = 2
_MIN_COMMAND_PATH_PARTS = 2
_VALID_COMMAND_TOKEN_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$", re.IGNORECASE)
_PLACEHOLDER_MARKERS = ("<", ">", "{", "}", "...", "[", "]")

_ALLOWED_COMMAND_ROOTS = frozenset(
    {
        "generate",
        "help",
        "init",
        "manage-column-templates",
        "manage-migrations",
        "manage-pipelines",
        "manage-server-group",
        "manage-service",
        "manage-service-schema",
        "manage-services",
        "manage-sink-groups",
        "manage-source-groups",
        "mm",
        "mp",
        "ms",
        "msc",
        "msig",
        "msog",
        "mss",
        "nuke-local",
        "reload-cdc-autocompletions",
        "reset-local",
        "scaffold",
        "setup-local",
        "test",
        "test-coverage",
        "validate",
    }
)


def _allowed_command_roots() -> set[str]:
    """Return valid first subcommands after `cdc`.

    Includes canonical commands from the current CLI plus a small legacy set
    used in older docs/history.
    """
    return set(_ALLOWED_COMMAND_ROOTS)


@dataclass(frozen=True)
class GitUser:
    """Stable user identity derived from git author metadata."""

    display_name: str
    email: str
    key: str


def _run_git_history(repo_root: Path) -> str:
    """Return git log with patch lines and author metadata."""
    command = [
        "git",
        "-C",
        str(repo_root),
        "log",
        "--no-color",
        "--pretty=format:" + _LOG_PREFIX + "%an" + _LOG_SEPARATOR + "%ae",
        "-p",
        "--unified=0",
    ]
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        error = result.stderr.strip() or "unknown git error"
        raise RuntimeError("Unable to read git history: " + error)
    return result.stdout


def _user_from_author(author_name: str, author_email: str) -> GitUser:
    """Create user identity with git-email-local fallback key."""
    email = author_email.strip().lower()
    name = author_name.strip()
    if "@" in email:
        local_part = email.split("@", 1)[0]
        key = local_part.strip() or name.lower().replace(" ", ".")
    else:
        key = name.lower().replace(" ", ".")
    return GitUser(display_name=name or key, email=email, key=key)


def _extract_candidate_commands(line: str) -> list[str]:
    """Extract command-like segments containing cdc invocations."""
    if _CDC_WORD not in line:
        return []
    if _COMMAND_LINE_RE.match(line) is None:
        return []

    first_cdc = line.find(_CDC_WORD)
    if first_cdc < 0:
        return []

    cleaned = line[first_cdc:].strip().strip("`\"' ")
    if "`" in cleaned:
        cleaned = cleaned.split("`", 1)[0].strip()
    if "#" in cleaned:
        cleaned = cleaned.split("#", 1)[0].strip()

    segments = _LINE_SPLIT_RE.split(cleaned)
    candidates: list[str] = []
    for segment in segments:
        if _CDC_WORD not in segment:
            continue
        cdc_pos = segment.find(_CDC_WORD)
        if cdc_pos < 0:
            continue
        tail = segment[cdc_pos:].strip().rstrip("\\").strip("`\"' ")
        if not tail:
            continue
        if not tail.startswith("cdc ") and tail != "cdc":
            continue
        candidates.append(tail)
    return candidates


def _canonicalize_command(raw_command: str) -> str | None:
    """Normalize command preserving command path and sorting flags/args.

    - Keeps `cdc` command path order as typed.
    - Normalizes flags + values independent of input order.
    - Normalizes positional args independent of input order.
    """
    text = raw_command.strip()
    if not text or any(marker in text for marker in _PLACEHOLDER_MARKERS):
        return None

    try:
        tokens = shlex.split(text, posix=True)
    except ValueError:
        tokens = text.split()

    if not tokens:
        return None
    if tokens[0] != _CDC_WORD:
        if _CDC_WORD not in tokens:
            return None
        cdc_index = tokens.index(_CDC_WORD)
        tokens = tokens[cdc_index:]

    command_path: list[str] = []
    cursor = 0
    while cursor < len(tokens):
        token = tokens[cursor]
        if token.startswith("-") and token != "-":
            break
        command_path.append(token)
        cursor += 1

    root_is_valid = (
        bool(command_path)
        and len(command_path) >= _MIN_COMMAND_PATH_PARTS
        and bool(_VALID_COMMAND_TOKEN_RE.match(command_path[1]))
        and command_path[1] in _allowed_command_roots()
    )
    if not root_is_valid:
        return None

    def _is_value_token(token_value: str) -> bool:
        return bool(token_value and token_value != "\\" and not token_value.startswith("-"))

    def _consume_option(option_token: str, start_index: int) -> tuple[str, int]:
        if "=" in option_token:
            return option_token, start_index + 1

        next_index = start_index + 1
        if next_index < len(tokens):
            next_token = tokens[next_index]
            if _is_value_token(next_token):
                return option_token + "=" + next_token, next_index + 1

        return option_token, start_index + 1

    flags: list[str] = []
    positional_args: list[str] = []
    while cursor < len(tokens):
        token = tokens[cursor]
        if token in {"", "\\"}:
            cursor += 1
            continue

        if token.startswith("-"):
            option, cursor = _consume_option(token, cursor)
            flags.append(option)
            continue

        positional_args.append(token)
        cursor += 1

    normalized = " ".join(command_path)
    if positional_args:
        normalized += " | args: " + ", ".join(sorted(positional_args))
    if flags:
        normalized += " | flags: " + ", ".join(sorted(flags))
    return normalized


def _collect_counts(repo_root: Path) -> dict[str, dict[str, int]]:
    """Aggregate normalized command counts by git-user key."""
    log_text = _run_git_history(repo_root)
    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    active_user: GitUser | None = None
    for raw_line in log_text.splitlines():
        if raw_line.startswith(_LOG_PREFIX):
            payload = raw_line.removeprefix(_LOG_PREFIX)
            parts = payload.split(_LOG_SEPARATOR, 1)
            active_user = (
                _user_from_author(parts[0], parts[1])
                if len(parts) == _AUTHOR_PARTS
                else None
            )
            continue

        if active_user is None:
            continue
        if not raw_line.startswith("+") or raw_line.startswith("+++"):
            continue

        added_line = raw_line[1:]
        for candidate in _extract_candidate_commands(added_line):
            canonical = _canonicalize_command(candidate)
            if canonical is None:
                continue
            counts[active_user.key][canonical] += 1

    return {user: dict(commands) for user, commands in counts.items()}


def _render_markdown(counts: dict[str, dict[str, int]]) -> str:
    """Render AI+human readable markdown report."""
    timestamp = datetime.now(UTC).isoformat(timespec="seconds")

    totals: dict[str, int] = defaultdict(int)
    for command_counts in counts.values():
        for command, value in command_counts.items():
            totals[command] += value

    serializable: dict[str, object] = {
        "generated_at_utc": timestamp,
        "users": counts,
        "total": dict(sorted(totals.items(), key=lambda item: (-item[1], item[0]))),
    }

    lines: list[str] = [
        "# CDC Command Usage Stats",
        "",
        "This report counts normalized `cdc ...` command occurrences by git user.",
        "Normalization keeps command path and ignores option/argument ordering.",
        "",
        "## Machine Readable",
        "",
        "```json",
        json.dumps(serializable, indent=2, sort_keys=True),
        "```",
        "",
        "## Human Readable",
        "",
        "Generated: " + timestamp,
        "",
    ]

    for user_key in sorted(counts):
        lines.append("### " + user_key)
        lines.append("")
        lines.append("| command | count |")
        lines.append("| --- | ---: |")
        for command, value in sorted(
            counts[user_key].items(),
            key=lambda item: (-item[1], item[0]),
        ):
            lines.append("| " + command.replace("|", "\\|") + " | " + str(value) + " |")
        lines.append("")

    lines.append("### total")
    lines.append("")
    lines.append("| command | count |")
    lines.append("| --- | ---: |")
    for command, value in sorted(totals.items(), key=lambda item: (-item[1], item[0])):
        lines.append("| " + command.replace("|", "\\|") + " | " + str(value) + " |")
    lines.append("")

    return "\n".join(lines)


def build_stats(repo_root: Path, output_file: Path) -> int:
    """Build and write stats markdown."""
    counts = _collect_counts(repo_root)
    content = _render_markdown(counts)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(content, encoding="utf-8")
    print("Wrote command stats to " + str(output_file))
    return 0


def main() -> int:
    """CLI entrypoint."""
    default_repo = Path(__file__).resolve().parents[2]
    default_output = default_repo / "_docs" / "command-stats" / "stats.md"

    parser = argparse.ArgumentParser(
        description="Build per-user CDC command usage stats from git history",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=default_repo,
        help="Repository root containing .git",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output,
        help="Output markdown file path",
    )
    args = parser.parse_args()

    return build_stats(args.repo_root.resolve(), args.output.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
