"""Tests for fish shell completions file (cdc.fish).

Validates structural correctness of the completions template to prevent
regressions where inline completion blocks silently fail due to fish
shell escaping issues.

Root cause context:
- Inside fish `complete -a "(code)"` blocks, escaped quotes `\\"` and
  `\\$` do NOT behave as expected — they produce literal characters
  instead of shell escapes, causing silent completion failures.
- `$cmd[(math $i + 1)]` inside inline `-a "()"` blocks is evaluated
  at source-time when `$cmd`/`$i` don't exist, producing warnings.
- Both issues are fixed by extracting logic into named functions,
  which are parsed but not executed at source-time.
"""

import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CDC_FISH = Path(__file__).resolve().parent.parent / (
    "cdc_generator/templates/init/cdc.fish"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_fish_file() -> str:
    """Read the cdc.fish template file."""
    return CDC_FISH.read_text(encoding="utf-8")


def _extract_function_bodies(content: str) -> list[tuple[str, str]]:
    """Extract (name, body) pairs for all `function ... end` blocks."""
    results: list[tuple[str, str]] = []
    pattern = re.compile(
        r"^function\s+(\S+)\s.*?\n(.*?)^end\b",
        re.MULTILINE | re.DOTALL,
    )
    for m in pattern.finditer(content):
        results.append((m.group(1), m.group(2)))
    return results


def _extract_inline_completion_blocks(content: str) -> list[tuple[int, str]]:
    r"""Extract (line_number, block_content) for all inline `-a "(…)"` blocks.

    These are the blocks inside `complete -c cdc ... -a "(code)"` that
    are NOT simple function calls like `-a "(__helper_func)"`.
    """
    results: list[tuple[int, str]] = []
    # Match -a "(...)" blocks that span multiple lines
    # (single-line function calls like -a "(__func)" are fine)
    lines = content.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]
        # Look for -a "( that opens a multi-line block
        match = re.search(r'-a\s+"\(', line)
        if match and not re.search(r'-a\s+"\([^"]*\)"', line):
            # Multi-line block — collect until closing )"
            block_start = i + 1  # 1-indexed
            block_lines = [line]
            i += 1
            while i < len(lines) and ')"' not in lines[i]:
                block_lines.append(lines[i])
                i += 1
            if i < len(lines):
                block_lines.append(lines[i])
            block_content = "\n".join(block_lines)
            results.append((block_start, block_content))
        i += 1
    return results


def _lines_outside_functions(content: str) -> str:
    """Return file content with all function bodies removed.

    This lets us check for patterns that should only appear inside
    named functions, not in top-level/inline completion code.
    """
    # Replace function bodies with empty markers
    pattern = re.compile(
        r"^function\s+\S+\s.*?\n.*?^end\b",
        re.MULTILINE | re.DOTALL,
    )
    return pattern.sub("# <function-removed>", content)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestNoEscapedQuotesInInlineBlocks:
    """Escaped quotes inside `-a "()"` blocks silently break completions.

    In fish shell, `\\"` inside `-a "(code)"` produces a literal
    backslash+quote instead of escaping. This causes `test -n \\"$var\\"`
    to always be true (non-empty string) but `$var` to not expand,
    making the Python command receive an empty string.

    Fix: use named helper functions instead of inline blocks with variables.
    """

    def test_no_escaped_quotes_in_inline_blocks(self) -> None:
        r"""Inline `-a "()"` blocks must not contain `\"`."""
        content = _read_fish_file()
        blocks = _extract_inline_completion_blocks(content)

        violations: list[str] = []
        for line_num, block in blocks:
            if '\\"' in block:
                violations.append(
                    f"Line {line_num}: inline block contains "
                    f"escaped quotes"
                )

        assert violations == [], (
            "Found escaped quotes in inline completion blocks. "
            "Use named helper functions instead.\n"
            + "\n".join(violations)
        )

    def test_no_escaped_dollar_in_inline_blocks(self) -> None:
        r"""Inline `-a "()"` blocks must not contain `\$` (except descriptions).

        `\$` in fish means a literal dollar sign, not variable expansion.
        """
        content = _read_fish_file()
        blocks = _extract_inline_completion_blocks(content)

        violations: list[str] = []
        for line_num, block in blocks:
            # Check each line in the block for \$ that isn't in a -d description
            for offset, bline in enumerate(block.split("\n")):
                if "\\$" in bline and " -d " not in bline:
                    violations.append(
                        f"Line {line_num + offset}: "
                        f"contains \\$ outside description"
                    )

        assert violations == [], (
            "Found \\$ in inline completion blocks (not in descriptions). "
            "Use named helper functions instead.\n"
            + "\n".join(violations)
        )


class TestNoIndexArithmeticOutsideFunctions:
    """$cmd[(math $i + 1)] outside named functions causes index warnings.

    Fish evaluates `-a "(code)"` blocks at source-time when `$cmd` and
    `$i` don't exist, producing "Invalid index value" warnings.
    Named functions are only parsed (not executed) at source-time.
    """

    def test_no_math_index_outside_functions(self) -> None:
        """No `(math $i + 1)` patterns outside function bodies."""
        content = _read_fish_file()
        outside = _lines_outside_functions(content)

        matches = re.findall(r".*\(math\s+\$\w+\s*\+.*", outside)
        assert matches == [], (
            "Found (math ...) index arithmetic outside named functions. "
            "Move to helper functions to avoid source-time evaluation.\n"
            + "\n".join(matches)
        )

    def test_no_cmd_index_outside_functions(self) -> None:
        """No `$cmd[...]` array indexing outside function bodies."""
        content = _read_fish_file()
        outside = _lines_outside_functions(content)

        matches = re.findall(r".*\$cmd\[.*", outside)
        assert matches == [], (
            "Found $cmd[...] indexing outside named functions.\n"
            + "\n".join(matches)
        )


class TestFishCompletionStructure:
    """General structural checks for the completions file."""

    def test_file_exists(self) -> None:
        """The cdc.fish template must exist."""
        assert CDC_FISH.exists(), f"Missing: {CDC_FISH}"

    def test_file_not_empty(self) -> None:
        """The cdc.fish template must not be empty."""
        content = _read_fish_file()
        assert len(content.strip()) > 100

    def test_has_main_subcommands(self) -> None:
        """Core subcommands must be registered."""
        content = _read_fish_file()
        for subcmd in [
            "manage-service",
            "manage-source-groups",
            "manage-sink-groups",
            "manage-column-templates",
            "generate",
            "scaffold",
        ]:
            assert f'-a "{subcmd}"' in content, (
                f"Missing subcommand: {subcmd}"
            )

    def test_helper_functions_defined(self) -> None:
        """All referenced helper functions must be defined."""
        content = _read_fish_file()
        functions = _extract_function_bodies(content)
        defined = {name for name, _ in functions}

        # Find all function calls in -a "(__func)" or -n "__func" patterns
        referenced = set(re.findall(r"__cdc_\w+", content))

        missing = referenced - defined
        # __fish_* functions are fish builtins, not ours
        missing = {f for f in missing if f.startswith("__cdc_")}

        assert missing == set(), (
            f"Referenced but not defined: {missing}"
        )

    def test_all_set_keyword_present(self) -> None:
        """Every variable assignment inside functions must use `set`."""
        content = _read_fish_file()
        functions = _extract_function_bodies(content)

        violations: list[str] = []
        # Known variable names that should be assigned with `set`
        var_names = [
            "sink_key",
            "sink_table",
            "table_spec",
            "service_name",
            "sink_group",
            "table_key",
            "target_table",
            "add_sink_table",
        ]
        for func_name, body in functions:
            for var in var_names:
                # Look for `var_name $cmd[` without `set` prefix
                pattern = re.compile(
                    rf"^\s+{re.escape(var)}\s+\$",
                    re.MULTILINE,
                )
                for m in pattern.finditer(body):
                    line_content = m.group(0).strip()
                    if not line_content.startswith("set"):
                        violations.append(
                            f"Function {func_name}: "
                            f"missing 'set' in: {line_content}"
                        )

        assert violations == [], (
            "Found variable assignments without 'set' keyword.\n"
            + "\n".join(violations)
        )

    def test_inline_blocks_only_call_simple_commands(self) -> None:
        """Remaining inline blocks should only call python3 or functions.

        Any inline block that sets local variables and uses loops/conditionals
        should be refactored into a named function.
        """
        content = _read_fish_file()
        blocks = _extract_inline_completion_blocks(content)

        violations: list[str] = []
        for line_num, block in blocks:
            # These patterns indicate logic that should be in a function
            if "for " in block and "in (seq" in block:
                violations.append(
                    f"Line {line_num}: inline block contains "
                    f"for-loop (move to function)"
                )
            if "set -l " in block and "set -l " in block:
                # Count how many set -l statements — more than 0 means
                # variable-dependent logic that should be a function
                set_count = block.count("set -l ")
                if set_count > 0:
                    # Allow simple blocks that just call
                    # __cdc_get_service_name, but flag complex ones
                    if "for " in block or "while " in block:
                        violations.append(
                            f"Line {line_num}: inline block has "
                            f"{set_count} local vars + loops"
                        )

        assert violations == [], (
            "Found complex inline blocks that should be helper functions.\n"
            + "\n".join(violations)
        )


class TestSinkCompletionRegistered:
    """Regression test: --sink must produce completions for existing sinks.

    The --sink flag completion was silently broken because it used
    inline `\\"$service_name\\"` which doesn't work in fish `-a "()"`.
    """

    def test_sink_uses_function_not_inline(self) -> None:
        """--sink completion must use a function call, not inline code."""
        content = _read_fish_file()
        # Find the --sink completion line (not --add-sink, --remove-sink, etc.)
        sink_pattern = re.compile(
            r'complete.*-l sink -d.*-a\s+"([^"]*)"'
        )
        match = sink_pattern.search(content)
        assert match is not None, "Missing --sink completion"

        completion_arg = match.group(1)
        # Should be a simple function call like (__cdc_complete_sink_keys)
        assert completion_arg.startswith("("), (
            f"--sink completion should call a function, got: {completion_arg}"
        )
        assert "__cdc_" in completion_arg, (
            f"--sink completion should call a __cdc_ helper, "
            f"got: {completion_arg}"
        )

    def test_remove_sink_uses_function(self) -> None:
        """--remove-sink completion must use a function call."""
        content = _read_fish_file()
        pattern = re.compile(
            r'complete.*-l remove-sink -d.*-a\s+"([^"]*)"'
        )
        match = pattern.search(content)
        assert match is not None, "Missing --remove-sink completion"
        assert "__cdc_" in match.group(1)

    def test_inspect_sink_uses_function(self) -> None:
        """--inspect-sink completion must use a function call."""
        content = _read_fish_file()
        pattern = re.compile(
            r'complete.*-l inspect-sink -d.*-a\s+"([^"]*)"'
        )
        match = pattern.search(content)
        assert match is not None, "Missing --inspect-sink completion"
        assert "__cdc_" in match.group(1)

    def test_sink_key_helper_calls_autocompletions(self) -> None:
        """The __cdc_complete_sink_keys function must call Python."""
        content = _read_fish_file()
        functions = _extract_function_bodies(content)
        func_dict = dict(functions)

        assert "__cdc_complete_sink_keys" in func_dict, (
            "Missing __cdc_complete_sink_keys function"
        )
        body = func_dict["__cdc_complete_sink_keys"]
        assert "--list-sink-keys" in body
        assert "__cdc_get_service_name" in body
