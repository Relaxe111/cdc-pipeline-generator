"""Unit tests for manage-service parser/main semantics."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from cdc_generator.cli.service import _build_parser, main


class TestServiceParserHints:
    """Friendly parser error messages for missing option values."""

    @pytest.mark.parametrize(
        "flag",
        [
            "--service",
            "--add-source-table",
            "--sink",
            "--from",
            "--target",
        ],
    )
    def test_missing_value_shows_friendly_hint(
        self, flag: str, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Flags in _FLAG_HINTS print a clear requires-value hint."""
        parser = _build_parser()

        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args([flag])

        assert exc_info.value.code == 1
        output = capsys.readouterr().out
        assert f"{flag} requires a value" in output

    def test_invalid_target_exists_choice_exits_1(
        self, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Invalid --target-exists choice returns exit code 1."""
        parser = _build_parser()

        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--target-exists", "maybe"])

        assert exc_info.value.code == 1
        output = capsys.readouterr().out
        assert "invalid choice" in output

    def test_track_columns_accepts_multiple_values_in_one_flag(self) -> None:
        """--track-columns should parse space-separated list after single flag."""
        parser = _build_parser()
        args = parser.parse_args([
            "--service", "proxy",
            "--source-table", "public.customer_user",
            "--track-columns",
            "public.customer_user.user_id",
            "public.customer_user.customer_id",
        ])

        assert args.track_columns == [[
            "public.customer_user.user_id",
            "public.customer_user.customer_id",
        ]]


class TestMainServiceAssignment:
    """main() precedence and assignment semantics."""

    def test_create_service_sets_service_before_dispatch(self) -> None:
        """--create-service value is assigned to `args.service`."""
        with patch(
            "sys.argv", ["manage-service", "--create-service", "newservice"],
        ), patch(
            "cdc_generator.cli.service._auto_detect_service",
            side_effect=lambda args: args,
        ), patch(
            "cdc_generator.cli.service._dispatch",
            return_value=0,
        ) as dispatch_mock:
            result = main()

        assert result == 0
        dispatched_args = dispatch_mock.call_args.args[0]
        assert dispatched_args.service == "newservice"

    def test_create_service_overrides_positional_service_name(self) -> None:
        """--create-service takes precedence over positional service_name."""
        with patch(
            "sys.argv",
            ["manage-service", "oldservice", "--create-service", "newservice"],
        ), patch(
            "cdc_generator.cli.service._auto_detect_service",
            side_effect=lambda args: args,
        ), patch(
            "cdc_generator.cli.service._dispatch",
            return_value=0,
        ) as dispatch_mock:
            result = main()

        assert result == 0
        dispatched_args = dispatch_mock.call_args.args[0]
        assert dispatched_args.service == "newservice"

    def test_auto_detect_failure_returns_1(self) -> None:
        """main() returns 1 when _auto_detect_service returns None."""
        with patch(
            "sys.argv", ["manage-service"],
        ), patch(
            "cdc_generator.cli.service._auto_detect_service",
            return_value=None,
        ), patch(
            "cdc_generator.cli.service._dispatch",
        ) as dispatch_mock:
            result = main()

        assert result == 1
        dispatch_mock.assert_not_called()

    def test_no_service_no_flags_dispatches(self) -> None:
        """main() with no service or flags still dispatches (handle_no_service)."""
        with patch(
            "sys.argv", ["manage-service"],
        ), patch(
            "cdc_generator.cli.service._auto_detect_service",
            side_effect=lambda args: args,
        ), patch(
            "cdc_generator.cli.service._dispatch",
            return_value=0,
        ) as dispatch_mock:
            result = main()

        assert result == 0
        dispatch_mock.assert_called_once()