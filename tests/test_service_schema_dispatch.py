"""Unit tests for ``manage-service-schema`` CLI dispatch and parser."""

from unittest.mock import Mock, patch

import pytest

from cdc_generator.cli.service_schema import _build_parser, _dispatch, main


class TestParserHints:
    """Friendly parser errors."""

    @pytest.mark.parametrize(
        "flag",
        ["--service", "--add-custom-table", "--show", "--remove-custom-table", "--column"],
    )
    def test_missing_value_shows_hint(
        self, flag: str, capsys: pytest.CaptureFixture[str],
    ) -> None:
        parser = _build_parser()

        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args([flag])

        assert exc_info.value.code == 1
        output = capsys.readouterr().out
        assert f"{flag} requires a value" in output


class TestDispatch:
    """Dispatch routing behavior."""

    @patch("cdc_generator.cli.service_schema._handle_list_services")
    def test_list_services_path(self, mock_handler: Mock) -> None:
        mock_handler.return_value = 0
        args = _build_parser().parse_args(["--list-services"])

        result = _dispatch(args)

        assert result == 0
        assert mock_handler.called

    @patch("cdc_generator.cli.service_schema._handle_list")
    def test_list_with_service_path(self, mock_handler: Mock) -> None:
        mock_handler.return_value = 0
        args = _build_parser().parse_args(["--service", "chat", "--list"])

        result = _dispatch(args)

        assert result == 0
        mock_handler.assert_called_once_with("chat")

    def test_list_without_service_returns_1(self) -> None:
        args = _build_parser().parse_args(["--list"])
        assert _dispatch(args) == 1

    def test_no_service_returns_1(self) -> None:
        args = _build_parser().parse_args([])
        assert _dispatch(args) == 1


class TestServiceActionDispatch:
    """Service-scoped action routing."""

    @patch("cdc_generator.cli.service_schema._handle_add_custom_table")
    def test_add_custom_table_path(self, mock_handler: Mock) -> None:
        mock_handler.return_value = 0
        args = _build_parser().parse_args(
            [
                "--service",
                "chat",
                "--add-custom-table",
                "public.audit_log",
                "--column",
                "id:uuid:pk",
            ],
        )

        result = _dispatch(args)

        assert result == 0
        mock_handler.assert_called_once_with(
            "chat", "public.audit_log", ["id:uuid:pk"],
        )

    @patch("cdc_generator.cli.service_schema._handle_show")
    def test_show_path(self, mock_handler: Mock) -> None:
        mock_handler.return_value = 0
        args = _build_parser().parse_args(
            ["--service", "chat", "--show", "public.audit_log"],
        )

        result = _dispatch(args)

        assert result == 0
        mock_handler.assert_called_once_with("chat", "public.audit_log")

    @patch("cdc_generator.cli.service_schema._handle_remove")
    def test_remove_path(self, mock_handler: Mock) -> None:
        mock_handler.return_value = 0
        args = _build_parser().parse_args(
            ["--service", "chat", "--remove-custom-table", "public.audit_log"],
        )

        result = _dispatch(args)

        assert result == 0
        mock_handler.assert_called_once_with("chat", "public.audit_log")

    @patch("cdc_generator.cli.service_schema._handle_list")
    def test_default_service_action_is_list(self, mock_handler: Mock) -> None:
        mock_handler.return_value = 0
        args = _build_parser().parse_args(["--service", "chat"])

        result = _dispatch(args)

        assert result == 0
        mock_handler.assert_called_once_with("chat")


class TestMain:
    """Main entry point behavior."""

    @patch("cdc_generator.cli.service_schema._dispatch")
    @patch("sys.argv", ["manage-service-schema", "--list-services"])
    def test_main_returns_dispatch_result(self, mock_dispatch: Mock) -> None:
        mock_dispatch.return_value = 0

        result = main()

        assert result == 0
        assert mock_dispatch.called
