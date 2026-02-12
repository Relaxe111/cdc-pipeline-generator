"""Tests for ``manage-sink-groups`` main dispatch routing."""

from unittest.mock import Mock, patch


class TestMainDispatch:
    """Dispatches each flag to the expected handler."""

    @patch("cdc_generator.cli.sink_group.handle_create")
    @patch("sys.argv", ["cdc", "--create"])
    def test_create_dispatches_to_handler(self, mock_handler: Mock) -> None:
        from cdc_generator.cli.sink_group import main

        mock_handler.return_value = 0

        result = main()

        assert result == 0
        assert mock_handler.called

    @patch("cdc_generator.cli.sink_group.handle_add_new_sink_group")
    @patch("sys.argv", ["cdc", "--add-new-sink-group", "analytics"])
    def test_add_new_sink_group_dispatches_to_handler(
        self, mock_handler: Mock,
    ) -> None:
        from cdc_generator.cli.sink_group import main

        mock_handler.return_value = 0

        result = main()

        assert result == 0
        assert mock_handler.called

    @patch("cdc_generator.cli.sink_group.handle_list")
    @patch("sys.argv", ["cdc", "--list"])
    def test_list_dispatches_to_handler(self, mock_handler: Mock) -> None:
        from cdc_generator.cli.sink_group import main

        mock_handler.return_value = 0

        result = main()

        assert result == 0
        assert mock_handler.called

    @patch("cdc_generator.cli.sink_group.handle_info_command")
    @patch("sys.argv", ["cdc", "--info", "sink_analytics"])
    def test_info_dispatches_to_handler(self, mock_handler: Mock) -> None:
        from cdc_generator.cli.sink_group import main

        mock_handler.return_value = 0

        result = main()

        assert result == 0
        assert mock_handler.called

    @patch("cdc_generator.cli.sink_group.handle_validate_command")
    @patch("sys.argv", ["cdc", "--validate"])
    def test_validate_dispatches_to_handler(self, mock_handler: Mock) -> None:
        from cdc_generator.cli.sink_group import main

        mock_handler.return_value = 0

        result = main()

        assert result == 0
        assert mock_handler.called

    @patch("cdc_generator.cli.sink_group.handle_add_server_command")
    @patch(
        "sys.argv",
        ["cdc", "--sink-group", "sink_analytics", "--add-server", "default"],
    )
    def test_add_server_dispatches_to_handler(self, mock_handler: Mock) -> None:
        from cdc_generator.cli.sink_group import main

        mock_handler.return_value = 0

        result = main()

        assert result == 0
        assert mock_handler.called

    @patch("cdc_generator.cli.sink_group.handle_remove_server_command")
    @patch(
        "sys.argv",
        ["cdc", "--sink-group", "sink_analytics", "--remove-server", "default"],
    )
    def test_remove_server_dispatches_to_handler(self, mock_handler: Mock) -> None:
        from cdc_generator.cli.sink_group import main

        mock_handler.return_value = 0

        result = main()

        assert result == 0
        assert mock_handler.called

    @patch("cdc_generator.cli.sink_group.handle_remove_sink_group_command")
    @patch("sys.argv", ["cdc", "--remove", "sink_analytics"])
    def test_remove_dispatches_to_handler(self, mock_handler: Mock) -> None:
        from cdc_generator.cli.sink_group import main

        mock_handler.return_value = 0

        result = main()

        assert result == 0
        assert mock_handler.called


class TestDispatchBehavior:
    """Behavior checks not tied to specific handler logic."""

    @patch("cdc_generator.cli.sink_group.SinkGroupArgumentParser.print_help")
    @patch("sys.argv", ["cdc"])
    def test_no_flags_prints_help_and_returns_zero(
        self, mock_print_help: Mock,
    ) -> None:
        from cdc_generator.cli.sink_group import main

        result = main()

        assert result == 0
        assert mock_print_help.called

    @patch("cdc_generator.cli.sink_group.handle_validate_command")
    @patch("sys.argv", ["cdc", "--validate"])
    def test_nonzero_from_handler_is_propagated(
        self, mock_handler: Mock,
    ) -> None:
        from cdc_generator.cli.sink_group import main

        mock_handler.return_value = 1

        result = main()

        assert result == 1
