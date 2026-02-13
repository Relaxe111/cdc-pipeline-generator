"""Tests for manage-source-groups dispatch logic.

Tests the main() function routing behavior by mocking handlers and validation.
Since main() internally calls argparse, we test by mocking sys.argv and handlers.
"""

from __future__ import annotations

from unittest.mock import Mock, patch

# ═══════════════════════════════════════════════════════════════════════════
# Main dispatch routing
# ═══════════════════════════════════════════════════════════════════════════

class TestMainDispatch:
    """Tests for main() dispatch logic."""

    @patch("cdc_generator.cli.source_group.handle_info")
    @patch("cdc_generator.cli.source_group.validate_manage_server_group_flags")
    @patch("sys.argv", ["cdc", "--info"])
    def test_info_flag_dispatches_to_handler(
        self,
        mock_validator: Mock,
        mock_handler: Mock,
    ) -> None:
        """--info flag → dispatches to handle_info()."""
        from cdc_generator.cli.source_group import main

        mock_result = Mock()
        mock_result.level = "ok"
        mock_result.message = None
        mock_validator.return_value = mock_result
        mock_handler.return_value = 0

        result = main()

        assert mock_handler.called
        assert result == 0

    @patch("cdc_generator.cli.source_group.handle_add_server")
    @patch("cdc_generator.cli.source_group.validate_manage_server_group_flags")
    @patch("sys.argv", ["cdc", "--add-server", "newsrv"])
    def test_add_server_dispatches_to_handler(
        self,
        mock_validator: Mock,
        mock_handler: Mock,
    ) -> None:
        """--add-server → dispatches to handle_add_server()."""
        from cdc_generator.cli.source_group import main

        mock_result = Mock()
        mock_result.level = "ok"
        mock_validator.return_value = mock_result
        mock_handler.return_value = 0

        result = main()

        assert mock_handler.called
        assert result == 0

    @patch("cdc_generator.cli.source_group.handle_remove_server")
    @patch("cdc_generator.cli.source_group.validate_manage_server_group_flags")
    @patch("sys.argv", ["cdc", "--remove-server", "oldsrv"])
    def test_remove_server_dispatches_to_handler(
        self,
        mock_validator: Mock,
        mock_handler: Mock,
    ) -> None:
        """--remove-server → dispatches to handle_remove_server()."""
        from cdc_generator.cli.source_group import main

        mock_result = Mock()
        mock_result.level = "ok"
        mock_validator.return_value = mock_result
        mock_handler.return_value = 0

        result = main()

        assert mock_handler.called
        assert result == 0

    @patch("cdc_generator.cli.source_group.handle_list_servers")
    @patch("cdc_generator.cli.source_group.validate_manage_server_group_flags")
    @patch("sys.argv", ["cdc", "--list-servers"])
    def test_list_servers_dispatches_to_handler(
        self,
        mock_validator: Mock,
        mock_handler: Mock,
    ) -> None:
        """--list-servers → dispatches to handle_list_servers()."""
        from cdc_generator.cli.source_group import main

        mock_result = Mock()
        mock_result.level = "ok"
        mock_validator.return_value = mock_result
        mock_handler.return_value = 0

        result = main()

        assert mock_handler.called
        assert result == 0

    @patch("cdc_generator.cli.source_group.handle_set_kafka_topology")
    @patch("cdc_generator.cli.source_group.validate_manage_server_group_flags")
    @patch("sys.argv", ["cdc", "--set-kafka-topology", "shared"])
    def test_set_topology_dispatches_to_handler(
        self,
        mock_validator: Mock,
        mock_handler: Mock,
    ) -> None:
        """--set-kafka-topology → dispatches to handle_set_kafka_topology()."""
        from cdc_generator.cli.source_group import main

        mock_result = Mock()
        mock_result.level = "ok"
        mock_validator.return_value = mock_result
        mock_handler.return_value = 0

        result = main()

        assert mock_handler.called
        assert result == 0

    @patch("cdc_generator.cli.source_group.handle_add_ignore_pattern")
    @patch("cdc_generator.cli.source_group.validate_manage_server_group_flags")
    @patch("sys.argv", ["cdc", "--add-to-ignore-list", "tempdb"])
    def test_add_db_excludes_dispatches_to_handler(
        self,
        mock_validator: Mock,
        mock_handler: Mock,
    ) -> None:
        """--add-to-ignore-list → dispatches to handle_add_ignore_pattern()."""
        from cdc_generator.cli.source_group import main

        mock_result = Mock()
        mock_result.level = "ok"
        mock_validator.return_value = mock_result
        mock_handler.return_value = 0

        result = main()

        assert mock_handler.called
        assert result == 0

    @patch("cdc_generator.cli.source_group.handle_add_schema_exclude")
    @patch("cdc_generator.cli.source_group.validate_manage_server_group_flags")
    @patch("sys.argv", ["cdc", "--add-to-schema-excludes", "temp"])
    def test_add_schema_excludes_dispatches_to_handler(
        self,
        mock_validator: Mock,
        mock_handler: Mock,
    ) -> None:
        """--add-to-schema-excludes → dispatches to handle_add_schema_exclude()."""
        from cdc_generator.cli.source_group import main

        mock_result = Mock()
        mock_result.level = "ok"
        mock_validator.return_value = mock_result
        mock_handler.return_value = 0

        result = main()

        assert mock_handler.called
        assert result == 0


# ═══════════════════════════════════════════════════════════════════════════
# Validation errors
# ═══════════════════════════════════════════════════════════════════════════

class TestDispatchValidation:
    """Tests for dispatch validation and error handling."""

    @patch("cdc_generator.cli.source_group.print_error")
    @patch("cdc_generator.cli.source_group.validate_manage_server_group_flags")
    @patch("sys.argv", ["cdc", "--add-server", "test"])
    def test_validation_error_returns_exit_code_1(
        self,
        mock_validator: Mock,
        mock_print_error: Mock,
    ) -> None:
        """Flag validation fails → returns exit code 1."""
        from cdc_generator.cli.source_group import main

        mock_result = Mock()
        mock_result.level = "error"
        mock_result.message = "Test error"
        mock_result.suggestion = None
        mock_validator.return_value = mock_result

        result = main()

        assert result == 1
        assert mock_print_error.called

    @patch("cdc_generator.cli.source_group.print")
    @patch("cdc_generator.cli.source_group.print_error")
    @patch("cdc_generator.cli.source_group.validate_manage_server_group_flags")
    @patch("sys.argv", ["cdc", "--set-kafka-topology", "shared"])
    def test_validation_error_with_suggestion_prints_both(
        self,
        mock_validator: Mock,
        mock_print_error: Mock,
        mock_print: Mock,
    ) -> None:
        """Validation error with suggestion → prints both message and suggestion."""
        from cdc_generator.cli.source_group import main

        mock_result = Mock()
        mock_result.level = "error"
        mock_result.message = "Error message"
        mock_result.suggestion = "Try this instead"
        mock_validator.return_value = mock_result

        result = main()

        assert result == 1
        assert mock_print_error.called
        assert mock_print.called  # For suggestion


# ═══════════════════════════════════════════════════════════════════════════
# Inline handlers
# ═══════════════════════════════════════════════════════════════════════════

class TestInlineHandlers:
    """Tests for inline handlers that don't delegate to separate functions."""

    @patch("cdc_generator.cli.source_group.print_info")
    @patch("cdc_generator.cli.source_group.load_schema_exclude_patterns")
    @patch("cdc_generator.cli.source_group.validate_manage_server_group_flags")
    @patch("sys.argv", ["cdc", "--list-schema-excludes"])
    def test_list_schema_excludes_inline_handler(
        self,
        mock_validator: Mock,
        mock_load: Mock,
        mock_print_info: Mock,
    ) -> None:
        """--list-schema-excludes → inline handler lists patterns."""
        from cdc_generator.cli.source_group import main

        mock_result = Mock()
        mock_result.level = "ok"
        mock_validator.return_value = mock_result
        mock_load.return_value = ["dbo", "sys"]

        result = main()

        assert result == 0
        assert mock_print_info.called

    @patch("cdc_generator.cli.source_group.print_info")
    @patch("cdc_generator.cli.source_group.load_database_exclude_patterns")
    @patch("cdc_generator.cli.source_group.validate_manage_server_group_flags")
    @patch("sys.argv", ["cdc", "--list-ignore-patterns"])
    def test_list_ignore_patterns_inline_handler(
        self,
        mock_validator: Mock,
        mock_load: Mock,
        mock_print_info: Mock,
    ) -> None:
        """--list-ignore-patterns → inline handler lists patterns."""
        from cdc_generator.cli.source_group import main

        mock_result = Mock()
        mock_result.level = "ok"
        mock_validator.return_value = mock_result
        mock_load.return_value = ["test", "tempdb"]

        result = main()

        assert result == 0
        assert mock_print_info.called

    @patch("cdc_generator.cli.source_group.print_info")
    @patch("cdc_generator.validators.manage_server_group.config.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.config.get_single_server_group")
    @patch("cdc_generator.cli.source_group.validate_manage_server_group_flags")
    @patch("sys.argv", ["cdc", "--view-services"])
    def test_view_services_inline_handler(
        self,
        mock_validator: Mock,
        mock_get_single: Mock,
        mock_load: Mock,
        mock_print_info: Mock,
    ) -> None:
        """--view-services → inline handler displays sources."""
        from cdc_generator.cli.source_group import main

        mock_result = Mock()
        mock_result.level = "ok"
        mock_validator.return_value = mock_result
        mock_load.return_value = {"mygroup": {"sources": {"db1": {}}}}
        mock_get_single.return_value = {"sources": {"db1": {"dev": {"database": "db1_dev"}}}}

        result = main()

        assert result == 0
        assert mock_print_info.called


# ═══════════════════════════════════════════════════════════════════════════
# Handler return values
# ═══════════════════════════════════════════════════════════════════════════

class TestHandlerReturnValues:
    """Tests for handler return value propagation."""

    @patch("cdc_generator.cli.source_group.handle_add_server")
    @patch("cdc_generator.cli.source_group.validate_manage_server_group_flags")
    @patch("sys.argv", ["cdc", "--add-server", "newsrv"])
    def test_handler_returns_nonzero_propagates(
        self,
        mock_validator: Mock,
        mock_handler: Mock,
    ) -> None:
        """Handler returns non-zero exit code → propagated to caller."""
        from cdc_generator.cli.source_group import main

        mock_result = Mock()
        mock_result.level = "ok"
        mock_validator.return_value = mock_result
        mock_handler.return_value = 1  # Error exit code

        result = main()

        assert result == 1

    @patch("cdc_generator.cli.source_group.handle_add_server")
    @patch("cdc_generator.cli.source_group.validate_manage_server_group_flags")
    @patch("sys.argv", ["cdc", "--add-server", "newsrv"])
    def test_handler_returns_zero_success(
        self,
        mock_validator: Mock,
        mock_handler: Mock,
    ) -> None:
        """Handler returns 0 → success."""
        from cdc_generator.cli.source_group import main

        mock_result = Mock()
        mock_result.level = "ok"
        mock_validator.return_value = mock_result
        mock_handler.return_value = 0

        result = main()

        assert result == 0
