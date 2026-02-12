"""Tests for --inspect command without --service flag."""

from argparse import Namespace

from cdc_generator.cli.service import _dispatch_validation


class TestInspectAllServices:
    """Test that --inspect works without --service flag."""

    def test_dispatch_validation_handles_inspect_without_service(self) -> None:
        """Test _dispatch_validation calls _dispatch_inspect for --inspect without --service."""
        args = Namespace(
            inspect=True,
            service=None,
            all=False,
            schema=None,
            save=False,
            inspect_sink=False,
            validate_config=False,
            validate_hierarchy=False,
            validate_bloblang=False,
            generate_validation=False,
            list_source_tables=False,
            create_service=False,
        )

        # Should return int (from handle_inspect), not None
        result = _dispatch_validation(args)
        assert result is not None
        assert isinstance(result, int)

    def test_dispatch_validation_handles_inspect_sink_without_service(self) -> None:
        """Test inspect-sink dispatch path without service.

        Verifies that `_dispatch_validation()` forwards to `_dispatch_inspect()`.
        """
        args = Namespace(
            inspect=False,
            service=None,
            all=False,
            schema=None,
            save=False,
            inspect_sink=True,
            validate_config=False,
            validate_hierarchy=False,
            validate_bloblang=False,
            generate_validation=False,
            list_source_tables=False,
            create_service=False,
        )

        # Should return int (from handle_inspect_sink error), not None
        result = _dispatch_validation(args)
        assert result is not None
        assert isinstance(result, int)
        assert result == 1  # Expected error: --inspect-sink requires --service

    def test_dispatch_validation_validates_config_without_service(self) -> None:
        """Test _dispatch_validation handles --validate-config without --service."""
        args = Namespace(
            inspect=False,
            service=None,
            all=False,
            schema=None,
            save=False,
            inspect_sink=False,
            validate_config=True,
            validate_hierarchy=False,
            validate_bloblang=False,
            generate_validation=False,
            list_source_tables=False,
            create_service=False,
        )

        # Should return int (from handle_validate_config), not None
        result = _dispatch_validation(args)
        assert result is not None
        assert isinstance(result, int)

    def test_dispatch_validation_returns_none_for_unhandled_commands(self) -> None:
        """Test _dispatch_validation returns None for commands requiring --service."""
        args = Namespace(
            inspect=False,
            service=None,
            all=False,
            schema=None,
            save=False,
            inspect_sink=False,
            validate_config=False,
            validate_hierarchy=True,  # Requires --service
            validate_bloblang=False,
            generate_validation=False,
            list_source_tables=False,
            create_service=False,
        )

        # Should return None (not handled without --service)
        result = _dispatch_validation(args)
        assert result is None
