"""Tests for top-level CLI main() error/abort handling."""

from __future__ import annotations

from unittest.mock import patch

import click
import pytest

from cdc_generator.cli import commands


class TestCommandsMainAbortHandling:
    """Ensure Ctrl-C style aborts produce friendly output without traceback."""

    def test_main_handles_click_abort_with_friendly_message(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """click.Abort should return 130 and print a friendly cancellation message."""
        with patch(
            "sys.argv",
            ["cdc", "manage-services", "config", "directory", "--al"],
        ), patch.dict("os.environ", {}, clear=False), patch.object(
            commands._click_cli,
            "main",
            side_effect=click.Abort(),
        ):
            result = commands.main()

        assert result == 130
        output = capsys.readouterr().out
        assert "Cancelled by user" in output

    def test_main_handles_click_exception_still_works(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Existing ClickException behavior remains unchanged."""
        exc = click.ClickException("boom")
        with patch(
            "sys.argv",
            ["cdc", "manage-services", "config", "directory"],
        ), patch.dict("os.environ", {}, clear=False), patch.object(
            commands._click_cli,
            "main",
            side_effect=exc,
        ):
            result = commands.main()

        assert result == exc.exit_code
        captured = capsys.readouterr()
        assert "Error: boom" in captured.err
