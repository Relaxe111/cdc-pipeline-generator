"""
CLI module for CDC Pipeline Generator.

This module provides command-line interface functionality for the CDC pipeline generator,
including the main entry point that can be installed as a console script.
"""

from .commands import main

__all__ = ["main"]
