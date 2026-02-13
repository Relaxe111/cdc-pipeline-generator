#!/usr/bin/env python3
"""Service-group CLI entrypoint.

Compatibility wrapper around the source-group implementation.
This keeps internals stable while exposing service-group naming externally.
"""

from cdc_generator.cli.source_group import main

__all__ = ["main"]


if __name__ == "__main__":
    raise SystemExit(main())
