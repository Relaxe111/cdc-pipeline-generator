#!/usr/bin/env python3
"""Module entry point for autocompletions package.

Allows running: python -m cdc_generator.helpers.autocompletions [args]
"""

from cdc_generator.helpers.autocompletions import main

if __name__ == '__main__':
    raise SystemExit(main())
