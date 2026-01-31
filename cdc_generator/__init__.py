"""
CDC Pipeline Generator

A library for generating Redpanda Connect pipeline configurations
for Change Data Capture (CDC) workflows.
"""

__version__ = "0.1.0"

from cdc_generator.core.pipeline_generator import generate_pipelines
from cdc_generator.cli.commands import CLI

__all__ = [
    "generate_pipelines",
    "CLI",
]
