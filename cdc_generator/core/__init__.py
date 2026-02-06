"""Core pipeline generation logic."""

from cdc_generator.core.pipeline_generator import (
    generate_consolidated_sink,
    generate_customer_pipelines,
)

__all__ = ["generate_consolidated_sink", "generate_customer_pipelines"]
