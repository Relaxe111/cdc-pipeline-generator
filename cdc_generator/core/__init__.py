"""Core pipeline generation logic."""

from typing import Any


def generate_customer_pipelines(*args: Any, **kwargs: Any) -> None:
    """Proxy to :mod:`cdc_generator.core.pipeline_generator` lazy import."""
    from cdc_generator.core.pipeline_generator import generate_customer_pipelines as _impl

    _impl(*args, **kwargs)


def generate_consolidated_sink(*args: Any, **kwargs: Any) -> None:
    """Proxy to :mod:`cdc_generator.core.pipeline_generator` lazy import."""
    from cdc_generator.core.pipeline_generator import generate_consolidated_sink as _impl

    _impl(*args, **kwargs)


__all__ = ["generate_consolidated_sink", "generate_customer_pipelines"]
