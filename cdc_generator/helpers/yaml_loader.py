#!/usr/bin/env python3
"""
Type-safe YAML loader with runtime validation.
Provides validated ruamel.yaml instance with proper type hints.
"""

from pathlib import Path
from typing import TYPE_CHECKING, Protocol, TextIO, Union, cast

if TYPE_CHECKING:
    from cdc_generator.helpers.ruamel_yaml_stub import YAMLInterface
else:
    # At runtime, import actual YAML class
    from ruamel.yaml import YAML as YAMLInterface  # type: ignore[assignment]


# Recursive type for nested YAML structures
ConfigValue = Union[str, int, float, bool, None, 'ConfigDict', list['ConfigValue']]
ConfigDict = dict[str, ConfigValue]


class YAMLLoader(Protocol):
    """Protocol for YAML loader with comment preservation."""
    preserve_quotes: bool
    default_flow_style: bool

    def load(self, stream: TextIO) -> ConfigValue:
        """Load YAML from stream."""
        ...


def _validate_yaml_loader(obj: object) -> None:
    """Runtime validation: Ensure YAML object has expected interface.
    
    Raises:
        AttributeError: If required attributes/methods are missing
        TypeError: If methods are not callable
    """
    required_attrs = ['load', 'preserve_quotes', 'default_flow_style']
    for attr in required_attrs:
        if not hasattr(obj, attr):
            raise AttributeError(f"YAML object missing required attribute: {attr}")

    load_method = obj.load
    if not callable(load_method):
        raise TypeError("YAML.load is not callable")


def _create_yaml_loader() -> YAMLLoader:
    """Create and validate YAML loader instance.
    
    Returns:
        Validated YAML loader with comment preservation enabled
        
    Raises:
        AttributeError: If YAML object is missing required interface
        TypeError: If YAML methods are not callable
    """
    # Initialize ruamel.yaml to preserve comments
    yaml_obj: YAMLInterface = YAMLInterface()
    yaml_obj.preserve_quotes = True
    yaml_obj.default_flow_style = False

    # Validate interface at runtime
    _validate_yaml_loader(yaml_obj)

    # After validation, cast to our Protocol type
    return cast(YAMLLoader, yaml_obj)


# Create singleton validated YAML loader instance
yaml: YAMLLoader = _create_yaml_loader()


def load_yaml_file(file_path: Path) -> ConfigDict:
    """Load YAML file with type safety.
    
    ruamel.yaml's load() is safe by default (unlike PyYAML's load()).
    It does not execute arbitrary Python code from YAML content.
    
    Args:
        file_path: Path to YAML file to load
        
    Returns:
        Configuration dictionary loaded from YAML
        
    Raises:
        FileNotFoundError: If file does not exist
    """
    if not file_path.exists():
        raise FileNotFoundError(f"YAML file not found: {file_path}")

    with open(file_path) as f:
        raw: ConfigValue = yaml.load(f)
        return cast(ConfigDict, raw)

