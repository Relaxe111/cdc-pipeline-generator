#!/usr/bin/env python3
"""
Type stubs for ruamel.yaml YAML class.
Provides only the interface we actually use in this project.
"""

from typing import TextIO


class YAMLInterface:
    """Type stub for ruamel.yaml.YAML class interface.
    
    This defines only the attributes and methods we actually use.
    Not meant to be instantiated - only for type checking.
    
    Note: ruamel.yaml's load() is safe by default (unlike PyYAML).
    It does not execute arbitrary Python code from YAML content.
    """
    preserve_quotes: bool
    default_flow_style: bool

    def __init__(self) -> None:
        """Initialize YAML loader."""
        ...

    def load(self, stream: TextIO) -> object:
        """Load YAML from stream with comment preservation.
        
        Safe by default - does not execute arbitrary Python code.
        """
        ...

