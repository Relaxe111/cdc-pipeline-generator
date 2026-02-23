# 0002 - Strict Python Type Checking with pyrightconfig.json

**Status:** Accepted  
**Date:** 2026-02-06

## Problem

Type/lint strictness was inconsistent across tools and editor-dependent, reducing reliability and CI portability.

## Decision

Enforce strict Python quality via repository config:
- `pyrightconfig.json` as source of truth for Pyright/Pylance strictness
- strict MyPy and Ruff configuration in `pyproject.toml`
- avoid local editor-only settings as the primary enforcement mechanism

## Scope

Applies to Python analysis and linting configuration for the generator repository.

## How to Use

Load this ADR when modifying type-check/lint rules, discussing strictness changes, or diagnosing tool-policy conflicts.

## Impact

- Improves consistency across local and CI environments
- Increases early detection of type/design defects
- Raises initial remediation effort on legacy files

## Status Notes

Strict mode is intentional; exceptions should be justified and explicit.
