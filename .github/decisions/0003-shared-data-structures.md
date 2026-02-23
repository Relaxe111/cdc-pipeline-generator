# 0003 - Shared Data Structures for Configuration Objects

**Status:** Proposed  
**Date:** 2026-02-06

## Problem

Configuration objects are frequently handled as raw YAML dicts, causing inconsistent access patterns and late runtime failures.

## Decision

Move toward shared typed config contracts plus centralized runtime validation:
- define TypedDict/dataclass contracts for core config objects
- load/validate once at boundaries
- pass typed objects through downstream logic

## Scope

Applies to service/server-group config handling and any code path consuming YAML config.

## How to Use

Load this ADR when introducing new config structures, refactoring YAML loaders, or reviewing config access patterns.

## Impact

- Improves type safety and schema consistency
- Reduces duplicate parsing/validation logic
- Requires coordinated refactoring and contract maintenance

## Status Notes

Proposed direction; implement incrementally starting from highest-churn config paths.
