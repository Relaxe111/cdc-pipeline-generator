# 0004 - Runtime Bloblang Validation with Sample Data

**Status:** Proposed  
**Date:** 2026-02-10

## Problem

Static Bloblang linting catches syntax issues but misses runtime logic errors such as wrong field references and type misuse.

## Decision

Add optional runtime validation using generated sample payloads:
- generate representative sample data per table/schema
- execute mappings/transform rules against that data
- report runtime failures before deployment

## Scope

Applies to mapping/transform validation quality gates; does not replace static linting.

## How to Use

Load this ADR when designing Bloblang validation flow, runtime validation CLI flags, or CI validation gates.

## Impact

- Improves detection of non-syntax mapping errors
- Increases validation confidence pre-release
- Adds runtime cost and requires realistic sample generation discipline

## Status Notes

Proposed. Treat as optional enhancement path until implementation is finalized.
