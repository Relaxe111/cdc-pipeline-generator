# 0005 - Schema Management CLI and Type Definitions

**Status:** Proposed
**Date:** 2026-02-10

## Problem

Schema/type management is fragmented: custom sink schemas, type catalogs, and schema operations are coupled and hard to reuse.

## Decision

Introduce a schema-management direction with three parts:
- generated type-definition catalogs from inspection output
- dedicated schema-management CLI surface
- reusable custom-table definitions referenced by sink configuration

## Scope

Applies to schema/type lifecycle management and CLI ergonomics around custom sink schemas.

## How to Use

Load this ADR when designing schema CLI behavior, autocomplete flow, or type-definition storage format.

## Impact

- Improves reuse and consistency of schema/type definitions
- Reduces coupling between schema authoring and sink wiring
- Introduces additional command and data-model complexity

## Status Notes

Proposed. Implement incrementally with backward compatibility for existing `manage-service` flows.
