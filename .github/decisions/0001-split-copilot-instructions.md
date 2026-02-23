# 0001 - Split Copilot Instructions for Token Efficiency

**Status:** Accepted  
**Date:** 2026-02-06

## Problem

One large instruction file mixed always-needed rules with rarely-needed details, causing context bloat and weaker routing.

## Decision

Adopt a router + scoped guides model:
- keep `copilot-instructions.md` minimal
- move detailed rules into `copilot-instructions-*` files
- use explicit context triggers to load only relevant guides

## Scope

Applies to instruction organization and loading strategy across workspace and generator guidance.

## How to Use

Load this ADR when changing instruction structure, adding new instruction files, or adjusting auto-load triggers.

## Impact

- Reduces always-loaded instruction weight
- Improves signal quality of the router file
- Adds dependency on correct trigger maintenance

## Status Notes

Current instruction layout follows this ADR and should remain the baseline.
