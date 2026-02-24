# 0006 - Pattern/CLI Audit Consolidation and Preprod Compatibility Policy

**Status:** Accepted
**Date:** 2026-02-24

## Problem

Pattern/CLI audit work was tracked in temporary documents and several historical command/path variants remained easy to reintroduce.
Without a durable decision record, future changes could drift back to legacy naming and compatibility behavior.

## Decision

1. Consolidate completed audit outcomes into a single canonical summary under `_docs/`.
2. Remove temporary audit workspace artifacts after consolidation.
3. During preprod phase, do not preserve legacy/backward compatibility by default; remove obsolete aliases/paths/docs/code when touched.
4. Keep pattern-specific behavior only where architecture requires it; unify CLI UX where intent is equivalent.

## Scope

Applies to CLI surface evolution, pattern-sensitive command behavior, and documentation governance in the generator and implementation repos during preprod.

## How to Use

Load this ADR when:
- deciding whether to keep/remove legacy command aliases or paths
- evaluating whether a pattern difference is required or can be unified
- deciding where to publish audit/refactor findings

## Impact

- Reduces command-surface drift and duplicate legacy pathways.
- Makes preprod cleanup behavior explicit and repeatable.
- Establishes `_docs/PATTERN_AND_CLI_AUDIT_SUMMARY.md` as the durable audit reference.

## Status Notes

- This preprod policy is temporary and must be revisited at production rollout.
- At production rollout, either update this ADR with the new compatibility policy or supersede it with a replacement ADR.
