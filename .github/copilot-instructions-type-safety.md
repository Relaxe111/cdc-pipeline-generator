# Type Safety Guidelines

## Non-Negotiables

- Never add `# type: ignore`, `# noqa`, or suppression-only workarounds.
- Fix type issues at source in our code, not at call sites.
- Use fully typed signatures (`dict[str, Any]`, `list[str]`, `Optional[...]`) for all touched functions.
- Use package imports only (for example from `cdc_generator...`), never fragile relative shortcuts.

## Resolution Order (Use This Sequence)

1. Fix our function/type definitions where the bad type originates.
2. Add or tighten type annotations in shared interfaces.
3. Install/update third-party stubs in `Dockerfile.dev` (for example `types-PyYAML`, `types-pymssql`).
4. Use `cast(...)` only for unavoidable third-party boundaries.

If a fix requires suppression, stop and refactor instead.

## Shared Config Structures (Required)

- Define shared `TypedDict`/dataclass contracts for service/server-group config.
- Validate YAML shape at load time in one centralized loader.
- Pass validated typed objects through the codebase; do not pass raw YAML dicts.
- Do not duplicate parsing/validation logic across modules.

## Required Patterns

- Keep one source of truth for config schema.
- Prefer explicit typed return values from loaders.
- Catch schema drift via validation failures early.

## Implementation Notes

- Keep type stub installs in `Dockerfile.dev`.
- When touching files, resolve all relevant Pylance/Ruff type issues in those files.
