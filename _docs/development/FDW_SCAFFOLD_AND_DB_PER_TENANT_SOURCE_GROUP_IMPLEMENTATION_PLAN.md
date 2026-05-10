# FDW Scaffold And db-per-tenant Source Group Implementation Plan

## Purpose

Define the generator changes needed so `cdc scaffold` produces the correct shape for `fdw` implementations and so `db-per-tenant` source groups can be fully instantiated through CDC CLI commands without manual YAML edits.

This plan applies directly to implementations like `adopus-cdc-pipeline` where:

- topology is `fdw`
- source type is `mssql`
- the implementation repository owns configuration, SQL, procedures, functions, and schema/versioning assets
- runtime execution is external and owned by `asma-cdc-orchestrator`

## Problem Statement

The current scaffold behavior still assumes the historical brokered/runtime-heavy model.

For `fdw` mode this is wrong in four ways:

1. The scaffold generates runtime assets that do not belong in the implementation repository.
2. The generated docs still describe a generator dev container, Redpanda, Bento source, and Bento sink as if they were part of the implementation runtime.
3. `db-per-tenant` service creation depends on `database_ref`, but the CLI does not expose a command to set it.
4. Generated `.env.example` files are not kept aligned with later server additions and sink placeholders.

## Recommendation

Treat scaffold output as a topology-specific asset profile, not as a single universal implementation shape.

Recommended direction:

- `redpanda` topology keeps brokered runtime assets
- `fdw` topology generates configuration-first repository layout only
- `pg_native` topology follows its own native runtime profile

For `fdw`, the implementation repository should default to no runtime compose file at all.

Optional local development assets should be explicit and minimal:

- local PostgreSQL when a local FDW target or local source-postgres workflow is needed
- pgAdmin when database inspection is needed
- no Redpanda
- no Bento runtime containers
- no generator dev container references

## Target Outcome

After this work, a fresh `fdw` scaffold should:

1. Create only configuration and database-shaping assets by default.
2. Clearly document that runtime belongs to `asma-cdc-orchestrator`.
3. Allow `db-per-tenant` source-group setup to be completed through CLI commands.
4. Keep `.env.example` consistent with generated YAML placeholders.

## Scope

In scope:

- `cdc scaffold`
- scaffold update behavior
- scaffold templates and generated docs
- `manage-source-groups` command surface for `database_ref`
- `.env.example` generation and synchronization
- manual instantiation flow for `fdw` + `db-per-tenant`

Out of scope:

- implementing `asma-cdc-orchestrator`
- changing external implementation YAML manually as the default workflow
- redesigning service/table schema semantics unrelated to scaffold/setup

## Design Principles

### 1. Topology decides runtime assets

The topology already exists as a first-class concept in [helpers/topology_runtime.py](../../cdc_generator/helpers/topology_runtime.py).

Scaffolded assets must follow the resolved topology profile.

### 2. Implementation repositories stay configuration-first in `fdw`

For `fdw`, the repository should contain:

- `source-groups.yaml`
- `sink-groups.yaml`
- `services/`
- `generated/schemas/`
- `generated/pg-migrations/`
- SQL procedures/functions/versioning assets
- documentation for setup and database-owned runtime assumptions

It should not imply that the repo itself hosts CDC runtime services.

### 3. db-per-tenant setup must be CLI-complete

If `create-service` requires `database_ref`, there must be a CLI command to set it.

The user should not need to choose between an incomplete CLI workflow and manual YAML edits for a standard setup path.

### 4. Generated env templates must match generated placeholders

Any placeholder written to `source-groups.yaml`, `sink-groups.yaml`, or scaffolded config must be representable in the root `.env.example`.

## Current Gaps

### Gap 1: `fdw` scaffold generates runtime-heavy assets

Current scaffold creation in [create.py](../../cdc_generator/validators/manage_server_group/scaffolding/create.py) always creates runtime-oriented assets such as:

- `docker-compose.yml`
- `Dockerfile.pg17-tds-fdw`
- `Dockerfile.pgadmin`
- `pgadmin/*`

Current scaffold update in [update.py](../../cdc_generator/validators/manage_server_group/scaffolding/update.py) can also recreate those files when missing.

### Gap 2: templates still describe brokered runtime

Current templates in [templates.py](../../cdc_generator/validators/manage_server_group/scaffolding/templates.py) still assume:

- generator dev container
- Redpanda
- Bento source/sink runtime
- `docker compose up -d` as the main implementation workflow

That is incompatible with `fdw` topology ownership.

### Gap 3: no CLI command for `database_ref`

`db-per-tenant` service creation in [service_creator.py](../../cdc_generator/validators/manage_service/service_creator.py) requires `database_ref`, but `manage-source-groups` currently has no `--set-database-ref` command in [source_group.py](../../cdc_generator/cli/source_group.py).

Result: `db-per-tenant` scaffold can be created, servers can be added, and discovery can run, but the setup still stops before service creation.

### Gap 4: `.env.example` drift after server/sink growth

The root `.env.example` is generated during scaffold, but later source-server and sink-server additions can leave YAML placeholders that are missing from the fresh env template.

## Proposed Solution

## Phase 1: Introduce Topology-Specific Scaffold Profiles

### Phase 2 Goal

Replace one universal scaffold output with topology-aware asset bundles.

### Proposed profiles

- `brokered_redpanda`
- `mssql_fdw_pull`
- `pg_logical`

These names should map from the existing topology resolution helpers rather than introducing a second competing model.

### `mssql_fdw_pull` default asset bundle

Create by default:

- `source-groups.yaml`
- `services/`
- `generated/schemas/`
- `generated/pg-migrations/`
- `_docs/`
- `.vscode/settings.json`
- `.gitignore`
- `.env.example`

Do not create by default:

- `docker-compose.yml`
- `Dockerfile.pg17-tds-fdw`
- `Dockerfile.pgadmin`
- `pgadmin/*`
- Redpanda-related assets
- Bento runtime examples as required runtime guidance

### Implementation approach

Refactor scaffold generation so `create.py` builds outputs from named bundles instead of unconditionally writing every runtime file.

Expected code areas:

- [create.py](../../cdc_generator/validators/manage_server_group/scaffolding/create.py)
- [update.py](../../cdc_generator/validators/manage_server_group/scaffolding/update.py)
- [templates.py](../../cdc_generator/validators/manage_server_group/scaffolding/templates.py)

## Phase 2: Add `database_ref` CLI Support

### Phase 3 Goal

Make `db-per-tenant` source-group setup complete through CLI commands.

### New command

Add:

```bash
cdc manage-source-groups --set-database-ref AdOpusTest
```

### Behavior

The command should:

- write `database_ref` at source-group root
- validate against discovered `sources.*.<env>.database` when possible
- allow pre-discovery assignment with a warning
- display current value under `--info`
- participate in shell completions and tests

### Implementation areas

- [source_group.py](../../cdc_generator/cli/source_group.py)
- [flag_validator.py](../../cdc_generator/validators/flag_validator.py)
- [validators/manage_server_group](../../cdc_generator/validators/manage_server_group)
- CLI tests and fish completion tests

## Phase 3: Add Env Template Synchronization

### Goal

Keep root `.env.example` aligned with placeholders emitted into YAML.

### Phase 3 Required Behavior

When source servers, sink servers, or topology-specific placeholders are introduced, the env template must be able to represent them.

### Preferred options

Option A:

```bash
cdc manage-source-groups --sync-env-template
```

Option B:

```bash
cdc scaffold --update-env-template
```

Option C:

Make `--add-server` and relevant sink-group operations update `.env.example` deterministically.

Recommended choice: explicit sync command plus deterministic automatic update where safe.

## Phase 4: Rewrite FDW Scaffold Documentation

### Phase 4 Goal

Ensure generated docs describe the actual runtime ownership model.

### Documentation changes

For `fdw` scaffold, generated docs should state:

- the implementation repository does not host CDC runtime by default
- runtime execution belongs to `asma-cdc-orchestrator`
- this repository owns configuration, SQL, schema shaping, functions, procedures, and migrations
- local compose is optional and only for explicit local DB tooling workflows

### Files to update

- scaffold README template
- project structure template
- env variables template doc
- CLI flow doc template

## Phase 5: Make Scaffold Update Respect Topology Profile

### Phase 5 Goal

Prevent `scaffold --update` from reintroducing runtime files into `fdw` implementations.

### Phase 5 Required Behavior

If an implementation is `fdw`:

- update missing config/docs assets only
- do not recreate runtime bundle files unless the user explicitly requests that bundle

## Manual Instantiation Flow

This section defines the target user workflow for a fresh `fdw` + `db-per-tenant` setup.

## Commands That Exist Today

### 1. Scaffold the implementation

```bash
cdc scaffold adopus \
  --pattern db-per-tenant \
  --source-type mssql \
  --topology fdw \
  --extraction-pattern '^AdOpus(?P<customer>.+?)(?:Dev)?$'
```

### 2. Add additional source servers

```bash
cdc manage-source-groups --add-server prod_primary
cdc manage-source-groups --add-server prod_avprod
cdc manage-source-groups --add-server prod_fretex
```

### 3. Set validation environment

```bash
cdc manage-source-groups --set-validation-env default
```

### 4. Discover databases

```bash
cdc manage-source-groups --update default
cdc manage-source-groups --update prod_primary
cdc manage-source-groups --update prod_avprod
cdc manage-source-groups --update prod_fretex
```

### 5. Inspect discovered state

```bash
cdc manage-source-groups --info
cdc manage-source-groups --list-servers
cdc manage-source-groups --list-envs
```

## Command That Must Be Added

```bash
cdc manage-source-groups --set-database-ref AdOpusTest
```

Without this command, the standard `db-per-tenant` setup path is incomplete.

## Commands That Should Work After Phase 2

### 6. Create service configuration

```bash
cdc manage-services config \
  --service adopus \
  --create-service adopus
```

### 7. Inspect schemas through the configured validation database

```bash
cdc manage-services config \
  --service adopus \
  --inspect --all
```

### 8. Add tracked source tables

```bash
cdc manage-services config \
  --service adopus \
  --add-source-table dbo.Actor \
  --primary-key actno
```

## Acceptance Criteria

This work is complete when all of the following are true.

### Scaffold profile criteria

1. `cdc scaffold ... --topology fdw` does not generate broker/runtime assets by default.
2. `cdc scaffold --update` does not recreate those runtime assets for `fdw` implementations.
3. Generated docs for `fdw` describe external runtime ownership correctly.

### Source-group setup criteria

1. `cdc manage-source-groups --set-database-ref <db>` exists.
2. A user can complete a standard `db-per-tenant` setup without manual YAML edits.
3. `cdc manage-services config --service <name> --inspect --all` works after discovery + `database_ref` assignment.

### Env template criteria

1. Every placeholder emitted into source or sink group YAML can be surfaced in root `.env.example`.
2. Adding servers does not silently produce placeholder drift.

## Recommended Delivery Order

1. Add `--set-database-ref`
2. Introduce topology-specific scaffold profiles
3. Make `fdw` scaffold default to config-first assets only
4. Fix `.env.example` synchronization
5. Rewrite generated `fdw` docs
6. Make scaffold update respect asset profile boundaries

## Risks

### Risk 1: preserving old behavior accidentally

If scaffold update still contains unconditional file creation, `fdw` repos will keep regressing back into runtime-heavy shape.

Mitigation:

- move file creation behind explicit bundle decisions
- add tests for negative assertions, not only positive file existence

### Risk 2: partial CLI support for `database_ref`

Adding read/display support without mutation support would not solve the actual setup problem.

Mitigation:

- ship mutation, validation, info display, and completion support together

### Risk 3: env drift remains hidden

Even after fixing source-group setup, users can still get invalid fresh setups if the env template lags behind emitted placeholders.

Mitigation:

- treat env synchronization as part of scaffold correctness, not as a documentation issue

## Test Plan

### Unit and CLI tests

Add tests that verify:

1. `fdw` scaffold does not create `docker-compose.yml` by default.
2. `fdw` scaffold update does not recreate runtime bundle files.
3. `redpanda` scaffold still creates the brokered runtime bundle.
4. `--set-database-ref` writes the expected root field.
5. `create-service` succeeds after `database_ref` is set and sources are discovered.
6. `.env.example` contains placeholders for all generated source/sink server placeholders.

## Non-Goals For The First Pass

The first implementation pass does not need to:

- redesign sink-group semantics
- redesign source discovery rules
- implement orchestrator execution logic
- remove every historical runtime-oriented document from the repo

It only needs to make `fdw` scaffold correct and make `db-per-tenant` setup CLI-complete.
