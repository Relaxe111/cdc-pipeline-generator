# Adaptive Native CDC Polling Gap Analysis And Implementation Plan

## Goal

Turn the current native CDC runtime from fixed-interval central polling into adaptive, centrally scheduled polling that is safer at scale for MSSQL CDC over `tds_fdw`.

This document covers:

1. what is already implemented today
2. what is still missing
3. the recommended adaptive polling algorithm
4. the exact generator/runtime files that need to change
5. the rollout sequence that keeps the system restart-safe

For the dedicated long-term runtime architecture based on an ASMA Bun.js worker, see:

- `ASMA_BUN_CDCRUNNER_ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md`

## Scope

In scope:

- MSSQL CDC pull into PostgreSQL via `tds_fdw`
- scheduling metadata in `cdc_management.source_table_registration`
- database-side claim, lease, success, and failure primitives
- generator templates for the native runtime
- a stateless runner command plus a stateless runner service for sub-minute polling
- monitoring views for due work, lag, and repeated failures

Out of scope:

- remote MSSQL schema changes
- source-side push listeners or Service Broker design
- logical replication fan-out details beyond their interaction with the pull loop
- replacing checkpoint semantics

## Recommendation

Adaptive polling is the recommended default for this project.

Reason:

- `tds_fdw` and SQL Server CDC are both fundamentally pull-oriented for PostgreSQL
- adaptive polling preserves the simplest failure model
- checkpoint advancement already fits a pull-and-merge transaction boundary
- it avoids source-side changes and avoids introducing another bridge service unless that is explicitly needed later
- if `1s` to `30s` polling is needed, the clean production shape is a stateless worker deployment, not a Kubernetes CronJob

## Validated Assumptions

Validated during the local PG17 `fdw-test-setup` work on `2026-03-28`:

- local PostgreSQL 17.9 + `tds_fdw` 2.0.5 works against the nonprod MSSQL source
- helper FDW queries work
- a minimal `Actor_CT` foreign table works
- local staging and merge from FDW rows works
- both the admin source login and the runtime source login worked in the local test environment

Also validated separately:

- nonprod PostgreSQL `directory_dev` still cannot reach the nonprod MSSQL host because of a network timeout

That network issue is real, but it is not an adaptive polling design gap.

## Current State In The Generator

### What Already Exists

The generated native runtime already has the core primitives for fixed-interval central scheduling.

Implemented today:

- `cdc_management.source_table_registration` already exists as the stable registration and mapping anchor
- `cdc_management.native_cdc_schedule_policy` already stores:
  - `schedule_profile`
  - `base_poll_interval_seconds`
  - `min_poll_interval_seconds`
  - `max_poll_interval_seconds`
  - `max_rows_per_pull`
  - `lease_seconds`
  - `poll_priority`
  - `jitter_millis`
  - `max_backoff_seconds`
  - `business_hours_profile_key`
- `cdc_management.native_cdc_runtime_state` already stores:
  - `current_poll_interval_seconds`
  - `empty_pull_streak`
  - `next_pull_at`
  - `lease_owner`
  - `lease_expires_at`
  - `last_pull_started_at`
  - `last_pull_at`
  - `last_success_at`
  - `last_nonempty_at`
  - `last_batch_rows`
  - `last_duration_ms`
  - `consecutive_failures`
  - `last_error`
- `cdc_management.native_cdc_checkpoint` already stores the last committed LSN state per customer plus table
- `cdc_management.claim_due_native_cdc_work(...)` already claims due rows using `FOR UPDATE SKIP LOCKED` against runtime rows only
- `cdc_management.renew_native_cdc_lease(...)` already renews leases without mutating policy rows
- `cdc_management.mark_native_cdc_success(...)` already resets the schedule after a successful pull with a fixed policy-driven millisecond jitter offset
- `cdc_management.mark_native_cdc_failure(...)` already records errors and reschedules retry work with a fixed policy-driven millisecond jitter offset
- `cdc_management.v_native_cdc_health` already exposes joined registration, policy, runtime, source, customer, and checkpoint state
- generator metadata can already seed per-table policy through `source.tables.<schema.table>.native_cdc`
- per-table pull functions already read CDC rows by checkpoint and return a batch id plus row count
- per-table merge procedures already update the checkpoint only after apply succeeds

Current implementation locations:

- `cdc_generator/templates/migrations/native-cdc-runtime.sql.j2`
- `cdc_generator/templates/migrations/native-staging.sql.j2`
- `cdc_generator/helpers/fdw_bootstrap.py`

### What That Means Practically

The current generator is already capable of:

- centrally scheduled pull orchestration
- concurrent-safe due-work claiming
- separate policy and runtime state ownership
- fixed polling tiers such as `30s` and `60s`
- fixed per-registration millisecond jitter
- row-specific `max_rows_per_pull` and `lease_seconds` policy
- health visibility through a generated view
- safe checkpoint advancement after merge commit

So this is not a greenfield feature.

The missing work is to make the scheduler adaptive, observable, and operationally easy to run.

## Gap Analysis

## P0 Gaps

### 1. Effective Interval Exists But Is Not Yet Adaptive

Current state:

- `base_poll_interval_seconds` lives in policy state
- `current_poll_interval_seconds` lives in runtime state and is seeded from the base interval

Gap:

- `current_poll_interval_seconds` is not yet widened or narrowed automatically based on empty pulls, full batches, or business-hours policy

### 2. No Idle Backoff Or Burst Acceleration

Current state:

- after success, `next_pull_at` is still based on a fixed `current_poll_interval_seconds` plus an optional fixed `jitter_millis` offset

Gap:

- repeated empty pulls do not widen the interval
- full or near-full batches do not shrink the interval

### 3. No Adaptive Jitter Policy

Current state:

- schedules can include a fixed per-registration `jitter_millis` offset owned by policy rows

Gap:

- jitter is not yet derived from schedule profile or effective interval
- jitter values are still static unless policy rows are reconfigured

### 4. No Business-Hours Interval Enforcement Yet

Current state:

- policy rows can already store `schedule_profile`, `business_hours_profile_key`, interval bounds, `max_rows_per_pull`, and `lease_seconds`

Gap:

- the generated success and failure procedures do not yet change `current_poll_interval_seconds` based on business-hours windows or outside-hours ceilings

### 5. No Runner Runtime For Automated Lease Renewal

Current state:

- claiming uses a lease expiration time
- `renew_native_cdc_lease(...)` exists as a dedicated lease-renewal procedure

Gap:

- there is still no production runner loop that calls lease renewal automatically during long-running pull or merge work

### 6. No Production Runner Runtime For Sub-Minute Polling

Current state:

- SQL primitives exist
- the runtime still expects an external scheduler or manual invocation pattern

Gap:

- there is no generator-owned stateless runner command or runner service that claims due work, dispatches pull and merge helpers, and records adaptive success or failure outcomes
- Kubernetes CronJobs are fine for minute-plus schedules, but they are not the right execution model for `1s` to `30s` adaptive polling

### 7. No Health View For Operations

Current state:

- `cdc_management.v_native_cdc_health` exists and exposes joined state for operators

Gap:

- no runner yet emits higher-level operational summaries, metrics, or alerts on top of the generated view

## P1 Gaps

### 8. No Policy Profiles In The Generator Model

Current state:

- manual SQL can set hot or warm tiers

Gap:

- there is no generator-modeled schedule profile such as `hot`, `warm`, or `cold`
- there is no CLI support to stamp or update those policies cleanly

### 9. No Failure Classification

Current state:

- all failures are treated similarly

Gap:

- network timeout, authentication error, retention-gap error, and transient query issues should not use the same retry schedule or the same alerting severity

### 10. No Test Coverage For Adaptive State Transitions

Current state:

- fixed scheduling primitives exist

Gap:

- no tests assert interval shrink, interval backoff, lease safety, or jitter behavior

## Recommended Adaptive Polling Model

## Principle

Keep `poll_interval_seconds` as the steady-state base interval.

Add `current_poll_interval_seconds` as the effective interval that changes at runtime.

Use simple deterministic rules:

- empty pulls widen the effective interval
- full batches shrink the effective interval
- normal non-empty batches reset the effective interval toward the base interval
- failures use exponential backoff with a cap and jitter

This gives adaptive behavior without a hard-to-debug control loop.

## Long-Term Runtime Architecture

The preferred long-term production runtime is not a CronJob-based scheduler.

It is:

- a stateless active/active Bun worker deployment
- PostgreSQL-owned leases and checkpoint state
- Option B data separation between cold schedule policy and hot runtime state

Dedicated design document:

- `ASMA_BUN_CDCRUNNER_ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md`

## Long-Term Data Model Preference: Option B

Short-term compatibility can evolve from the current `source_table_registration` design.

Long-term preferred design is Option B:

- keep stable registration identity separate
- move rare-changing scheduler policy into its own table
- move hot mutable runtime state into its own table

Why Option B is preferred:

- clearer cache boundaries
- less contention on hot rows
- easier future UI and CLI policy management
- less risk of policy edits interfering with runtime ownership state

This is now the preferred implementation target, not only a long-term aspiration.

## Preferred Option B Ownership

Use this split when implementing the native runtime.

Registration owns:

- stable source and target mapping identity
- deterministic table and routine identity

Policy owns:

- effective scheduler enablement
- base and bounded intervals
- business-hours profiles
- limits such as `max_rows_per_pull`
- lease defaults, priority, jitter, and backoff caps

Runtime owns:

- `next_pull_at`
- current effective interval
- lease ownership and expiry
- failure streaks
- last-run timestamps and metrics
- checkpoint linkage metadata

Checkpoint state still owns:

- committed LSN position
- committed seqval position

This is the cleanest split for active/active workers because policy becomes naturally cacheable while runtime remains naturally non-cacheable.

## Compatibility Bridge Only If Option B Must Be Deferred

If an incremental migration is needed before Option B is introduced, these columns can temporarily live on `cdc_management.source_table_registration`:

```sql
ALTER TABLE "cdc_management"."source_table_registration"
ADD COLUMN IF NOT EXISTS "schedule_profile" text,
ADD COLUMN IF NOT EXISTS "min_poll_interval_seconds" integer,
ADD COLUMN IF NOT EXISTS "max_poll_interval_seconds" integer,
ADD COLUMN IF NOT EXISTS "current_poll_interval_seconds" integer,
ADD COLUMN IF NOT EXISTS "empty_pull_streak" integer,
ADD COLUMN IF NOT EXISTS "max_rows_per_pull" integer,
ADD COLUMN IF NOT EXISTS "lease_seconds" integer,
ADD COLUMN IF NOT EXISTS "jitter_millis" integer,
ADD COLUMN IF NOT EXISTS "max_backoff_seconds" integer,
ADD COLUMN IF NOT EXISTS "last_success_at" timestamptz,
ADD COLUMN IF NOT EXISTS "last_nonempty_at" timestamptz;
```

Seed defaults:

```sql
UPDATE "cdc_management"."source_table_registration"
SET
    "schedule_profile" = COALESCE("schedule_profile", 'warm'),
    "poll_interval_seconds" = COALESCE("poll_interval_seconds", 60),
    "min_poll_interval_seconds" = COALESCE("min_poll_interval_seconds", GREATEST(5, COALESCE("poll_interval_seconds", 60) / 4)),
    "max_poll_interval_seconds" = COALESCE("max_poll_interval_seconds", GREATEST(COALESCE("poll_interval_seconds", 60) * 5, 300)),
    "current_poll_interval_seconds" = COALESCE("current_poll_interval_seconds", COALESCE("poll_interval_seconds", 60)),
    "empty_pull_streak" = COALESCE("empty_pull_streak", 0),
    "max_rows_per_pull" = COALESCE("max_rows_per_pull", 1000),
    "lease_seconds" = COALESCE("lease_seconds", 120),
    "jitter_millis" = COALESCE("jitter_millis", 500),
    "max_backoff_seconds" = COALESCE("max_backoff_seconds", 900)
WHERE true;
```

## Recommended Interval Ladder

The concrete ladder discussed here is reasonable if it is treated as a bounded state machine, not as one universal static interval for every table.

Business-hours window:

- working hours: Monday to Friday, `07:00-20:00`, local time
- outside working hours: `20:00-07:00`, local time, plus all day Saturday and Sunday

Allowed effective intervals:

- `1s`
- `5s`
- `30s`
- `1m`
- `5m`
- `10m`
- `30m`

Important caveat:

- `1s` only makes sense for a small number of truly hot tables
- do not make `1s` the default for broad table sets or all customers
- for subsecond-aware scheduling, use `jitter_millis` instead of whole-second jitter
- if `1s` remains a real requirement, the Bun runner heartbeat must be faster than `1s`, for example `100ms` to `250ms`

## Recommended Profile Defaults

### Hot

- business-hours target interval: `1s`
- minimum interval: `1s`
- idle demotion after `2m` with no changes: `1s -> 30s`
- outside-hours ceiling: `5m`
- recommended `jitter_millis`: `0`
- max rows per pull: `2000`
- lease seconds: `120`

### Warm

- business-hours target interval: `5s`
- minimum interval: `5s`
- business-hours idle ceiling: `1m`
- outside-hours ceiling: `10m`
- recommended `jitter_millis`: `250`
- max rows per pull: `1000`
- lease seconds: `120`

### Cool

- business-hours target interval: `30s`
- minimum interval: `30s`
- idle demotion after `30m` with no changes: `30s -> 5m`
- outside-hours ceiling: `10m`
- recommended `jitter_millis`: `1000`
- max rows per pull: `750`
- lease seconds: `120`

### Cold

- business-hours target interval: `1m`
- minimum interval: `1m`
- outside-hours ceiling: `30m`
- recommended `jitter_millis`: `5000`
- max rows per pull: `500`
- lease seconds: `180`

This shape uses the interval set you asked for and it does make sense operationally:

- `1s` is reserved for the hottest tables only
- `5s` is a good default for warm operational tables
- `30s` and `1m` are reasonable business-hours targets for cooler tables
- `5m`, `10m`, and `30m` are reasonable outside-hours ceilings
- the business-hours policy stays in cacheable policy state instead of being hardcoded in the runner

## Success Rules

Use a bounded step-ladder model after a successful pull plus merge.

Do not use unrestricted doubling and halving when the business wants specific operational states like `1s`, `5s`, `30s`, `1m`, `5m`, `10m`, and `30m`.

### Case 1: `p_rows = 0`

- increment `empty_pull_streak`
- keep `last_nonempty_at` unchanged
- if the current interval is `1s` and there have been no changes for `2m`, widen the effective interval to `30s`
- if the current interval is `30s` or faster and there have been no changes for `30m`, widen the effective interval to `5m`
- during outside working hours, clamp the effective interval to the profile's outside-hours ceiling

### Case 2: `0 < p_rows < max_rows_per_pull`

- reset `empty_pull_streak` to `0`
- set `last_nonempty_at = now()`
- if the current interval is slower than the profile's business-hours target and the current time is inside working hours, move one ladder step faster toward the target
- otherwise keep the current effective interval stable

### Case 3: `p_rows >= max_rows_per_pull`

- reset `empty_pull_streak` to `0`
- set `last_nonempty_at = now()`
- move one ladder step faster toward the profile's business-hours target
- never go faster than the profile minimum interval

Recommended ladder order, slowest to fastest:

- `30m`
- `10m`
- `5m`
- `1m`
- `30s`
- `5s`
- `1s`

The two concrete demotion rules you requested are reasonable:

- after `2m` with no changes, `1s -> 30s`
- after `30m` with no changes, `30s -> 5m`

Those are better modeled as explicit idle demotion thresholds than as generic exponential backoff.

After that, compute:

```text
jitter_offset_millis = random(0..jitter_millis)
next_pull_at = now() + current_poll_interval_seconds + jitter_offset_millis / 1000.0
```

Practical note:

- for the `1s` tier, use `jitter_millis = 0`
- for the `5s` tier, keep `jitter_millis` small, for example `0-250`
- PostgreSQL can store subsecond `timestamptz` values, but the Bun runner must poll often enough to honor them

## Failure Rules

Use this logic after a failed pull or merge:

```text
backoff = min(
    max_backoff_seconds,
    max(current_poll_interval_seconds, poll_interval_seconds) * 2 ^ min(consecutive_failures, 4)
)
jitter_offset_millis = random(0..jitter_millis)
next_pull_at = now() + backoff + jitter_offset_millis / 1000.0
```

Do not advance checkpoints.

Keep the last error text.

Release the lease.

## Lease Rules

- claim leases should be per registration, not one global default for all tables
- the runner should renew the lease before half the remaining lease time is gone if it is still processing
- expired leases must remain reclaimable by another worker

## Concrete Generator Changes

## 1. Update `native-cdc-runtime.sql.j2`

Add:

- `native_cdc_schedule_policy`
- `native_cdc_runtime_state`
- backfill SQL from current registration data into the split tables
- `v_native_cdc_health` view
- `renew_native_cdc_lease(...)` procedure
- updated `claim_due_native_cdc_work(...)` that joins policy state but locks only runtime rows
- adaptive success and failure procedures that update runtime state instead of registration state
- compatibility handling so `source_table_registration` becomes identity-only over time

Current file to change:

- `cdc_generator/templates/migrations/native-cdc-runtime.sql.j2`

## 2. Update `native-staging.sql.j2`

Keep checkpoint semantics unchanged.

Add or verify:

- pull helpers accept `p_max_rows`
- runner can pass `max_rows_per_pull` from registration metadata
- merge procedures keep checkpoint advancement atomic with data apply

Current file to change:

- `cdc_generator/templates/migrations/native-staging.sql.j2`

## 3. Add A Health View

Recommended view fields:

- `source_instance_key`
- `logical_table_name`
- `schedule_profile`
- `poll_interval_seconds`
- `current_poll_interval_seconds`
- `next_pull_at`
- `overdue_seconds`
- `consecutive_failures`
- `last_error`
- `last_pull_at`
- `last_success_at`
- `last_nonempty_at`
- `lease_owner`
- `lease_expires_at`
- `last_batch_rows`
- `last_duration_ms`

This should live in `cdc_management` and join registration, policy, runtime, source instance, customer registry, and checkpoint state.

## 4. Add A Stateless Runner Runtime

Long-term preferred runtime:

- `asma-bun-cdcrunner`

Use the dedicated Bun runner design document for runtime best practices, replica model, cache boundaries, and Kubernetes deployment shape.

Recommended repository placement:

- `asma-modules/cdc/asma-bun-cdcrunner`

This keeps the runtime next to the existing CDC implementations, including:

- `asma-modules/cdc/adopus-cdc-pipeline`
- `asma-modules/cdc/asma-cdc-pipelines`

Recommended first interfaces:

```text
cdc native-cdc run-due
```

That command is useful for:

- local development
- one-shot execution
- smoke testing generated SQL primitives
- fallback minute-plus scheduling if sub-minute execution is not required

Recommended production shape when sub-minute polling is required:

- a stateless Bun worker deployment
- one worker process per pod
- `2` to `3` active replicas
- optional Kubernetes `Service` only if metrics, health endpoints, or an admin API need to be exposed

If this is implemented in the ASMA ecosystem as a dedicated repository, follow the canonical naming convention already documented there:

- `asma-bun-cdcrunner`

Do not use legacy `asma-srv-*` naming for a new Bun service.

Recommended behavior:

1. claim due work with a configurable limit
2. for each claimed registration, read policy from cache or PostgreSQL if stale
3. call the generated per-table pull helper using the row-specific `max_rows_per_pull`
4. if a batch was staged, call the merge procedure
5. mark success or failure using the adaptive procedures
6. renew the lease if a pull or merge runs longer than the initial lease budget
7. emit structured logs and return a summary

Recommended flags:

- `--dsn`
- `--limit`
- `--worker-id`
- `--default-lease-seconds`
- `--stop-after-seconds`

Important design choice:

- keep this command stateless
- keep PostgreSQL as the source of truth for due work, leases, checkpoint state, and adaptive runtime state
- if sub-minute execution is required, prefer a stateless Bun deployment over CronJob scheduling
- Kubernetes CronJob remains acceptable only for minute-plus fallback schedules or one-shot wrappers
- avoid building a permanently running Python daemon into the generator package as the first production shape
- do not introduce active/passive leader election unless a later requirement proves active/active workers are insufficient

### Recommended Active/Active Replica Model

Use multiple active replicas, not one active pod with sleeping standbys.

Recommended behavior:

- every pod gets a unique `worker_id`, derived from hostname or pod name
- every pod polls on a short heartbeat such as `1s`
- every pod asks PostgreSQL for due work
- `FOR UPDATE SKIP LOCKED` plus lease expiry ensures one registration is owned by one worker at a time
- if one pod dies, another pod picks up the work after lease expiry

Why this is better than active/passive:

- fewer failover moving parts
- no separate leader-election subsystem required
- better horizontal throughput when many hot tables are active
- the database already has the correct coordination boundary

### Recommended Cache Strategy

Long-term preferred implementation is Option B table separation.

Until that split exists, follow these boundaries:

Use cache only for rare-changing policy metadata. Do not make cache authoritative for runtime state.

Never cache as authoritative:

- `next_pull_at`
- `current_poll_interval_seconds`
- `lease_owner`
- `lease_expires_at`
- `consecutive_failures`
- checkpoint LSN state
- in-flight work ownership

Cache with short TTL or version checks:

- `schedule_profile`
- `poll_interval_seconds`
- `min_poll_interval_seconds`
- `max_poll_interval_seconds`
- `max_rows_per_pull`
- `lease_seconds`
- `poll_priority`
- business-hour policy
- enabled or disabled flags

Cache long or derive once:

- generated procedure names
- schema and table mapping metadata
- stable registration identity fields

Recommended first implementation:

- keep one cheap due-work claim query against PostgreSQL on every heartbeat
- add `updated_at` or `config_version` to policy-bearing rows
- refresh rare-changing policy cache every `30` to `60` seconds
- optionally add `LISTEN/NOTIFY` invalidation later if config-churn becomes operationally important

The optimization target is remote MSSQL roundtrips, not the local PostgreSQL claim query.

## 5. Add Policy Seeding Support

Implemented in the generator today:

- service YAML can seed policy rows through `source.tables.<schema.table>.native_cdc`
- the generated runtime SQL syncs those defaults into `native_cdc_schedule_policy` and `native_cdc_runtime_state`

Still missing:

- CLI command to apply or rebalance profiles

The CLI surface is still P1.

## Gaps In `fdw_bootstrap.py`

Current state:

- `fdw_bootstrap.py` inserts or updates `source_table_registration`
- it only seeds the core registration columns

Gap:

- bootstrap still does not write policy rows directly; it relies on the generated runtime sync helper and trigger to seed policy and runtime rows from registration identity

Recommended action:

- leave bootstrap focused on identity and FDW mapping in the first iteration
- let the native runtime migration template own policy and runtime sync
- add policy-aware bootstrap only after the CLI and config model are agreed

## Concrete Health Queries To Support

### Overdue Work

```sql
SELECT *
FROM "cdc_management"."v_native_cdc_health"
WHERE "overdue_seconds" > 0
ORDER BY "overdue_seconds" DESC;
```

### Repeated Failures

```sql
SELECT *
FROM "cdc_management"."v_native_cdc_health"
WHERE "consecutive_failures" >= 3
ORDER BY "consecutive_failures" DESC, "next_pull_at" ASC;
```

### Registrations Still Running Hot

```sql
SELECT *
FROM "cdc_management"."v_native_cdc_health"
WHERE "current_poll_interval_seconds" <= "min_poll_interval_seconds" * 2
ORDER BY "poll_priority", "source_instance_key", "logical_table_name";
```

## What We Do Not Need To Add

Do not add for the first adaptive polling iteration:

- source-side MSSQL triggers
- Service Broker
- another external message broker
- Kubernetes CronJob-per-table designs for sub-minute polling

Those options add complexity without solving the core problem more safely than adaptive pull scheduling.

Note:

- a long-running stateless worker deployment is acceptable and recommended if `1s` to `30s` polling is required

## External Gaps That Are Real But Separate

These are not adaptive polling gaps, but they matter operationally:

- nonprod PostgreSQL still needs network access to nonprod MSSQL
- downstream logical replication still needs `wal_level = logical` on nonprod before that part can be tested there

## Rollout Plan

## Phase 1: Document And Normalize Current Primitives

- publish this document
- align the FDW guide with the current runtime state
- treat `poll_interval_seconds` as the steady-state base interval

Acceptance:

- docs reflect the current implementation accurately

## Phase 2: Add Adaptive Metadata And Views

Status:

- implemented in generated SQL

- create `native_cdc_schedule_policy` and `native_cdc_runtime_state`
- backfill them from current scheduler metadata
- keep stable registration identity separate from hot runtime state
- add the health view
- update the lease model to be per registration

Acceptance:

- generated runtime still works with existing fixed tiers
- health view shows all tracked registrations
- policy and runtime responsibilities are clearly separated in the target design

## Phase 3: Switch Scheduler Procedures To The Split

Status:

- implemented in generated SQL

- update claim, renew, success, and failure procedures to operate on runtime rows
- join policy rows for enablement, priority, limits, and lease defaults
- stop using `source_table_registration` as the hot state row during normal scheduling

Acceptance:

- scheduler primitives lock only runtime rows
- policy is read-only during normal claiming

## Phase 4: Add Adaptive Success And Failure Logic

- implement effective interval tracking
- implement empty-pull backoff
- implement full-batch acceleration
- implement exponential failure backoff with jitter

Acceptance:

- repeated empty polls widen the interval
- full batches shrink the interval
- failures back off without losing checkpoint safety

## Phase 5: Add The Runner Runtime

- implement the one-shot runner command
- implement the long-running stateless `asma-bun-cdcrunner` deployment shape for sub-minute polling
- make the runtime claim due work and call generated helpers
- add structured logging, worker ids, and lease renewal
- keep policy metadata cacheable but keep due-work state authoritative in PostgreSQL

Acceptance:

- one command can run a full due-work cycle safely
- `2+` replicas can run active/active without duplicate work because of leases and `SKIP LOCKED`
- sub-minute polling is possible without Kubernetes CronJobs

## Phase 6: Add Profile Configuration And Cleanup

- add profile-driven policy seeding
- add CLI helpers or config modeling for hot, warm, and cold tables
- remove or tombstone legacy hot columns from `source_table_registration` after the split is proven stable

Acceptance:

- tables can be assigned to profiles without manual SQL edits
- registration is reduced to identity and mapping responsibility

## Tests To Add

## SQL Template Tests

- generated runtime contains new adaptive columns
- generated runtime contains health view and lease renewal procedure

## Scheduler Logic Tests

- claim ordering honors priority then due time
- empty pull streak widens intervals
- full batches shrink intervals
- failure backoff is capped
- jitter keeps `next_pull_at` within the expected bounds

## End-To-End Tests

- checkpoint is updated only after merge commit
- expired leases can be reclaimed
- two workers do not process the same registration concurrently

## Assessment

Adaptive polling should be the default runtime direction for native CDC in this project.

The current generator already contains enough of the scheduler foundation that this is an incremental evolution, not a redesign.

The right next implementation step is not another listener technology. It is finishing the adaptive scheduling layer around the generator’s existing native runtime primitives.

For production execution, the preferred runtime shape is a stateless worker deployment with active/active replicas and PostgreSQL-owned leases.

If the runtime is implemented inside the ASMA platform, use the canonical ASMA naming convention for a new Bun service, for example:

- `asma-bun-cdcrunner`

Recommended location:

- `asma-modules/cdc/asma-bun-cdcrunner`

For long-term maintainability, use Option B data separation rather than keeping all scheduler policy and runtime state on one registration row.
