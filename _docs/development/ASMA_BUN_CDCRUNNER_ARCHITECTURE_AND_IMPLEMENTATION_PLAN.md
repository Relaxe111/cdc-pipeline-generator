# ASMA Bun CDC Runner Architecture And Implementation Plan

## Purpose

Define the long-term production runtime for adaptive native CDC polling when sub-minute polling is required.

Canonical ASMA naming for new work:

- `asma-bun-cdcrunner`

This document captures the best practices agreed during design review and translates them into a concrete long-term implementation target.

## Recommendation

Use a stateless Bun worker deployment in Kubernetes.

Recommended shape:

- one repository: `asma-bun-cdcrunner`
- one Kubernetes `Deployment`
- `2` to `3` active replicas
- one worker process per pod
- optional Kubernetes `Service` only for metrics, health, or admin endpoints

Recommended placement:

- `asma-modules/cdc/asma-bun-cdcrunner`

This keeps the runner in the same CDC workspace area as the current CDC repositories:

- `asma-modules/cdc/adopus-cdc-pipeline`
- `asma-modules/cdc/asma-cdc-pipelines`

Do not use Kubernetes CronJobs for sub-minute polling.

CronJobs remain acceptable only for minute-plus schedules or one-shot wrappers.

## Why This Runtime Exists

The PostgreSQL-native CDC design already has the correct database-side foundation:

- due-work claiming
- leases
- checkpointed pull functions
- merge procedures
- success and failure bookkeeping

What is missing for production sub-minute polling is a safe orchestrator.

The Bun runner should be that orchestrator.

It should not own correctness-critical state.

## Core Principles

### 1. PostgreSQL Is The Source Of Truth

Authoritative state must stay in PostgreSQL:

- due work
- lease ownership
- lease expiry
- checkpoint LSN state
- current effective interval
- failure streaks
- next scheduled pull time

The Bun runner may cache policy metadata, but it must not become the source of truth for runtime state.

### 2. Keep The Worker Stateless

Each pod should be replaceable without data loss or manual recovery.

That means:

- no persistent local scheduler state
- no in-memory checkpoint truth
- no cache-only ownership model
- no leader-only responsibility for correctness

### 3. Keep Heavy Data Work In PostgreSQL

The Bun runner should coordinate work, not reimplement merge logic.

Use Bun for:

- claiming work
- invoking generated pull functions
- invoking generated merge procedures
- renewing leases
- recording success or failure
- logging and metrics

Keep in PostgreSQL:

- FDW reads
- staging inserts
- merge and delete logic
- checkpoint advancement

### 4. Prefer Active/Active Over Active/Passive

Run several active replicas.

Do not start with one active pod and sleeping backups.

The database already has the right coordination mechanism via leases and `FOR UPDATE SKIP LOCKED`.

## Long-Term Data Model: Option B

This is the preferred long-term architecture.

Split cold policy state from hot runtime state.

### Table 1: Stable Registration Identity

Keep `cdc_management.source_table_registration` focused on registration identity and enablement.

Recommended responsibilities:

- `source_instance_key`
- `logical_table_name`
- remote and target mapping identity
- stable registration identity

This table should not remain the dumping ground for all hot runtime scheduling fields.

Long-term target:

- effective scheduler enablement belongs in policy state, not runtime state
- the existing registration `enabled` column can remain temporarily for compatibility during migration, but it should stop being the primary scheduler switch

### Table 2: Schedule Policy

Add a dedicated policy table, for example:

```sql
"cdc_management"."native_cdc_schedule_policy"
```

Recommended fields:

- `source_instance_key`
- `logical_table_name`
- `schedule_profile`
- `enabled`
- `base_poll_interval_seconds`
- `min_poll_interval_seconds`
- `max_poll_interval_seconds`
- `max_rows_per_pull`
- `lease_seconds`
- `poll_priority`
- `jitter_millis`
- `max_backoff_seconds`
- `business_hours_profile_key`
- `config_version`
- `updated_at`

This table changes rarely.

This is the right cache boundary.

### Table 3: Runtime State

Add a dedicated runtime table, for example:

```sql
"cdc_management"."native_cdc_runtime_state"
```

Recommended fields:

- `source_instance_key`
- `logical_table_name`
- `checkpoint_table_name`
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
- `updated_at`

This table is hot and must remain database-authoritative.

Checkpoint linkage belongs here as runtime metadata, but checkpoint LSN values do not.

The actual checkpoint values must remain in `cdc_management.native_cdc_checkpoint`.

### Why Option B Is Better Long-Term

- separates cold config from hot state
- cleaner cache boundaries
- lower chance of overwriting runtime state during config edits
- easier future UI and CLI management
- easier auditing of policy changes versus runtime behavior
- better scaling characteristics when many workers update runtime state frequently

### Transitional Note

The current generator can evolve incrementally from the all-in-one registration table.

Short-term compatibility is acceptable.

Long-term target should still be Option B.

## Preferred Ownership By Table

Use this ownership split as the implementation target.

### Registration Owns

- `source_instance_key`
- `logical_table_name`
- `remote_schema_name`
- `remote_table_name`
- `target_schema_name`
- `target_table_name`
- deterministic naming identity for generated pull and merge routines

Registration is identity and mapping only.

### Policy Owns

- effective scheduler enablement
- `schedule_profile`
- base, min, and max intervals
- business-hours behavior
- max rows per pull
- lease seconds
- poll priority
- millisecond jitter and failure backoff ceilings
- `config_version`

Policy is the natural cache boundary.

### Runtime State Owns

- `checkpoint_table_name`
- `current_poll_interval_seconds`
- `empty_pull_streak`
- `next_pull_at`
- lease ownership and expiry
- success and failure streaks
- last run timestamps
- last batch and duration metrics
- last error text

Runtime state is hot and must stay non-cacheable and database-authoritative.

### Checkpoint Table Owns

- `last_start_lsn`
- `last_seqval`
- checkpoint commit time

Do not duplicate checkpoint LSN values into runtime state.

## Concrete Option B SQL Shape

This is the preferred PostgreSQL shape for the split.

```sql
CREATE TABLE IF NOT EXISTS "cdc_management"."native_cdc_schedule_policy" (
    "source_instance_key" text NOT NULL,
    "logical_table_name" text NOT NULL,
    "enabled" boolean NOT NULL DEFAULT true,
    "schedule_profile" text NOT NULL DEFAULT 'warm',
    "base_poll_interval_seconds" integer NOT NULL,
    "min_poll_interval_seconds" integer NOT NULL,
    "max_poll_interval_seconds" integer NOT NULL,
    "max_rows_per_pull" integer NOT NULL,
    "lease_seconds" integer NOT NULL,
    "poll_priority" integer NOT NULL DEFAULT 100,
    "jitter_millis" integer NOT NULL DEFAULT 500,
    "max_backoff_seconds" integer NOT NULL DEFAULT 900,
    "business_hours_profile_key" text,
    "config_version" bigint NOT NULL DEFAULT 1,
    "updated_at" timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("source_instance_key", "logical_table_name"),
    FOREIGN KEY ("source_instance_key", "logical_table_name")
        REFERENCES "cdc_management"."source_table_registration" (
            "source_instance_key",
            "logical_table_name"
        )
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "cdc_management"."native_cdc_runtime_state" (
    "source_instance_key" text NOT NULL,
    "logical_table_name" text NOT NULL,
    "checkpoint_table_name" text NOT NULL,
    "current_poll_interval_seconds" integer NOT NULL,
    "empty_pull_streak" integer NOT NULL DEFAULT 0,
    "next_pull_at" timestamptz NOT NULL DEFAULT now(),
    "lease_owner" text,
    "lease_expires_at" timestamptz,
    "last_pull_started_at" timestamptz,
    "last_pull_at" timestamptz,
    "last_success_at" timestamptz,
    "last_nonempty_at" timestamptz,
    "last_batch_rows" bigint,
    "last_duration_ms" bigint,
    "consecutive_failures" integer NOT NULL DEFAULT 0,
    "last_error" text,
    "updated_at" timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("source_instance_key", "logical_table_name"),
    FOREIGN KEY ("source_instance_key", "logical_table_name")
        REFERENCES "cdc_management"."source_table_registration" (
            "source_instance_key",
            "logical_table_name"
        )
        ON DELETE CASCADE
);
```

Design rule:

- `source_table_registration` remains the durable identity anchor
- `native_cdc_schedule_policy` holds rare-changing scheduler policy and enablement
- `native_cdc_runtime_state` holds all hot mutable execution state
- `native_cdc_checkpoint` continues to own committed LSN progress

## Claiming And Leasing With Split Tables

The critical behavior with Option B is that workers lock and update only runtime rows.

Policy rows are joined for read-only decision-making.

Recommended query shape:

```sql
WITH due AS (
        SELECT runtime.ctid
        FROM "cdc_management"."native_cdc_runtime_state" runtime
        JOIN "cdc_management"."native_cdc_schedule_policy" policy
            ON policy."source_instance_key" = runtime."source_instance_key"
         AND policy."logical_table_name" = runtime."logical_table_name"
        JOIN "cdc_management"."source_table_registration" reg
            ON reg."source_instance_key" = runtime."source_instance_key"
         AND reg."logical_table_name" = runtime."logical_table_name"
        WHERE policy."enabled" = true
            AND COALESCE(runtime."next_pull_at", now()) <= now()
            AND (
                        runtime."lease_expires_at" IS NULL
                        OR runtime."lease_expires_at" <= now()
            )
        ORDER BY
                policy."poll_priority" ASC,
                runtime."next_pull_at" ASC,
                runtime."source_instance_key" ASC,
                runtime."logical_table_name" ASC
        FOR UPDATE OF runtime SKIP LOCKED
        LIMIT $1
)
UPDATE "cdc_management"."native_cdc_runtime_state" runtime
SET
        "lease_owner" = $2,
        "lease_expires_at" = now() + make_interval(secs => $3),
        "last_pull_started_at" = now(),
        "updated_at" = now()
FROM due
WHERE runtime.ctid = due.ctid;
```

That split matters operationally:

- all replicas can run the same cheap heartbeat query
- policy can be cached without making work ownership ambiguous
- runtime rows stay the only lock target for work claiming
- if one pod dies, another pod recovers the work after lease expiry without leader election

Because `next_pull_at` is `timestamptz`, PostgreSQL can store subsecond scheduled times.

That means `asma-bun-cdcrunner` can be made subsecond-aware in Bun or TypeScript, but only if:

- policy stores `jitter_millis` instead of whole-second jitter
- the runner heartbeat is faster than `1s`, for example `100ms` or `250ms`
- the due-work loop compares against the database clock often enough to observe those timestamps

## Implementation Plan For The Split

This is the preferred migration order.

### Phase 1: Introduce Split Tables Without Changing Behavior

- add `native_cdc_schedule_policy`
- add `native_cdc_runtime_state`
- backfill them from the current `source_table_registration` scheduler columns
- set `checkpoint_table_name = logical_table_name` for the first iteration
- keep existing generated functions working while both models coexist

Acceptance:

- every existing registration has one policy row and one runtime row
- no checkpoint data is moved or rewritten

### Phase 2: Switch The Generated Database Contract

- update `native-cdc-runtime.sql.j2` so claim, renew, success, and failure procedures read or write the split tables
- update health views to join registration, policy, runtime, source instance, customer registry, and checkpoint state
- stop treating `source_table_registration` as the hot runtime state store

Acceptance:

- scheduler primitives lock only runtime rows
- policy rows are read-only during normal claiming

### Phase 3: Align The Runner To The Split

- keep all replicas active
- cache only policy rows, keyed by `config_version`
- read due work and lease state from runtime rows every heartbeat
- keep checkpoint ownership fully in generated SQL procedures

Acceptance:

- the runner never relies on cache for `next_pull_at`, lease ownership, or failure state
- active/active replicas process work safely through database leases alone

### Phase 4: Remove Legacy Hot Columns From Registration

- stop writing runtime scheduling fields to `source_table_registration`
- keep compatibility views only as long as migration safety requires
- remove or tombstone obsolete columns after one stable release window

Acceptance:

- registration is identity-only
- policy and runtime ownership boundaries are clear in both schema and code

## Runner Responsibilities

Each worker instance should:

1. wake up on a short heartbeat, such as `1s` for second-based schedules or `100ms` to `250ms` when millisecond jitter is enabled
2. claim due work from PostgreSQL
3. fetch policy metadata from cache or PostgreSQL if stale
4. call the generated pull function for the claimed table registration
5. call the merge procedure if rows were staged
6. renew the lease if work exceeds the original lease window
7. mark success or failure
8. emit logs and metrics

Each worker instance should not:

- compute correctness from cache alone
- update checkpoints directly outside the generated SQL contract
- poll MSSQL directly outside the FDW and generated SQL path
- make remote source changes on MSSQL

## Replica Model

### Recommended

- `2` to `3` active replicas
- unique `worker_id` per pod
- all replicas participate in claiming due work

### Claiming Model

- call the claim function on every heartbeat
- use `FOR UPDATE SKIP LOCKED`
- write `lease_owner`
- write `lease_expires_at`
- renew lease during long-running pulls or merges

### Failure Recovery

If a pod dies:

- its lease expires
- another replica claims the work
- checkpoint safety is preserved because checkpoint advancement stays tied to the merge transaction

### Why Not Active/Passive First

- introduces avoidable leader-election complexity
- reduces throughput when multiple hot tables are active
- slows failover
- duplicates functionality already provided by DB-side leasing

## Cache Strategy

### Never Cache As Authoritative

- `next_pull_at`
- `current_poll_interval_seconds`
- `lease_owner`
- `lease_expires_at`
- `consecutive_failures`
- checkpoint LSN values
- in-flight ownership

### Cache With Short TTL Or Version Checks

- schedule profile
- base interval
- min or max interval
- batch size
- lease seconds
- priority
- business-hours policy
- enabled or disabled state

### Cache Long Or Derive Once

- generated procedure names
- schema and table mapping metadata
- stable registration identity

### Recommended First Cache Policy

- heartbeat query to PostgreSQL every `250ms` by default
- reduce the heartbeat to `100ms` when any enabled table can run at `1s` or when small `jitter_millis` windows on `1s` tiers must be honored closely
- keep `250ms` when the fastest enabled effective interval is `5s` or slower
- policy cache refresh every `30` to `60` seconds
- invalidate early when `config_version` changes
- add `LISTEN/NOTIFY` later only if needed

The main optimization target is remote MSSQL roundtrips, not the local PostgreSQL claim query.

## Heartbeat Loop And Sleep Strategy

Use one sequential async loop per worker.

Do not use overlapping `setInterval(...)` callbacks for claim and execution work, because slow pull or merge cycles will stack callbacks and make lease timing harder to reason about.

### Cadence Selection

- use `250ms` when the fastest enabled policy interval is `5s` or slower
- use `100ms` when any enabled table can run at `1s` or when the runtime must honor small `jitter_millis` windows on `1s` tiers
- only recalculate the cadence after a policy cache refresh or `config_version` change, not after every claim result
- prefer `250ms` first and enable `100ms` only for verified hot tables

### Sleep Strategy

- track the next heartbeat with a monotonic clock such as `performance.now()` or `Bun.nanoseconds()`
- after each loop, sleep only the remaining time until the next heartbeat with `await Bun.sleep(...)`
- if the claim batch comes back full, allow one immediate drain pass before sleeping so backlog does not wait an extra `100ms` or `250ms`
- if processing overruns a heartbeat boundary, skip missed ticks and schedule the next heartbeat from the current monotonic time instead of replaying every missed beat
- never busy-wait for sub-millisecond precision; the scheduler target here is `100ms` or `250ms`, not microseconds

Example worker loop:

```ts
const heartbeatMs = hasHotOneSecondTier ? 100 : 250;
let nextHeartbeatAtMs = performance.now();

for (;;) {
    const nowMs = performance.now();
    const sleepMs = Math.max(0, nextHeartbeatAtMs - nowMs);

    if (sleepMs > 0) {
        await Bun.sleep(sleepMs);
    }

    const startedAtMs = performance.now();
    nextHeartbeatAtMs = startedAtMs + heartbeatMs;

    const claimed = await claimDueNativeCdcWork(workerId, claimLimit);

    if (claimed.length > 0) {
        await processClaimedWork(claimed);

        if (claimed.length === claimLimit) {
            nextHeartbeatAtMs = performance.now();
            continue;
        }
    }

    const finishedAtMs = performance.now();

    if (finishedAtMs > nextHeartbeatAtMs) {
        nextHeartbeatAtMs = finishedAtMs + heartbeatMs;
    }
}
```

## Kubernetes Shape

### Required

- `Deployment`
- `Secret` or external secret integration for DB credentials
- CPU and memory requests and limits
- liveness and readiness endpoints if HTTP server is present

### Optional

- `Service` for metrics or health endpoints
- `PodDisruptionBudget`
- `HorizontalPodAutoscaler`

### Not Recommended For This Use Case

- one CronJob per tracked table
- one CronJob waking every minute and pretending to be sub-minute

## Suggested ASMA Naming

Use the canonical ASMA repo naming convention:

- `asma-bun-cdcrunner`

Use the same canonical base for:

- repository name
- image name
- ArgoCD application name
- Helm release name

Repository location:

- `asma-modules/cdc/asma-bun-cdcrunner`

Avoid new `asma-srv-*` names for this service.

## Database Contract For The Runner

The runner should call a small, stable set of database-side entry points.

Recommended contract:

- `claim_due_native_cdc_work(...)`
- `renew_native_cdc_lease(...)`
- `mark_native_cdc_success(...)`
- `mark_native_cdc_failure(...)`
- generated `pull_<table>_batch(...)`
- generated `sp_merge_<table>(...)`

The runner should not need table-specific SQL embedded in Bun code.

All table-specific work should stay generated and deterministic.

## Operational Best Practices

### Logging

Emit structured logs with at least:

- `worker_id`
- `source_instance_key`
- `logical_table_name`
- claimed or skipped status
- rows pulled
- rows merged
- duration
- failure category

### Metrics

Track at least:

- due work claimed per second
- empty pulls
- non-empty pulls
- merge duration
- remote FDW pull duration
- failure count by class
- lease renewals
- overdue registrations

### Health Endpoints

If an HTTP server is added, expose:

- readiness: can talk to PostgreSQL
- liveness: event loop healthy
- metrics endpoint

### Failure Classification

Differentiate at minimum:

- transient network timeout
- authentication or configuration error
- retention-gap error
- SQL execution error
- merge failure

Failure classification should feed both backoff behavior and alerting.

## What Not To Do

Do not:

- make in-memory cache the source of truth
- couple correctness to one leader pod
- move merge logic from SQL into Bun
- advance checkpoints outside the merge transaction boundary
- use Kubernetes CronJobs as the primary sub-minute scheduler
- poll every table every second forever regardless of activity

## Suggested Delivery Plan

### Phase 1: Database Foundations

- introduce `native_cdc_schedule_policy` and `native_cdc_runtime_state`
- backfill split tables from the current registration row
- add runtime health view
- add lease-renewal procedure
- keep current generator output working during transition

### Phase 2: One-Shot Runner Contract

- switch generated scheduler procedures to the split-table model
- implement `cdc native-cdc run-due`
- verify the Bun runtime can call the same DB contract later
- validate error handling and success bookkeeping

### Phase 3: Bun Runner Skeleton

- create `asma-bun-cdcrunner`
- implement heartbeat loop
- implement claim, pull, merge, mark-success, mark-failure flow
- derive `worker_id` from pod identity

### Phase 4: Replica Safety

- run `2` replicas in nonprod
- validate lease expiry recovery
- validate no duplicate processing under concurrent claim pressure

### Phase 5: Policy Cache And Observability

- add short-TTL policy cache
- add `config_version` invalidation
- add metrics and dashboards

### Phase 6: Registration Cleanup And Adaptive Policy Rollout

- stop using registration as the hot scheduler state row
- remove or tombstone legacy hot columns from `source_table_registration`
- enable hot or warm or cold profiles
- validate idle backoff and burst acceleration behavior
- tune business-hours policy

## Final Recommendation

For long-term production design, the right architecture is:

- generator-managed SQL primitives in PostgreSQL
- Option B split between policy state and runtime state
- stateless active/active Bun worker deployment
- PostgreSQL-owned leases and checkpoint safety
- selective cache only for cold policy metadata

That is the cleanest path to sub-minute adaptive polling without turning the runtime into a fragile distributed scheduler.
