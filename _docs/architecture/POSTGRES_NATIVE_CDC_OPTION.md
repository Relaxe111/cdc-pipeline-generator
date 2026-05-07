# PostgreSQL-Native CDC Option

> Status: Proposed architecture option
> Last updated: 2026-03-26

---

## Executive Summary

This option keeps the current CDC control plane intact while replacing the Redpanda/Bento transport layer with a PostgreSQL-native data plane.

For the broader composition model that proposes a simpler hierarchy of `pattern -> source type -> topology`, see:

- `TOPOLOGY_RUNTIME_COMPOSITION.md`

What stays the same:

- `cdc manage-source-groups` remains the source topology entry point.
- `cdc manage-sink-groups` remains the sink topology entry point.
- `cdc manage-services resources` remains the source schema inspection and table selection entry point.
- `cdc manage-migrations generate|diff|apply|status` remains the schema and migration lifecycle.
- The existing staging pattern, merge procedures, and delete-aware CDC semantics remain the core apply model.

What changes:

- MSSQL source ingestion is pulled directly into PostgreSQL via `tds_fdw` against MSSQL CDC objects.
- PostgreSQL-to-PostgreSQL transport uses native logical replication instead of an external broker.
- The runtime becomes database-centric rather than broker-centric.

The goal is to reduce moving parts, avoid broker operations overhead, and keep local microservice databases self-sufficient while preserving the existing CLI-driven workflow.

---

## Scope

This document describes a simplified runtime option for the generator architecture. It does not propose removing:

- source-group management
- sink-group management
- service-level table selection
- schema inspection
- SQL migration generation
- staging tables
- merge/upsert logic
- delete handling
- checkpointing

It only replaces the transport and event-delivery mechanism.

---

## Why Consider This Option

The current brokered design is strong when we need high fan-out, replayable streams, independent consumers, or cross-platform event integration. It is heavier than necessary when the real requirement is narrower:

- pull CDC changes from MSSQL into PostgreSQL
- replicate PostgreSQL changes to downstream PostgreSQL databases
- preserve inserts, updates, and deletes
- keep local service databases independently recoverable

For that narrower requirement, PostgreSQL already gives us most of the mechanics we need.

---

## Proposed Architecture

### 1. MSSQL -> PostgreSQL Local Service Database

For MSSQL sources, PostgreSQL becomes the pull engine.

Flow:

```text
MSSQL source tables
      |
      | CDC enabled on source tables
      v
MSSQL cdc.<capture_instance>_CT tables
      |
      | tds_fdw foreign tables in PostgreSQL
      v
PostgreSQL local staging tables
      |
      | merge procedure / upsert-delete apply logic
      v
PostgreSQL final service tables
```

Key point: `tds_fdw` is read-only, which is acceptable here because we only need to read CDC changes from MSSQL.

### 2. PostgreSQL -> PostgreSQL Downstream Replication

For PostgreSQL sources, or for downstream fan-out after local materialization, use logical replication.

Flow:

```text
PostgreSQL source database
      |
      | PUBLICATION
      v
PostgreSQL subscriber database
      |
      | native pull via SUBSCRIPTION
      v
Local replicated tables
```

Logical replication is already pull-based on the subscriber side, which fits the desired stability model.

### 3. Recommended Topology

Recommended:

- MSSQL -> service-local PostgreSQL via `tds_fdw`
- service-local PostgreSQL -> optional downstream PostgreSQL replicas via logical replication

Not recommended as the default:

- long-lived `dev -> stage -> prod` replication chains

That chain couples environments and reintroduces the kind of domino effect this proposal is trying to reduce. If environments must remain isolated, each environment should ingest independently from its own approved source path.

### 4. Canonical Owner Database Rule

For MSSQL-derived tables, "service-local PostgreSQL" should be read as the canonical owner database for that table, not as every database that happens to consume a copy.

Recommended default:

- pull each source table once from MSSQL into one owner PostgreSQL database
- keep checkpoints, staging tables, merge procedures, and runtime state in that owner database
- fan out to additional ASMA service databases from PostgreSQL, not by repeating the MSSQL pull in each consumer database
- choose the owner by domain ownership; shared reference tables may land in `directory`, while domain-owned tables should land in the database that owns that domain

Avoid by default:

- registering the same MSSQL CDC table as direct FDW/native ingestion work in multiple consumer databases

Why:

- one checkpoint stream per source table and customer
- less load on MSSQL CDC tables and the source server
- simpler recovery, because consumers can be rebuilt from PostgreSQL rather than by rereading MSSQL
- clearer schema ownership and downstream contracts

### 5. Administration And Versioning Model

For both FDW-backed MSSQL ingestion and PostgreSQL-native downstream fan-out, the normal administration path in this workspace should stay generator-managed.

Recommended default:

- keep implementation YAML and inspected schema files as the input source of truth
- generate versioned SQL and runtime helpers through `cdc` commands from `cdc-pipeline-generator`
- keep generated migration artifacts versioned in the implementation repository that owns the runtime
- treat live database state as the result of generator-managed inputs, not as the authoring surface

Manual SQL or setup is still possible, but it should be treated as a fallback path for debugging, one-off bootstrap, or gaps in current CLI support. Any manual change that becomes permanent must be reconciled back into the implementation inputs and regenerated artifacts so the repository remains the source of truth.

---

## What We Keep From The Current CDC Model

The current generator model still makes sense in a native PostgreSQL runtime:

### Control Plane

- `cdc manage-source-groups` defines source connectivity and source behavior.
- `cdc manage-sink-groups` defines sink connectivity and sink behavior.
- `cdc manage-services resources` keeps the source-of-truth table list and inspected schema definitions.
- `cdc manage-migrations` continues to own DDL generation and application.

### Data Shape Management

- service YAML remains the definition of tracked tables
- generated PostgreSQL DDL remains the sink schema contract
- staging tables remain the safety boundary between raw changes and final materialized state

### Apply Semantics

- upsert for inserts and updates
- explicit delete handling
- checkpoint persistence
- merge procedures and monitoring

This means the native option is a runtime replacement, not a control-plane rewrite.

---

## MSSQL Path Using `tds_fdw`

### Why `tds_fdw`

`tds_fdw` lets PostgreSQL map Microsoft SQL Server objects as foreign tables through FreeTDS. For this design, PostgreSQL can read directly from MSSQL CDC objects without an intermediate broker.

Relevant properties:

- supports Microsoft SQL Server over TDS
- read-oriented access is sufficient for our use case
- supports PostgreSQL 15 and PostgreSQL 17 builds
- requires FreeTDS configuration and extension installation on the PostgreSQL side

### What To Read From MSSQL

We should read from MSSQL CDC capture tables or CDC functions, depending on the source design we standardize on:

- `cdc.<capture_instance>_CT` tables when we want direct access to all raw CDC rows
- `cdc.fn_cdc_get_all_changes_<capture_instance>` when we want SQL Server to filter by LSN window for us

For a first implementation, direct reads from `cdc.<capture_instance>_CT` are simpler to reason about because they expose:

- `__$start_lsn`
- `__$seqval`
- `__$operation`
- `__$update_mask`
- captured business columns

### Delete Handling

Deletes are preserved by the MSSQL CDC operation code.

Typical MSSQL CDC operation meanings:

| Operation | Meaning |
|---|---|
| `1` | delete |
| `2` | insert |
| `3` | update before image |
| `4` | update after image |

For the simplified runtime, the safe default is:

- ignore `3` for final-state materialization
- apply `2` and `4` as upserts
- apply `1` as delete

If we need audit or history tables later, we can preserve `3` in a raw history store, but it is not required for the main sink tables.

### Checkpointing

Checkpoint state should live in PostgreSQL, not in the runtime filesystem.

Recommended checkpoint table:

```sql
CREATE TABLE IF NOT EXISTS cdc_management.native_cdc_checkpoint (
    source_name text NOT NULL,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    last_start_lsn text NOT NULL,
    last_seqval text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_name, schema_name, table_name)
);
```

Why this matters:

- crash-safe state is in the database
- restart does not depend on a container volume
- operational visibility is much better than file-based cursor state

### Staging And Apply Loop

Recommended sequence for each table:

1. Read new MSSQL CDC rows through the foreign table using the last stored LSN.
2. Insert raw rows into a PostgreSQL staging table.
3. Apply `INSERT ... ON CONFLICT DO UPDATE` for operations `2` and `4`.
4. Apply `DELETE` for operation `1`.
5. Advance checkpoint only after the transaction commits successfully.

Pseudo-flow:

```text
read FDW rows > stage raw rows > merge to final table > update checkpoint
```

That preserves at-least-once pull behavior while keeping final tables convergent.

---

## PostgreSQL Path Using Logical Replication

Logical replication is the native option for PostgreSQL-to-PostgreSQL streaming.

### Why It Fits

- subscriber pulls changes from publisher
- transactional ordering is preserved within a subscription
- cascading is supported when a subscriber republishes onward
- cross-major-version replication is supported

### Requirements

On the publisher:

- `wal_level = logical`
- enough `max_replication_slots`
- enough `max_wal_senders`

On the subscriber:

- tables must already exist
- schema changes are not replicated automatically
- replication conflicts must be treated as operational failures, not hidden behavior

### Delete And Update Rules

Logical replication depends on replication identity.

For published tables, ensure one of the following:

- a primary key exists
- a suitable replica identity exists
- `REPLICA IDENTITY FULL` is used only when necessary

If tables rely on `REPLICA IDENTITY FULL`, be careful with data types that do not support the required comparison behavior. Primary-key-based replication remains the cleaner option.

---

## Stability-First Sync Logic

This proposal is valid only if restart and outage behavior are boring and predictable.

### MSSQL Outage Or Network Break

If PostgreSQL cannot reach MSSQL temporarily:

- MSSQL keeps accumulating changes in CDC objects until retention cleanup removes them
- PostgreSQL resumes from the last committed checkpoint when connectivity returns

This is the correct recovery behavior, but only if CDC retention is long enough.

### PostgreSQL Subscriber Outage

If a logical replication subscriber is offline:

- the publisher replication slot retains WAL
- the subscriber resumes from its last confirmed position after recovery

This is also the correct recovery behavior, but only if WAL retention is bounded and monitored.

### Final-Table Convergence

The final tables should be treated as state stores, not append-only logs.

Rules:

- inserts and update-after rows become upserts
- deletes remove the target row
- checkpoints move only after the full apply transaction succeeds

That gives us deterministic convergence even when the same change is replayed.

---

## Infrastructure Requirements

### PostgreSQL

Required:

- `wal_level = logical`
- `max_replication_slots` sized for real subscriptions plus maintenance headroom
- `max_wal_senders` sized for active replication sessions
- `max_slot_wal_keep_size` set to a bounded value such as `10GB` or another environment-appropriate ceiling

Recommended:

- `pg_cron` if we want in-database scheduled pull/apply jobs
- separate `cdc_management` schema for checkpoints, monitors, and control tables

### MSSQL

Required:

- CDC enabled on tracked source tables
- retention configured long enough to survive expected outages
- capture instances standardized so generator output is predictable

### `tds_fdw`

Required on PostgreSQL hosts that will pull from MSSQL:

- `tds_fdw` extension installed
- FreeTDS installed and configured
- encrypted connectivity defined in `freetds.conf` if required by policy
- validated TDS protocol version for Unicode and newer MSSQL servers

---

## Risks And Tradeoffs

### What We Gain

- fewer moving parts
- no broker cluster to operate
- tighter operational model around PostgreSQL only
- easier reasoning for pure database replication cases

### What We Give Up

- no durable broker buffer between producer and consumer tiers
- less natural multi-consumer fan-out than Kafka-style topics
- weaker replay tooling for arbitrary downstream consumers
- more direct load on source and target databases

### Specific Risks

#### 1. WAL Bloat On Publisher

If a subscriber or slot is stalled, WAL can accumulate indefinitely unless bounded.

Mitigation:

- set `max_slot_wal_keep_size`
- monitor slot lag
- alert on inactive subscriptions and slot retention growth

#### 2. MSSQL CDC Retention Gaps

If MSSQL retention is too short, an outage can create an unrecoverable LSN gap.

Mitigation:

- size retention to exceed realistic outage windows
- alert when checkpoint age approaches CDC cleanup horizon

#### 3. Schema Drift

Logical replication does not replicate DDL.

Mitigation:

- keep `cdc manage-migrations` as the required schema synchronization path
- apply additive sink changes before source-side changes when possible

#### 4. `tds_fdw` Pushdown Limits

`tds_fdw` does not give us full pushdown behavior and is not a write path.

Mitigation:

- keep remote reads narrow
- read only CDC objects, not broad joined source queries
- materialize locally before doing heavier PostgreSQL work

---

## PostgreSQL 17 Nonprod vs PostgreSQL 15 Prod

This matters.

### Safe Position

Design to the PostgreSQL 15 feature floor, even if nonprod is already on 17.

Practical rules:

- do not depend on replication features that are newer than PostgreSQL 15
- validate publication, subscription, row-filter, and column-list behavior against 15 semantics
- keep operational runbooks compatible with 15 first, then allow 17 conveniences where they are optional

### Cross-Version Replication

Logical replication supports replication between different major PostgreSQL versions. That means 15 and 17 can participate in the same logical replication design.

However, for long-lived production topology, mixed major versions still increase operational complexity. The right posture is:

- keep features compatible with 15
- test exact publisher/subscriber direction in a representative environment
- avoid treating cross-version support as a substitute for version standardization

### Upgrade Operations

One important nuance: newer PostgreSQL versions improve `pg_upgrade` handling for logical replication metadata, but migration of logical slots and subscription dependencies during upgrade is only supported when the old cluster is version 17 or later.

Implication for this estate:

- the production PostgreSQL 15 estate should not assume the smoother logical-replication upgrade path available to 17+
- future major upgrades from 15 need a more explicit replication runbook: disable subscriptions, upgrade carefully, validate tables, then refresh or re-enable replication as needed

### Recommendation

For rollout:

- allow nonprod experiments on 17
- certify the design against 15 behavior before calling it production-ready
- if possible, standardize all logical replication participants onto the same major version before broad rollout

### Verified Nonprod State

Verified on 2026-03-26 from the generator dev container using the configured nonprod PostgreSQL credentials in `.env`:

- server version is `17.9`
- `wal_level` is currently `replica`, not `logical`
- `tds_fdw` is installed in the checked database
- `tds_fdw` is also available at the server level

Operational meaning:

- the `tds_fdw` prerequisite is already satisfied on the checked nonprod PostgreSQL database
- PostgreSQL logical replication is not yet ready on that server because `wal_level` still needs to be changed to `logical`
- enabling logical replication will require a PostgreSQL configuration change and a restart or managed-service equivalent maintenance action

---

## Recommended Rollout Plan

### Phase 1: Prove The MSSQL Pull Bridge

- verify and complete `tds_fdw` and FreeTDS readiness on a nonprod PostgreSQL 17 host
- map one MSSQL CDC capture table through foreign tables
- implement one checkpoint table and one pull/apply procedure
- verify inserts, updates, and deletes

### Phase 2: Reuse Existing Generator Assets

- keep current service YAML and migration generation
- generate final tables and staging tables exactly as today
- wire native pull jobs to those generated tables instead of Bento sink ingestion

### Phase 3: Add PostgreSQL Logical Replication

- change `wal_level` from `replica` to `logical` in nonprod before testing publications and subscriptions
- publish final materialized tables from the local PostgreSQL service database
- subscribe from a downstream PostgreSQL database
- test slot retention, lag monitoring, and restart behavior

### Phase 4: Operational Hardening

- add monitoring for MSSQL checkpoint lag
- add monitoring for PostgreSQL slot lag and WAL retention
- document outage recovery and rebootstrap procedures
- prove behavior under source outage, subscriber outage, and schema change events

---

## Assessment

This is a credible simplification if the problem is primarily:

- MSSQL CDC ingestion
- PostgreSQL materialization
- PostgreSQL downstream replication

It is not a universal replacement for a brokered architecture. If we still need broad fan-out, non-PostgreSQL consumers, long retention replay, or strong producer-consumer isolation, Redpanda or Kafka-class tooling remains the better fit.

For the narrower use case described here, the native PostgreSQL option is operationally simpler and matches the current generator model well because it preserves the existing control plane while replacing only the transport runtime.

---

## Bottom Line

Keep the current CLI and migration system.

Replace:

- MSSQL -> Redpanda with MSSQL CDC -> `tds_fdw` -> PostgreSQL staging/final tables
- PostgreSQL -> Redpanda -> PostgreSQL with native PostgreSQL logical replication where downstream PostgreSQL replication is required

Do the design against PostgreSQL 15 compatibility first, even if nonprod is already on 17.
