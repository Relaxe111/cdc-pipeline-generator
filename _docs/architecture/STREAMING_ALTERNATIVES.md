# Streaming Alternatives: Bytewax, Materialize & Vector

> **Status:** Research & evaluation phase — neither technology is currently in use.
> **Last updated:** February 2025

---

## Table of Contents

- [Streaming Alternatives: Bytewax, Materialize \& Vector](#streaming-alternatives-bytewax-materialize--vector)
  - [Table of Contents](#table-of-contents)
  - [Current Architecture](#current-architecture)
  - [Bytewax](#bytewax)
    - [What Is Bytewax](#what-is-bytewax)
    - [Bytewax Strengths](#bytewax-strengths)
    - [Bytewax Potential Use Cases](#bytewax-potential-use-cases)
      - [1. High-Performance Sink Layer (Primary Use Case)](#1-high-performance-sink-layer-primary-use-case)
      - [2. Cache Invalidation / Event Hooks](#2-cache-invalidation--event-hooks)
      - [3. Complex Transformations](#3-complex-transformations)
    - [What Bytewax Replaces](#what-bytewax-replaces)
    - [What Bytewax Does NOT Replace](#what-bytewax-does-not-replace)
    - [When to Introduce Bytewax](#when-to-introduce-bytewax)
  - [Materialize](#materialize)
    - [What Is Materialize](#what-is-materialize)
    - [Materialize Strengths](#materialize-strengths)
    - [Materialize Potential Use Cases](#materialize-potential-use-cases)
      - [1. Real-Time Reporting / Analytics Layer](#1-real-time-reporting--analytics-layer)
      - [2. Cross-Service Data Joins](#2-cross-service-data-joins)
      - [3. Event-Driven Triggers via SUBSCRIBE](#3-event-driven-triggers-via-subscribe)
      - [4. Streaming Data Validation](#4-streaming-data-validation)
    - [What Materialize Replaces](#what-materialize-replaces)
    - [What Materialize Does NOT Replace](#what-materialize-does-not-replace)
    - [When to Introduce Materialize](#when-to-introduce-materialize)
  - [Vector](#vector)
    - [What Is Vector](#what-is-vector)
    - [Vector Strengths](#vector-strengths)
    - [Vector Potential Use Cases](#vector-potential-use-cases)
    - [What Vector Replaces](#what-vector-replaces)
    - [What Vector Does NOT Replace](#what-vector-does-not-replace)
    - [When to Introduce Vector](#when-to-introduce-vector)
  - [Comparison Matrix](#comparison-matrix)
  - [Phased Introduction Plan](#phased-introduction-plan)
    - [Phase 0 — Current State (Now)](#phase-0--current-state-now)
    - [Phase 1 — Hook Dispatcher (When cache invalidation is needed)](#phase-1--hook-dispatcher-when-cache-invalidation-is-needed)
    - [Phase 2 — Bytewax Sink (When performance demands it)](#phase-2--bytewax-sink-when-performance-demands-it)
    - [Phase 3 — Materialize (When real-time analytics is needed)](#phase-3--materialize-when-real-time-analytics-is-needed)
    - [Phase 4 — Full Event-Driven Architecture (Long-term vision)](#phase-4--full-event-driven-architecture-long-term-vision)
  - [Decision Criteria](#decision-criteria)
    - [Must-Have Before Adopting](#must-have-before-adopting)
    - [Bytewax Adoption Checklist](#bytewax-adoption-checklist)
    - [Materialize Adoption Checklist](#materialize-adoption-checklist)
    - [Vector Adoption Checklist](#vector-adoption-checklist)
  - [References](#references)

---

## Current Architecture

```
Source DB (MSSQL/PG)
  → CDC capture (Redpanda Connect source pipeline)
    → Kafka/Redpanda topics
      → Redpanda Connect sink pipeline
        → staging table (INSERT)
          → MERGE into target table (SQL)
            → PostgreSQL target
```

Redpanda Connect handles **both** the source capture and sink delivery today. It works well for straightforward CDC replication, but has limitations when requirements grow beyond simple row-level replication.

---

## Bytewax

### What Is Bytewax

[Bytewax](https://bytewax.io/) is a **Python-native stream processing framework** built on Rust (via Timely Dataflow). It provides a dataflow API where you write Python functions for map, filter, window, join, and reduce operations on streaming data.

**Key characteristics:**
- Pure Python API — no JVM, no Scala, no external DSL
- Built on Rust/Timely Dataflow for performance
- Stateful processing with built-in recovery (snapshots, Kafka offset tracking)
- Runs as a regular Python process (no cluster manager required)
- Can read from Kafka, files, HTTP, custom sources
- Can write to any destination via custom Python sink code

### Bytewax Strengths

| Strength | Why It Matters for Us |
|----------|----------------------|
| **Python-native** | Our entire codebase is Python — zero learning curve |
| **Custom sink logic** | Can implement PostgreSQL binary COPY protocol for 10-50× throughput vs row-level INSERT |
| **Stateful processing** | Windowed aggregations, sessionization, deduplication built-in |
| **Lightweight deployment** | Single Python process, Docker-friendly, no Kafka Streams cluster |
| **Flexible transformations** | Full Python expressiveness vs Bloblang's limited DSL |
| **Exactly-once semantics** | Kafka offset tracking + recovery snapshots |

### Bytewax Potential Use Cases

#### 1. High-Performance Sink Layer (Primary Use Case)

Replace Redpanda Connect sink pipelines with a Bytewax dataflow that:

- Reads CDC events from Kafka topics
- Transforms/maps columns using Python (instead of Bloblang)
- Batches rows and writes via PostgreSQL **binary COPY protocol**
- Handles merge/upsert logic in Python with conflict resolution

**Why:** The binary COPY protocol can deliver **10-50× throughput** compared to row-level INSERTs through Redpanda Connect. For large initial loads or high-volume CDC streams, this is a significant advantage.

```
Kafka topic
  → Bytewax dataflow (Python)
    → batch rows
      → PG binary COPY into staging
        → SQL MERGE into target
```

#### 2. Cache Invalidation / Event Hooks

A Bytewax dataflow can sit alongside the sink pipeline and:

- Watch for specific row changes (filter by table, column values)
- Dispatch cache invalidation events (Redis PUBLISH, HTTP webhook)
- Trigger downstream workflows based on data changes

**Why:** Redpanda Connect cannot easily fan-out CDC events to multiple destinations with conditional logic. Bytewax's Python filter/map makes this trivial.

#### 3. Complex Transformations

For tables that need transformations beyond what Bloblang can express:

- Multi-table joins (enrich CDC events with reference data)
- Windowed aggregations (rolling counts, time-based summaries)
- Conditional routing (different sinks based on event content)

### What Bytewax Replaces

- **Redpanda Connect sink pipelines** — Bytewax can fully replace the sink side
- **Bloblang transformations** — replaced by Python functions
- **Staging → merge SQL** — can be handled in the Bytewax sink logic

### What Bytewax Does NOT Replace

- **CDC source capture** — Bytewax does not have built-in CDC connectors for MSSQL or PostgreSQL logical replication. Redpanda Connect (or Debezium) still handles source capture.
- **Kafka/Redpanda** — Bytewax reads FROM Kafka; it does not replace the message broker.
- **Pipeline YAML generation** — our generator library still produces configuration; Bytewax would be an alternative sink runtime.

### When to Introduce Bytewax

**Introduce when ANY of these triggers occur:**

| Trigger | Signal |
|---------|--------|
| **Performance bottleneck** | Sink pipeline throughput is insufficient; row-level INSERTs are too slow for volume |
| **Cache invalidation needed** | Application requires near-real-time cache busting on data changes |
| **Complex transformation** | A table needs joins, windowing, or logic that Bloblang cannot express cleanly |
| **Binary COPY requirement** | Initial bulk load or high-throughput sync demands binary COPY protocol |
| **Multi-destination fanout** | Same CDC event must go to DB + cache + webhook + analytics |

**Do NOT introduce for:**
- Simple 1:1 table replication (Redpanda Connect handles this fine)
- Small volume CDC streams (overhead not justified)
- Source-side capture (Bytewax has no CDC source connectors)

**Estimated effort to prototype:** 1-2 weeks for a single-table Bytewax sink replacing one Redpanda Connect sink pipeline.

---

## Materialize

### What Is Materialize

[Materialize](https://materialize.com/) is a **streaming SQL database** that maintains incrementally updated materialized views over streaming data. You write standard SQL `CREATE MATERIALIZED VIEW` statements, and Materialize keeps the results up-to-date as source data changes in real time.

**Key characteristics:**
- PostgreSQL wire-compatible (connect with any PG client, psql, SQLAlchemy)
- Reads from Kafka, PostgreSQL CDC, webhooks, S3
- Maintains materialized views incrementally (not periodic refresh)
- Sub-second view freshness on streaming data
- Supports joins, aggregations, windows, temporal filters in SQL
- Managed cloud service (Materialize Cloud) or self-hosted

### Materialize Strengths

| Strength | Why It Matters for Us |
|----------|----------------------|
| **Streaming SQL** | No custom code — define views in SQL, Materialize maintains them |
| **Incremental maintenance** | Views update in milliseconds as source data changes |
| **PG wire-compatible** | Applications connect as if it were PostgreSQL |
| **Multi-source joins** | Join CDC streams from different databases in real time |
| **Temporal queries** | Built-in support for time-windowed aggregations |
| **No sink code** | Materialize IS the query layer — no separate sink pipeline needed |

### Materialize Potential Use Cases

#### 1. Real-Time Reporting / Analytics Layer

Instead of replicating data into PostgreSQL and running queries there, Materialize can:

- Ingest CDC events directly from Kafka
- Maintain pre-computed dashboards, KPIs, aggregations
- Serve queries with sub-second latency on always-fresh data

**Why:** Traditional approach requires ETL + periodic refresh. Materialize gives real-time freshness with zero ETL code.

#### 2. Cross-Service Data Joins

When data from multiple services (adopus, directory, calendar) needs to be joined:

- Each service's CDC stream feeds into Materialize
- SQL views join across services in real time
- Applications query the joined view as a regular table

**Why:** Today, cross-service joins require replicating all data to a shared database first. Materialize eliminates this step.

#### 3. Event-Driven Triggers via SUBSCRIBE

Materialize's `SUBSCRIBE` (formerly TAIL) lets applications receive push notifications when a materialized view changes:

```sql
SUBSCRIBE TO my_materialized_view;
-- Receives a row for every change to the view result
```

This enables:
- Cache invalidation when aggregated values change
- Alerting when KPIs cross thresholds
- Webhook dispatch on computed conditions

#### 4. Streaming Data Validation

Materialize can continuously validate data quality:

```sql
CREATE MATERIALIZED VIEW data_quality_issues AS
SELECT * FROM cdc_events
WHERE required_field IS NULL
   OR amount < 0
   OR customer_id NOT IN (SELECT id FROM customers);
```

Issues appear in real time as bad data flows through.

### What Materialize Replaces

- **Reporting database** — Materialize can serve as the real-time query layer, reducing need for a separate analytics DB
- **ETL aggregation jobs** — materialized views replace batch aggregation scripts
- **Custom join/enrichment logic** — SQL views replace code-level joins

### What Materialize Does NOT Replace

- **Operational database** — Materialize is not a transactional OLTP database; it doesn't support UPDATE/DELETE/INSERT from applications
- **CDC source capture** — still need Redpanda Connect or Debezium to capture changes and publish to Kafka
- **Sink replication** — if the goal is a writable replica of the source database, Materialize is not the right tool (it's read-only views)
- **Kafka/Redpanda** — Materialize reads from Kafka; it does not replace the broker

### When to Introduce Materialize

**Introduce when ANY of these triggers occur:**

| Trigger | Signal |
|---------|--------|
| **Real-time reporting** | Stakeholders need dashboards that refresh in seconds, not minutes/hours |
| **Cross-service queries** | Applications need to join data from multiple CDC streams |
| **Complex streaming aggregations** | Rolling windows, sessionization, or multi-step aggregations on CDC data |
| **SUBSCRIBE-based events** | Need push-based notifications when computed values change |
| **Eliminating batch ETL** | Want to replace scheduled aggregation jobs with continuous computation |

**Do NOT introduce for:**
- Simple 1:1 table replication to a target database
- Transactional workloads (OLTP reads/writes)
- Small-scale CDC with no analytics requirements
- Environments where adding another stateful service is unacceptable

**Estimated effort to prototype:** 2-3 weeks for a proof-of-concept with Kafka source → materialized views → application queries.

---

## Vector

### What Is Vector

[Vector](https://vector.dev/) is a **high-performance observability data pipeline** written in Rust. It is designed for collecting, transforming, and routing logs/metrics/traces with low overhead.

In our context, Vector is not a replacement for CDC itself; it is a strong candidate for **pipeline telemetry and event diagnostics transport**.

**Key characteristics:**
- Rust-based, low CPU and memory footprint
- Very high throughput, backpressure-aware buffering
- Enrichment/transforms via VRL (Vector Remap Language)
- Many sources/sinks: Kafka, HTTP, files, OpenTelemetry, Elasticsearch, S3, ClickHouse, etc.
- Built-in delivery guarantees, retries, disk buffers

### Vector Strengths

| Strength | Why It Matters for Us |
|----------|----------------------|
| **Low overhead** | Good fit for always-on telemetry pipelines in production |
| **Excellent routing** | Can fan out pipeline diagnostics to multiple backends (Kafka, S3, Elastic, etc.) |
| **Strong reliability** | Backpressure + buffering + retries reduce observability data loss |
| **VRL transforms** | Can normalize/shape tracing events before storage |
| **Operational maturity** | Widely used for log/metric forwarding at scale |

### Vector Potential Use Cases

1. **CDC tracing transport layer**
  - Ship sampled trace events (from source/sink pipelines) to a dedicated observability stream.
  - Route same data to PostgreSQL + object storage (cold retention) + alerting sink.

2. **Unified operational telemetry**
  - Centralize Redpanda Connect logs, health metrics, and pipeline events.
  - Build one ingestion path for dashboards and alerts.

3. **Cost control for observability**
  - Apply filtering/sampling at ingestion time.
  - Keep high-value events hot; archive full raw events cheaply.

### What Vector Replaces

- **Ad-hoc telemetry forwarders** (custom scripts/sidecars for logs and diagnostics)
- **Part of hook-dispatch plumbing** for pure routing/filtering use cases

### What Vector Does NOT Replace

- **CDC source capture** from MSSQL/PG (keep Redpanda Connect/Debezium)
- **Business sink replication** into target DB (Vector is not a CDC merge engine)
- **Streaming SQL analytics layer** (Materialize still fills that role)

### When to Introduce Vector

**Introduce when ANY of these triggers occur:**

| Trigger | Signal |
|---------|--------|
| **Telemetry sprawl** | Multiple custom scripts are forwarding logs/traces inconsistently |
| **Observability cost pressure** | Need ingestion filtering/sampling/routing to control storage cost |
| **Reliability issues in monitoring path** | Lost diagnostics during incidents due to weak buffering/retry |
| **Need multi-sink diagnostics** | Same trace event must go to DB + object storage + alert stream |

**Do NOT introduce for:**
- Replacing CDC replication logic
- Replacing Kafka broker
- Solving SQL analytics/query needs

**Estimated effort to prototype:** 3-5 days for telemetry routing PoC (Kafka source → Vector transform → multi-sink output).

---

## Comparison Matrix

| Dimension | Redpanda Connect (current) | Bytewax | Materialize | Vector |
|-----------|---------------------------|---------|-------------|--------|
| **Primary role** | CDC source + sink connector | Stream processor (sink-side) | Streaming SQL query engine | Telemetry/log routing pipeline |
| **Language** | YAML + Bloblang | Python | SQL | TOML + VRL |
| **CDC source capture** | ✅ Built-in (MSSQL, PG) | ❌ No CDC connectors | ⚠️ PG CDC only (no MSSQL) | ❌ Not a CDC capture tool |
| **Sink to PostgreSQL** | ✅ Row-level INSERT | ✅ Binary COPY (10-50× faster) | ❌ Read-only views | ⚠️ Can write events, not CDC merge semantics |
| **Complex transforms** | ⚠️ Limited (Bloblang) | ✅ Full Python | ✅ Full SQL | ✅ Strong event transforms (VRL) |
| **Joins / aggregations** | ❌ Not supported | ✅ Stateful dataflows | ✅ Materialized views | ❌ Not a stream compute engine |
| **Cache invalidation** | ❌ Not built-in | ✅ Custom Python hooks | ✅ SUBSCRIBE | ⚠️ Routing possible, logic limited |
| **Deployment** | Docker container | Docker / Python process | Managed cloud / Docker | Docker / binary agent |
| **Operational complexity** | Low | Medium | Medium-High | Low-Medium |
| **Learning curve for us** | Already known | Low (Python) | Medium (streaming SQL concepts) | Low-Medium (VRL) |
| **Maturity** | Production-proven | Growing (v0.x → v1.x) | Production-ready (cloud) | Production-proven |

---

## Phased Introduction Plan

### Phase 0 — Current State (Now)

```
MSSQL/PG → Redpanda Connect Source → Kafka → Redpanda Connect Sink → PostgreSQL
```

**Focus:** Stabilize current CDC pipelines, complete db-shared pattern, improve CLI tooling.

### Phase 1 — Hook Dispatcher (When cache invalidation is needed)

```
Kafka → Redpanda Connect Sink → PostgreSQL
                                    ↓
                            Hook Dispatcher (Python)
                                    ↓
                        Redis / Webhook / Event Bus
```

**Add a lightweight Python hook dispatcher** that listens to Kafka topics and dispatches events. This is simpler than introducing Bytewax and solves the immediate cache invalidation need.

**Trigger:** First application requests near-real-time cache invalidation.

### Phase 2 — Bytewax Sink (When performance demands it)

```
Kafka → Bytewax Dataflow → PG binary COPY → PostgreSQL
                         → Hook dispatch → Redis / Webhooks
```

**Replace Redpanda Connect sink pipelines** with Bytewax for tables that need:
- Higher throughput (binary COPY)
- Complex transformations
- Multi-destination fanout

Keep Redpanda Connect sink for simple tables where it works fine.

**Trigger:** Sink throughput bottleneck OR complex transformation requirement.

### Phase 3 — Materialize (When real-time analytics is needed)

```
Kafka → Bytewax Sink → PostgreSQL (operational replica)
     → Materialize (real-time analytics / cross-service joins)
```

**Add Materialize as a parallel consumer** of Kafka topics for:
- Real-time dashboards and reporting
- Cross-service data joins
- Streaming data quality validation

**Trigger:** Stakeholders need real-time reporting OR cross-service join requirements emerge.

### Phase 4 — Full Event-Driven Architecture (Long-term vision)

```
Source DBs → CDC → Kafka → Bytewax (sink + hooks)
                         → Materialize (analytics)
                         → Vector (telemetry routing)
                         → Other consumers (search index, data lake, etc.)
```

Kafka becomes the central event bus. Multiple consumers process the same CDC streams for different purposes.

---

## Decision Criteria

Before introducing either technology, evaluate against these criteria:

### Must-Have Before Adopting

- [ ] Current Redpanda Connect pipeline is stable and in production
- [ ] Clear use case that current stack cannot handle
- [ ] Team capacity for learning and maintaining additional technology
- [ ] Monitoring and alerting infrastructure in place

### Bytewax Adoption Checklist

- [ ] Identified specific tables with throughput bottleneck
- [ ] Measured current INSERT throughput vs required throughput
- [ ] Prototype binary COPY sink for one table
- [ ] Recovery/replay mechanism tested (Kafka offset snapshots)
- [ ] Deployment pipeline supports Python process alongside Redpanda Connect

### Materialize Adoption Checklist

- [ ] Identified specific real-time query requirements
- [ ] Evaluated Materialize Cloud pricing vs self-hosted cost
- [ ] Tested PG wire compatibility with target application frameworks
- [ ] Validated Kafka source connector configuration
- [ ] Assessed operational overhead of running another stateful service

### Vector Adoption Checklist

- [ ] Identified telemetry flows currently handled by custom scripts/sidecars
- [ ] Defined required sinks (DB, object storage, alerts, search)
- [ ] Designed sampling and retention policy for diagnostics data
- [ ] Validated buffering/retry behavior under broker outage tests
- [ ] Confirmed observability schema/versioning for long-term compatibility

---

## References

- [Bytewax Documentation](https://docs.bytewax.io/)
- [Bytewax GitHub](https://github.com/bytewax/bytewax)
- [Materialize Documentation](https://materialize.com/docs/)
- [Materialize GitHub](https://github.com/MaterializeInc/materialize)
- [Vector Documentation](https://vector.dev/docs/)
- [Vector GitHub](https://github.com/vectordotdev/vector)
- [PostgreSQL Binary COPY Protocol](https://www.postgresql.org/docs/current/sql-copy.html)
- [Redpanda Connect Documentation](https://docs.redpanda.com/redpanda-connect/)
