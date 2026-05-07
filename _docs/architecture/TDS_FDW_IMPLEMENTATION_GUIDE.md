# tds_fdw Implementation Guide

> Status: Practical implementation guide
> Last updated: 2026-03-28

---

## Purpose

This document is a copy-paste-oriented implementation guide for pulling MSSQL CDC data into PostgreSQL with `tds_fdw`.

It is written for the practical workflow discussed for Adopus-style ingestion:

- one source MSSQL database becomes one PostgreSQL FDW server
- multiple source tables from that same database reuse the same FDW server
- rows are pulled into one shared staging table
- an async merge procedure moves them into one shared final table
- `customer_id` is stored on the target rows so multiple source databases can land in one shared PostgreSQL schema

## Canonical Landing Principle

Use `tds_fdw` to establish the first PostgreSQL landing zone for external data, not to create identical MSSQL pull paths in every consumer database.

Recommended default:

- each logical table has one canonical owner PostgreSQL database
- Adopus MSSQL tables are pulled once into that owner database
- downstream ASMA services consume from PostgreSQL, via `asma-cdc-pipeline`, logical replication, or another PostgreSQL-native fan-out mechanism
- shared reference data can reasonably land in `directory`; domain-owned data should land in the service database that owns the domain

The examples in this guide use `directory_dev` because the current first target is shared directory-style data. That example should not be read as a rule that every consumer database should run its own FDW bootstrap or that all Adopus data belongs in `directory`.

---

## Mental Model

Use this mapping:

- `CREATE SERVER` = one remote MSSQL database connection definition inside PostgreSQL
- `CREATE USER MAPPING` = credentials for that remote MSSQL database
- `CREATE FOREIGN TABLE` = one exposed remote source table or CDC table

Rule of thumb:

- one remote source database = one FDW server
- more tables in that same source database = more foreign tables
- another source database = another FDW server

---

## Before You Start

This guide assumes:

- `tds_fdw` is already installed on the PostgreSQL host
- FreeTDS is already installed and configured on the PostgreSQL host
- you are connected to the target PostgreSQL database where the replicated data should live
- the remote MSSQL source already has CDC enabled for the source tables you want to read

This guide does not create anything in MSSQL. It creates PostgreSQL-side objects only.

---

## Adapt These Values First

Replace these example values before running the SQL:

- source database name: `AdOpusTest`
- source server name in PostgreSQL: `mssql_avansas`
- FDW schema name: `fdw_avansas`
- local target schema: `adopus`
- local final table: `Actor`
- local staging table: `stg_Actor`
- customer UUID: `11111111-1111-1111-1111-111111111111`
- MSSQL host: `10.90.37.9`
- MSSQL port: `49852`
- MSSQL username: `cdc_pipeline_admin`
- MSSQL password: `REPLACE_ME`
- remote CDC table: `cdc.dbo_Actor_CT`

---

## Recommended Workflow: Use `cdc fdw`

If you are working inside this generator-driven repository, the recommended bootstrap flow is now:

1. keep `source-groups.yaml` current with the customer databases and `customer_id` values
2. keep `services/<service>.yaml` current with the tracked source tables
3. keep `services/_schemas/<service>/<schema>/<table>.yaml` current with inspected source schemas
4. run `cdc fdw plan` to preview the derived FDW bootstrap plan
5. run `cdc fdw sql` to generate idempotent SQL
6. apply the generated SQL in the target PostgreSQL database
7. verify FDW connectivity with `SELECT ... LIMIT 1`
8. set source-group topology to `fdw` and run `cdc manage-migrations generate --service <service>` to generate the local apply layer
9. apply those migrations in PostgreSQL
10. let an external scheduler call the generated claim/pull/merge helpers

This removes the slow, error-prone part of the workflow: hand-writing one `CREATE SERVER`, `CREATE USER MAPPING`, and `CREATE FOREIGN TABLE` bundle per customer database.

### Canonical Administration Model

For this repository, the normal FDW administration path is:

- maintain source definitions in implementation YAML
- render FDW bootstrap SQL with `cdc fdw`
- render PostgreSQL materialization/runtime SQL with `cdc manage-migrations`
- version the resulting artifacts in the implementation repository

That means the long-term source of truth is the implementation inputs plus generated artifacts, not a manually curated database state.

The manual SQL sections later in this guide remain supported as a reference and fallback path, but they are not the recommended day-to-day administration model. If manual SQL is used for a lasting change, reconcile it back into the implementation files and regenerate.

### What `cdc fdw` Reads

The command derives the bootstrap state from existing implementation files:

- `source-groups.yaml`
- `services/<service>.yaml`
- `services/_schemas/<service>/<schema>/<table>.yaml`
- `.env` when server definitions use `${VAR}` placeholders

### What `cdc fdw` Generates

The command renders idempotent SQL for:

- `cdc_management.customer_registry`
- `cdc_management.environment_profile`
- `cdc_management.source_instance`
- `cdc_management.source_table_registration`
- `CREATE SCHEMA` for FDW schemas
- `CREATE SERVER` or `ALTER SERVER`
- `CREATE USER MAPPING` or `ALTER USER MAPPING`
- `CREATE FOREIGN TABLE` for tracked MSSQL CDC tables
- `CREATE FOREIGN TABLE` for tracked live MSSQL base tables
- `CREATE FOREIGN TABLE` helper objects for `sys.fn_cdc_get_min_lsn(...)`
- `CREATE FOREIGN TABLE` helper objects for `sys.fn_cdc_get_max_lsn()` per source database

### What `cdc fdw` Does Not Generate

The command does **not** replace the rest of the local apply layer by itself.

That next layer is now generated by:

```bash
cdc manage-source-groups --set-topology fdw
cdc manage-migrations generate --service <service>
```

For a one-off override without changing source-group config:

```bash
cdc manage-migrations generate --service <service> --topology fdw
```

That native runtime mode generates:

- final target tables with native CDC metadata columns
- shared staging tables
- `cdc_management.native_cdc_checkpoint`
- per-table pull functions
- per-table merge procedures
- scheduler metadata columns plus external-scheduler helper routines

So the split is now:

- `cdc fdw` handles FDW and source metadata bootstrap
- `cdc manage-migrations generate` handles PostgreSQL-side materialization and runtime helpers once topology resolves to `fdw`

Current behavior:

- topology is the user-facing selector (`redpanda`, `fdw`, `pg_native`)
- internal runtime selection is derived automatically from topology
- use `--topology fdw` only when you want a one-off override instead of changing `source-groups.yaml`

### Current Scope

Current command scope is intentionally narrow:

- MSSQL sources only
- `db-per-tenant` source groups only
- tracked tables already modeled in service YAML and schema files
- SQL generation only; the command does not auto-apply SQL to PostgreSQL

That scope is deliberate. It keeps onboarding deterministic and avoids runtime side effects from the generator CLI.

---

## Step-By-Step With `cdc fdw`

### Step 0: Ensure The Source Model Is Ready

Before running the command, make sure these are already true:

- the customer databases exist in `source-groups.yaml`
- each source entry has a `customer_id`
- the tracked source tables exist in `services/<service>.yaml`
- the source schema files already exist under `services/_schemas/<service>/...`

For Adopus, that normally means:

- `source-groups.yaml` contains entries like `Test`, `FretexDev`, `GenesisDev`
- `services/adopus.yaml` contains tracked tables like `dbo.Actor`, `dbo.Soknad`
- `services/_schemas/adopus/dbo/Actor.yaml` exists

### Step 1: Preview The Derived Bootstrap Plan

Run the plan command first.

Example for the current Adopus source-group shape:

```bash
cdc fdw plan \
    --service adopus \
    --source-env default
```

What this does:

- reads all customers from the selected `source-groups.yaml` environment
- reads all tracked source tables from the service config
- resolves server credentials from `.env` if placeholders are used
- computes deterministic FDW server names like `mssql_default_test`
- computes deterministic FDW schema names like `fdw_default_test`
- shows how many foreign tables and gap-check helper tables will be generated

Useful filters:

```bash
cdc fdw plan \
    --service adopus \
    --source-env default \
    --customer Test \
    --customer FretexDev \
    --table Actor \
    --table Soknad
```

Use `--keep-placeholders` if you want the rendered SQL to keep `${VAR}` references instead of resolving them from `.env`.

### Step 2: Render The SQL

Render the full bootstrap SQL to a file:

```bash
cdc fdw sql \
    --service adopus \
    --source-env default \
    --output generated/fdw/adopus-default-fdw.sql
```

If you want only the metadata registration tables and inserts, without any `CREATE SERVER` or `CREATE FOREIGN TABLE` statements:

```bash
cdc fdw sql \
    --service adopus \
    --source-env default \
    --metadata-only \
    --output generated/fdw/adopus-default-metadata.sql
```

Important flags:

- `--service`: service name from `services/<service>.yaml`
- `--source-env`: source environment key from `source-groups.yaml` such as `default` or `prod`
- `--customer`: limit generation to specific customer source entries
- `--table`: limit generation to specific tracked tables
- `--target-schema`: override the target schema name stored in `source_table_registration`
- `--runner-role`: PostgreSQL role used for `CREATE USER MAPPING`, default `cdc_runner`
- `--fdw-server-prefix`: default `mssql`
- `--fdw-schema-prefix`: default `fdw`
- `--keep-placeholders`: do not resolve `${VAR}` placeholders from `.env`

### Step 3: Review The Generated SQL

Before applying, verify these parts in the output:

- `INSERT INTO "cdc_management"."customer_registry"`
- `INSERT INTO "cdc_management"."source_instance"`
- `INSERT INTO "cdc_management"."source_table_registration"`
- one FDW schema per source database
- one FDW server per source database
- one foreign table per tracked CDC table per source database
- one base-table foreign table per tracked table per source database
- one gap-detection helper foreign table per tracked CDC table per source database
- one `cdc_max_lsn` helper per source database

At this stage you are checking naming, scope, and credentials resolution, not only syntax.

### Step 4: Apply The Generated SQL

Apply it in the target PostgreSQL database using your normal deployment path.

Example with `psql`:

```bash
psql "$PG_DSN" -f generated/fdw/adopus-default-fdw.sql
```

If your deployment layer templates secrets or hostnames later, generate with `--keep-placeholders` and substitute them at deployment time instead of on the developer machine.

### Step 5: Verify FDW Connectivity Immediately

Do not continue to pull/merge logic until the FDW reads succeed.

First inspect the generated objects:

```sql
SELECT
        s.srvname,
        w.fdwname,
        s.srvoptions
FROM pg_foreign_server s
JOIN pg_foreign_data_wrapper w
        ON w.oid = s.srvfdw
ORDER BY s.srvname;
```

Then read one generated foreign table:

```sql
SELECT *
FROM "fdw_default_test"."Actor_CT"
LIMIT 10;
```

Then read the corresponding live base-table foreign table:

```sql
SELECT *
FROM "fdw_default_test"."Actor_base"
LIMIT 10;
```

Then check the retention-horizon helper:

```sql
SELECT *
FROM "fdw_default_test"."cdc_min_lsn_Actor";
```

Then check the current capture-boundary helper:

```sql
SELECT *
FROM "fdw_default_test"."cdc_max_lsn";
```

If these fail, fix FDW connectivity first. Do not debug pull functions before this is green.

### Step 6: Generate The Local Native Runtime Layer

Once the generated FDW objects work, generate the PostgreSQL-side apply layer:

```bash
cdc manage-migrations generate \
    --service adopus \
    --topology fdw
```

If `source-groups.yaml` already has `topology: fdw`, the `--topology fdw` override is optional.

Useful variants:

```bash
cdc manage-migrations generate \
    --service adopus \
    --topology fdw \
    --table Actor

cdc manage-migrations generate \
    --service adopus \
    --topology fdw \
    --dry-run
```

This mode writes, per sink target:

- `00-infrastructure/03-native-cdc-runtime.sql`
- `01-tables/<Table>.sql`
- `01-tables/<Table>-staging.sql`

The generated SQL adds:

- shared final tables with native metadata columns like `__source_start_lsn`
- shared staging tables with `batch_id` and `source_instance_key`
- `cdc_management.native_cdc_checkpoint`
- `cdc_management.claim_due_native_cdc_work(...)`
- `cdc_management.mark_native_cdc_success(...)`
- `cdc_management.mark_native_cdc_failure(...)`
- `cdc_management.bootstrap_native_cdc_tables(source_instance_key, table_names, enable_after)`
- per-table `pull_<table>_batch(...)` functions
- per-table `bootstrap_<table>_snapshot(source_instance_key, enable_after)` functions
- per-table `sp_merge_<table>(uuid)` procedures

### Step 7: Apply The Native Runtime Migrations

Apply the generated runtime files using the normal migration path:

```bash
cdc manage-migrations apply --env dev --sink sink_asma.directory
```

At this point the database has both:

- FDW/bootstrap objects from `cdc fdw sql`
- local pull/apply objects from `cdc manage-migrations generate --topology fdw`

For newly tracked tables, do not hand the table to steady-state scheduling before the generated bootstrap function succeeds.

Canonical bootstrap contract:

- keep `source_table_registration.enabled = false` before bootstrap
- keep `native_cdc_schedule_policy.enabled = false` before bootstrap
- call the common wrapper function to bootstrap all pending tables or an explicit table subset
- let the function capture the current max LSN, load the live base table, seed `native_cdc_checkpoint`, reset runtime state, and optionally enable the table at the final commit point

Recommended mutation-friendly entry point:

```sql
SELECT *
FROM "cdc_management"."bootstrap_native_cdc_tables"(
    '<source_instance_key>',
    NULL,
    true
);
```

Behavior:

- `p_table_names = NULL` bootstraps all tables for the source instance whose bootstrap state is still `pending`
- `p_table_names = ARRAY['Actor', 'Soknad']` limits execution to those logical table names and will retry tables currently in `failed` state
- the function returns one row per table with statuses like `bootstrapped`, `skipped_already_initialized`, `skipped_active`, `skipped_in_progress`, or `failed`

Example:

```sql
SELECT *
FROM "cdc_management"."bootstrap_native_cdc_tables"(
    '<source_instance_key>',
    ARRAY['Actor'],
    true
);
```

The wrapper dispatches to the generated per-table `bootstrap_<table>_snapshot(...)` functions internally. With `p_enable_after = true`, the function enables both registration and schedule policy only after the snapshot load and checkpoint seed commit successfully. If you want a dry operational pause after loading, call it with `false` and enable the rows separately later.

For admin UI queries, customer/table status screens, and representative Hasura calls, see [_docs/cdc-orchestrator/ADMIN_INTERFACE_GUIDE.md](../../_docs/cdc-orchestrator/ADMIN_INTERFACE_GUIDE.md).

### Step 8: Hand Off To The External Scheduler

The generator does **not** register `pg_cron` jobs or any other scheduler.

The external scheduler should:

1. call `cdc_management.claim_due_native_cdc_work(...)`
2. for each claimed row, call `<target_schema>.pull_<table>_batch(source_instance_key)`
3. if rows were pulled, call `<target_schema>.sp_merge_<table>(batch_id)`
4. on success, call `cdc_management.mark_native_cdc_success(...)`
5. on failure, call `cdc_management.mark_native_cdc_failure(...)`

This keeps scheduling deployment-specific while still making the runtime behavior metadata-driven and generator-owned. For a newly tracked table, this handoff starts only after `bootstrap_<table>_snapshot(...)` has completed successfully.

### Step 9: Use The Remaining Sections As SQL Reference

The next sections remain valuable as the low-level SQL reference path and for understanding the generated design, but the generator workflow above is now the recommended path for this repository.

Use the remaining sections when you need to debug, bootstrap around a temporary tooling gap, or run a one-off smoke test. Do not treat the manual path as the primary versioning or administration surface.

---

## Step 1: Enable Required PostgreSQL Extensions

The next sections are still the **manual reference path**. If you use `cdc fdw sql`, most of the FDW-object bootstrap from this part is already rendered for you.

```sql
CREATE EXTENSION IF NOT EXISTS tds_fdw;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
```

`pgcrypto` is used here only for `gen_random_uuid()` in the example pull function.

---

## Step 2: Create Local Schemas

Use:

- one schema for FDW objects
- one schema for target business tables
- one schema for checkpoints and control tables

```sql
CREATE SCHEMA IF NOT EXISTS "fdw_avansas";
CREATE SCHEMA IF NOT EXISTS "adopus";
CREATE SCHEMA IF NOT EXISTS "cdc_management";
```

---

## Step 3: Create One FDW Server For One Source Database

This registers one remote MSSQL database inside PostgreSQL.

```sql
CREATE SERVER "mssql_avansas"
FOREIGN DATA WRAPPER tds_fdw
OPTIONS (
    servername '10.90.37.9', -- For HA failover, use a comma-separated list e.g., '10.90.37.9,10.90.37.10'
    port '49852',
    database 'AdOpusTest',
    tds_version '7.4',
    dbuse '0',
    msg_handler 'notice'
);
```

Important:

- this is one server per source database, not per source table
- if `Actor`, `Bruker`, and `AdgangLinjer` all live in `AdOpusTest`, they all use this same server

---

## Step 4: Create User Mapping

This attaches MSSQL credentials to the FDW server.

```sql
CREATE USER MAPPING FOR "cdc_runner"
SERVER "mssql_avansas"
OPTIONS (
    username 'cdc_pipeline_admin',
    password 'REPLACE_ME'
);
```

> **Security Note:** `CREATE USER MAPPING` stores passwords in plaintext inside system catalogs. Never use the `postgres` superuser for this in production. Always create a dedicated, unprivileged role (e.g., `cdc_runner`) and map *only* that user, restricting broad access to `pg_user_mappings`.

---

## Step 5: Create One Foreign Table Per Source CDC Table

This exposes the remote MSSQL CDC table inside PostgreSQL.

```sql
CREATE FOREIGN TABLE "fdw_avansas"."Actor_CT" (
    "__$start_lsn" bytea,
    "__$seqval" bytea,
    "__$operation" integer,
    "__$update_mask" bytea,
    "actno" integer,
    "Navn" text,
    "epost" text,
    "changedt" timestamp without time zone
)
SERVER "mssql_avansas"
OPTIONS (
    schema_name 'cdc',
    table_name 'dbo_Actor_CT',
    match_column_names 'true',
    row_estimate_method 'showplan_all'
);
```

Sanity check:

```sql
SELECT
    "__$operation",
    "actno",
    "Navn",
    "epost"
FROM "fdw_avansas"."Actor_CT"
LIMIT 20;
```

If this query fails, fix the FDW connection or remote object mapping before moving forward.

---

## Step 5b: Create Gap Detection Foreign Table

To prevent silent data loss, PostgreSQL must cross-check MSSQL's CDC retention horizon before pulling.

```sql
CREATE FOREIGN TABLE "fdw_avansas"."cdc_min_lsn_Actor" (
    "min_lsn" bytea
)
SERVER "mssql_avansas"
OPTIONS (
    query 'SELECT sys.fn_cdc_get_min_lsn(''dbo_Actor'') AS min_lsn',
    row_estimate_method 'execute'
);
```

---

## Step 6: Create The Shared Final Table

This example uses one shared final `Actor` table in the `adopus` schema.

Use `(customer_id, actno)` as the primary key so rows from different customer databases do not collide.

```sql
CREATE TABLE IF NOT EXISTS "adopus"."Actor" (
    "customer_id" uuid NOT NULL,
    "actno" integer NOT NULL,
    "Navn" text,
    "epost" text,
    "__sync_timestamp" timestamptz NOT NULL DEFAULT now(),
    "__source" text NOT NULL DEFAULT 'mssql_cdc',
    "__source_db" text NOT NULL,
    "__source_table" text NOT NULL DEFAULT 'dbo.Actor',
    "__source_start_lsn" bytea,
    "__source_seqval" bytea,
    "__cdc_operation" integer,
    PRIMARY KEY ("customer_id", "actno")
) WITH (fillfactor = 85); -- Critical for CDC write amplification
```

Recommended indexes:

```sql
CREATE INDEX IF NOT EXISTS "idx_Actor_source_db"
ON "adopus"."Actor" ("__source_db");

CREATE INDEX IF NOT EXISTS "idx_Actor_sync_timestamp"
ON "adopus"."Actor" ("__sync_timestamp");
```

---

## Step 7: Create The Shared Staging Table

Use one shared staging table for the shared final table.

Do not create one staging table per customer unless you intentionally want tenant-level operational isolation.

```sql
CREATE UNLOGGED TABLE IF NOT EXISTS "adopus"."stg_Actor" (
    "batch_id" uuid NOT NULL,
    "customer_id" uuid NOT NULL,
    "actno" integer NOT NULL,
    "Navn" text,
    "epost" text,
    "__start_lsn" bytea NOT NULL,
    "__seqval" bytea,
    "__operation" integer NOT NULL,
    "__source_db" text NOT NULL,
    "__source_table" text NOT NULL DEFAULT 'dbo.Actor',
    "__pulled_at" timestamptz NOT NULL DEFAULT now()
) WITH (
    autovacuum_enabled = true,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_cost_delay = 2,
    autovacuum_analyze_scale_factor = 0.05
); -- Critical to prevent massive UNLOGGED dead-tuple bloat
```

Recommended indexes:

```sql
CREATE INDEX IF NOT EXISTS "idx_stg_Actor_customer_actno"
ON "adopus"."stg_Actor" ("customer_id", "actno");

CREATE INDEX IF NOT EXISTS "idx_stg_Actor_lsn_seqval"
ON "adopus"."stg_Actor" ("__start_lsn", "__seqval");

CREATE INDEX IF NOT EXISTS "idx_stg_Actor_batch_id"
ON "adopus"."stg_Actor" ("batch_id");
```

---

## Step 8: Register The Source Database

This registry table lets you add more customer source databases later without rewriting the pull logic.

```sql
CREATE TABLE IF NOT EXISTS "cdc_management"."actor_sources" (
    "customer_id" uuid PRIMARY KEY,
    "customer_name" text NOT NULL UNIQUE,
    "fdw_schema" text NOT NULL,
    "fdw_table" text NOT NULL,
    "source_db" text NOT NULL,
    "enabled" boolean NOT NULL DEFAULT true
);
```

Insert the first source registration:

```sql
INSERT INTO "cdc_management"."actor_sources" (
    "customer_id",
    "customer_name",
    "fdw_schema",
    "fdw_table",
    "source_db",
    "enabled"
)
VALUES (
    '11111111-1111-1111-1111-111111111111',
    'avansas',
    'fdw_avansas',
    'Actor_CT',
    'AdOpusTest',
    true
)
ON CONFLICT ("customer_id") DO UPDATE
SET
    "customer_name" = EXCLUDED."customer_name",
    "fdw_schema" = EXCLUDED."fdw_schema",
    "fdw_table" = EXCLUDED."fdw_table",
    "source_db" = EXCLUDED."source_db",
    "enabled" = EXCLUDED."enabled";
```

---

## Step 9: Create Checkpoint Table

Checkpoint per customer plus table.

```sql
CREATE TABLE IF NOT EXISTS "cdc_management"."native_cdc_checkpoint" (
    "customer_id" uuid NOT NULL,
    "table_name" text NOT NULL,
    "last_start_lsn" bytea NOT NULL,
    "last_seqval" bytea,
    "updated_at" timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("customer_id", "table_name")
);
```

Seed the first checkpoint:

```sql
INSERT INTO "cdc_management"."native_cdc_checkpoint" (
    "customer_id",
    "table_name",
    "last_start_lsn",
    "last_seqval"
)
VALUES (
    '11111111-1111-1111-1111-111111111111',
    'Actor',
    decode('00000000000000000000', 'hex'),
    NULL
)
ON CONFLICT ("customer_id", "table_name") DO NOTHING;
```

---

## Step 10: Create Pull Function

This function:

- reads new rows from the remote CDC foreign table
- injects the correct `customer_id`
- inserts rows into the shared staging table
- returns the generated batch id and number of inserted rows

```sql
CREATE OR REPLACE FUNCTION "adopus"."pull_actor_customer"(
    p_customer_id uuid
)
RETURNS TABLE (
    "batch_id" uuid,
    "rows_inserted" bigint
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_fdw_schema text;
    v_fdw_table text;
    v_source_db text;
    v_last_start_lsn bytea;
    v_last_seqval bytea;
    v_min_valid_lsn bytea;
    v_batch_id uuid := gen_random_uuid();
    v_sql text;
BEGIN
    -- Prevent infinite hang on network drop
    SET LOCAL statement_timeout = '2min';

    SELECT
        s."fdw_schema",
        s."fdw_table",
        s."source_db"
    INTO
        v_fdw_schema,
        v_fdw_table,
        v_source_db
    FROM "cdc_management"."actor_sources" s
    WHERE s."customer_id" = p_customer_id
      AND s."enabled" = true;

    IF v_fdw_schema IS NULL THEN
        RAISE EXCEPTION 'No enabled actor source found for customer_id %', p_customer_id;
    END IF;

    SELECT
        c."last_start_lsn",
        c."last_seqval"
    INTO
        v_last_start_lsn,
        v_last_seqval
    FROM "cdc_management"."native_cdc_checkpoint" c
    WHERE c."customer_id" = p_customer_id
      AND c."table_name" = 'Actor';

    IF v_last_start_lsn IS NULL THEN
        RAISE EXCEPTION 'No checkpoint found for customer_id % and table Actor', p_customer_id;
    END IF;

    -- 1. Gap Detection: Ensure our checkpoint is not older than MSSQL's cleanup horizon
    EXECUTE format('SELECT "min_lsn" FROM %I."cdc_min_lsn_Actor"', v_fdw_schema)
    INTO v_min_valid_lsn;

    IF v_last_start_lsn < v_min_valid_lsn THEN
        RAISE EXCEPTION 'FATAL: LSN Gap detected for Actor. Last pulled %s is older than MSSQL retention limit %s', v_last_start_lsn, v_min_valid_lsn;
    END IF;

    -- 2. Pull with Bounded Limits (to prevent OOM on massive updates)
    v_sql := format(
        $fmt$
        INSERT INTO "adopus"."stg_Actor" (
            "batch_id",
            "customer_id",
            "actno",
            "Navn",
            "epost",
            "__start_lsn",
            "__seqval",
            "__operation",
            "__source_db",
            "__source_table"
        )
        SELECT
            %L::uuid,
            %L::uuid,
            f."actno",
            f."Navn",
            f."epost",
            f."__$start_lsn",
            f."__$seqval",
            f."__$operation",
            %L,
            'dbo.Actor'
        FROM %I.%I f
        WHERE
            (
                f."__$start_lsn" > %L::bytea
                OR (
                    f."__$start_lsn" = %L::bytea
                    AND (
                        %L::bytea IS NULL
                        OR f."__$seqval" > %L::bytea
                    )
                )
            )
        ORDER BY f."__$start_lsn", f."__$seqval"
        LIMIT 50000 -- Safe transactional cap for memory and WAL
        $fmt$,
        v_batch_id,
        p_customer_id,
        v_source_db,
        v_fdw_schema,
        v_fdw_table,
        v_last_start_lsn,
        v_last_start_lsn,
        v_last_seqval,
        v_last_seqval
    );

    EXECUTE v_sql;

    RETURN QUERY
    SELECT
        v_batch_id,
        count(*)
    FROM "adopus"."stg_Actor"
    WHERE "batch_id" = v_batch_id;
END;
$$;
```

---

## Step 11: Create Merge Procedure

This procedure:

- upserts operations `2` and `4`
- deletes operation `1`
- advances the checkpoint to the latest merged LSN
- clears the merged staging rows

```sql
CREATE OR REPLACE PROCEDURE "adopus"."sp_merge_actor"(
    p_batch_id uuid
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_customer_id uuid;
    v_last_start_lsn bytea;
    v_last_seqval bytea;
BEGIN
    SELECT DISTINCT s."customer_id"
    INTO v_customer_id
    FROM "adopus"."stg_Actor" s
    WHERE s."batch_id" = p_batch_id;

    IF v_customer_id IS NULL THEN
        RAISE NOTICE 'No staging rows found for batch_id %', p_batch_id;
        RETURN;
    END IF;

    INSERT INTO "adopus"."Actor" (
        "customer_id",
        "actno",
        "Navn",
        "epost",
        "__sync_timestamp",
        "__source",
        "__source_db",
        "__source_table",
        "__source_start_lsn",
        "__source_seqval",
        "__cdc_operation"
    )
    SELECT
        s."customer_id",
        s."actno",
        s."Navn",
        s."epost",
        now(),
        'mssql_cdc',
        s."__source_db",
        s."__source_table",
        s."__start_lsn",
        s."__seqval",
        s."__operation"
    FROM "adopus"."stg_Actor" s
    WHERE s."batch_id" = p_batch_id
      AND s."__operation" IN (2, 4)
    ON CONFLICT ("customer_id", "actno")
    DO UPDATE SET
        "Navn" = EXCLUDED."Navn",
        "epost" = EXCLUDED."epost",
        "__sync_timestamp" = EXCLUDED."__sync_timestamp",
        "__source" = EXCLUDED."__source",
        "__source_db" = EXCLUDED."__source_db",
        "__source_table" = EXCLUDED."__source_table",
        "__source_start_lsn" = EXCLUDED."__source_start_lsn",
        "__source_seqval" = EXCLUDED."__source_seqval",
        "__cdc_operation" = EXCLUDED."__cdc_operation";

    DELETE FROM "adopus"."Actor" a
    USING "adopus"."stg_Actor" s
    WHERE s."batch_id" = p_batch_id
      AND s."__operation" = 1
      AND a."customer_id" = s."customer_id"
      AND a."actno" = s."actno";

    SELECT
        x."__start_lsn",
        x."__seqval"
    INTO
        v_last_start_lsn,
        v_last_seqval
    FROM "adopus"."stg_Actor" x
    WHERE x."batch_id" = p_batch_id
    ORDER BY x."__start_lsn" DESC, x."__seqval" DESC
    LIMIT 1;

    UPDATE "cdc_management"."native_cdc_checkpoint"
    SET
        "last_start_lsn" = v_last_start_lsn,
        "last_seqval" = v_last_seqval,
        "updated_at" = now()
    WHERE "customer_id" = v_customer_id
      AND "table_name" = 'Actor';

    DELETE FROM "adopus"."stg_Actor"
    WHERE "batch_id" = p_batch_id;
END;
$$;
```

---

## Step 12: First Manual Run

Pull one batch:

```sql
SELECT *
FROM "adopus"."pull_actor_customer"(
    '11111111-1111-1111-1111-111111111111'::uuid
);
```

Inspect staging:

```sql
SELECT *
FROM "adopus"."stg_Actor"
WHERE "batch_id" = 'REPLACE_WITH_BATCH_ID'::uuid
ORDER BY "__start_lsn", "__seqval";
```

Run merge:

```sql
CALL "adopus"."sp_merge_actor"(
    'REPLACE_WITH_BATCH_ID'::uuid
);
```

Inspect final table:

```sql
SELECT
    "customer_id",
    "actno",
    "Navn",
    "epost",
    "__sync_timestamp",
    "__source_db",
    "__cdc_operation"
FROM "adopus"."Actor"
WHERE "customer_id" = '11111111-1111-1111-1111-111111111111'::uuid
ORDER BY "actno";
```

---

## Add More Tables From The Same Source Database

Do not create another FDW server.

Reuse:

- same FDW server: `mssql_avansas`
- same user mapping
- same FDW schema: `fdw_avansas`

Add one foreign table per additional source table.

Example:

```sql
CREATE FOREIGN TABLE "fdw_avansas"."Bruker_CT" (
    "__$start_lsn" bytea,
    "__$seqval" bytea,
    "__$operation" integer,
    "__$update_mask" bytea,
    "BrukerNavn" text,
    "Navn" text
)
SERVER "mssql_avansas"
OPTIONS (
    schema_name 'cdc',
    table_name 'dbo_Bruker_CT',
    match_column_names 'true',
    row_estimate_method 'showplan_all'
);
```

That is the normal workflow for adding more tables.

---

## Add Another Source Database

If the remote source database changes, create another FDW server.

Example:

```sql
CREATE SCHEMA IF NOT EXISTS "fdw_blue";
```

```sql
CREATE SERVER "mssql_blue"
FOREIGN DATA WRAPPER tds_fdw
OPTIONS (
    servername '10.90.37.9',
    port '49852',
    database 'AdOpusBlue',
    tds_version '7.4',
    dbuse '0',
    msg_handler 'notice'
);
```

```sql
CREATE USER MAPPING FOR "postgres"
SERVER "mssql_blue"
OPTIONS (
    username 'cdc_pipeline_admin',
    password 'REPLACE_ME'
);
```

```sql
CREATE FOREIGN TABLE "fdw_blue"."Actor_CT" (
    "__$start_lsn" bytea,
    "__$seqval" bytea,
    "__$operation" integer,
    "__$update_mask" bytea,
    "actno" integer,
    "Navn" text,
    "epost" text
)
SERVER "mssql_blue"
OPTIONS (
    schema_name 'cdc',
    table_name 'dbo_Actor_CT',
    match_column_names 'true',
    row_estimate_method 'showplan_all'
);
```

Then register that second source in `cdc_management.actor_sources` with its own `customer_id`.

---

## Inspection Queries

See all FDW servers:

```sql
SELECT
    s.srvname,
    w.fdwname,
    s.srvoptions
FROM pg_foreign_server s
JOIN pg_foreign_data_wrapper w
    ON w.oid = s.srvfdw;
```

See all user mappings:

```sql
SELECT
    s.srvname,
    r.rolname,
    um.umoptions
FROM pg_user_mappings um
JOIN pg_foreign_server s
    ON s.oid = um.srvid
JOIN pg_roles r
    ON r.oid = um.umuser;
```

See all foreign tables:

```sql
SELECT
    n.nspname AS schema_name,
    c.relname AS foreign_table_name,
    s.srvname AS server_name
FROM pg_foreign_table ft
JOIN pg_class c
    ON c.oid = ft.ftrelid
JOIN pg_namespace n
    ON n.oid = c.relnamespace
JOIN pg_foreign_server s
    ON s.oid = ft.ftserver
ORDER BY n.nspname, c.relname;
```

---

## Cleanup Commands

Drop foreign tables:

```sql
DROP FOREIGN TABLE IF EXISTS "fdw_avansas"."Actor_CT";
DROP FOREIGN TABLE IF EXISTS "fdw_avansas"."Bruker_CT";
```

Drop mapping and server:

```sql
DROP USER MAPPING IF EXISTS FOR "postgres" SERVER "mssql_avansas";
DROP SERVER IF EXISTS "mssql_avansas";
```

Drop local objects:

```sql
DROP PROCEDURE IF EXISTS "adopus"."sp_merge_actor"(uuid);
DROP FUNCTION IF EXISTS "adopus"."pull_actor_customer"(uuid);
DROP TABLE IF EXISTS "adopus"."stg_Actor";
DROP TABLE IF EXISTS "adopus"."Actor";
```

---

## Making It Generic Across Environments

If you have:

- one nonprod MSSQL server
- one prod MSSQL server
- one nonprod PostgreSQL target
- one prod PostgreSQL target
- mostly the same customers in all environments

do not copy-paste one new SQL bundle per customer.

Instead, make the implementation metadata-driven.

### Recommended Principle

Write the PostgreSQL logic once, then drive it from control tables.

Use:

- one global customer registry
- one environment profile table
- one source-instance registry table
- one source-table registration table
- one generic object-bootstrap procedure
- one generic pull procedure
- one generic merge procedure per target table or one table-driven merge framework

That way, adding a new customer or enabling another environment becomes an `INSERT` into metadata tables, not a new handwritten SQL script.

### What Should Stay Static

Write once:

- final target tables like `adopus."Actor"`
- shared staging tables like `adopus."stg_Actor"`
- checkpoint tables
- merge procedures
- orchestration functions

### What Should Become Metadata

Store as data:

- environment name: `nonprod`, `prod`
- MSSQL host and port
- MSSQL credentials or the credential reference you want to use
- source database name per customer per environment
- customer UUID
- FDW schema name
- FDW server name
- source table registration

---

## Recommended Metadata Tables

### 1. Customer Registry

Store each business customer once.

```sql
CREATE TABLE IF NOT EXISTS "cdc_management"."customer_registry" (
    "customer_key" text PRIMARY KEY,
    "customer_id" uuid NOT NULL UNIQUE,
    "customer_name" text NOT NULL
);
```

Example:

```sql
INSERT INTO "cdc_management"."customer_registry" (
    "customer_key",
    "customer_id",
    "customer_name"
)
VALUES
    (
        'avansas',
        '4d43855c-afa9-45ca-9e31-382dbde9681b',
        'Avansas'
    )
ON CONFLICT ("customer_key") DO UPDATE
SET
    "customer_id" = EXCLUDED."customer_id",
    "customer_name" = EXCLUDED."customer_name";
```

### 2. Environment Profiles

Store connection-level configuration per environment.

```sql
CREATE TABLE IF NOT EXISTS "cdc_management"."environment_profile" (
    "environment_name" text PRIMARY KEY,
    "mssql_host" text NOT NULL,
    "mssql_port" integer NOT NULL,
    "mssql_username" text NOT NULL,
    "mssql_password" text NOT NULL,
    "tds_version" text NOT NULL DEFAULT '7.4',
    "enabled" boolean NOT NULL DEFAULT true
);
```

Example:

```sql
INSERT INTO "cdc_management"."environment_profile" (
    "environment_name",
    "mssql_host",
    "mssql_port",
    "mssql_username",
    "mssql_password",
    "tds_version",
    "enabled"
)
VALUES
    (
        'nonprod',
        '10.90.37.9',
        49852,
        'cdc_pipeline_admin',
        'REPLACE_ME_NONPROD',
        '7.4',
        true
    ),
    (
        'prod',
        'REPLACE_ME_PROD_HOST',
        1433,
        'REPLACE_ME_PROD_USER',
        'REPLACE_ME_PROD_PASSWORD',
        '7.4',
        true
    )
ON CONFLICT ("environment_name") DO UPDATE
SET
    "mssql_host" = EXCLUDED."mssql_host",
    "mssql_port" = EXCLUDED."mssql_port",
    "mssql_username" = EXCLUDED."mssql_username",
    "mssql_password" = EXCLUDED."mssql_password",
    "tds_version" = EXCLUDED."tds_version",
    "enabled" = EXCLUDED."enabled";
```

### 3. Source Instances

Store one row per customer plus environment plus source database.

```sql
CREATE TABLE IF NOT EXISTS "cdc_management"."source_instance" (
    "source_instance_key" text PRIMARY KEY,
    "environment_name" text NOT NULL REFERENCES "cdc_management"."environment_profile"("environment_name"),
    "customer_key" text NOT NULL REFERENCES "cdc_management"."customer_registry"("customer_key"),
    "source_database" text NOT NULL,
    "fdw_server_name" text NOT NULL UNIQUE,
    "fdw_schema_name" text NOT NULL UNIQUE,
    "enabled" boolean NOT NULL DEFAULT true
);
```

Example:

```sql
INSERT INTO "cdc_management"."source_instance" (
    "source_instance_key",
    "environment_name",
    "customer_key",
    "source_database",
    "fdw_server_name",
    "fdw_schema_name",
    "enabled"
)
VALUES
    (
        'nonprod_avansas',
        'nonprod',
        'avansas',
        'AdOpusTest',
        'mssql_nonprod_avansas',
        'fdw_nonprod_avansas',
        true
    ),
    (
        'prod_avansas',
        'prod',
        'avansas',
        'AdOpusAVProd',
        'mssql_prod_avansas',
        'fdw_prod_avansas',
        true
    )
ON CONFLICT ("source_instance_key") DO UPDATE
SET
    "environment_name" = EXCLUDED."environment_name",
    "customer_key" = EXCLUDED."customer_key",
    "source_database" = EXCLUDED."source_database",
    "fdw_server_name" = EXCLUDED."fdw_server_name",
    "fdw_schema_name" = EXCLUDED."fdw_schema_name",
    "enabled" = EXCLUDED."enabled";
```

### 4. Source Table Registration

Store which tables should be created and pulled for each source instance.

```sql
CREATE TABLE IF NOT EXISTS "cdc_management"."source_table_registration" (
    "source_instance_key" text NOT NULL REFERENCES "cdc_management"."source_instance"("source_instance_key"),
    "logical_table_name" text NOT NULL,
    "remote_schema_name" text NOT NULL,
    "remote_table_name" text NOT NULL,
    "target_schema_name" text NOT NULL,
    "target_table_name" text NOT NULL,
    "enabled" boolean NOT NULL DEFAULT true,
    PRIMARY KEY ("source_instance_key", "logical_table_name")
);
```

Example:

```sql
INSERT INTO "cdc_management"."source_table_registration" (
    "source_instance_key",
    "logical_table_name",
    "remote_schema_name",
    "remote_table_name",
    "target_schema_name",
    "target_table_name",
    "enabled"
)
VALUES
    (
        'nonprod_avansas',
        'Actor',
        'cdc',
        'dbo_Actor_CT',
        'adopus',
        'Actor',
        true
    ),
    (
        'prod_avansas',
        'Actor',
        'cdc',
        'dbo_Actor_CT',
        'adopus',
        'Actor',
        true
    )
ON CONFLICT ("source_instance_key", "logical_table_name") DO UPDATE
SET
    "remote_schema_name" = EXCLUDED."remote_schema_name",
    "remote_table_name" = EXCLUDED."remote_table_name",
    "target_schema_name" = EXCLUDED."target_schema_name",
    "target_table_name" = EXCLUDED."target_table_name",
    "enabled" = EXCLUDED."enabled";
```

---

## How To Use Environment Variables Without Hardcoding Everything

Environment variables are useful at deployment time, but they should not be your main runtime control plane inside PostgreSQL.

Recommended approach:

1. keep secrets and hostnames in environment variables or secret storage outside PostgreSQL
2. load or refresh the metadata tables from a bootstrap script
3. let PostgreSQL runtime logic read from metadata tables, not from duplicated handwritten SQL blocks

Good pattern:

- env vars or secret manager -> bootstrap SQL -> metadata tables -> generic runtime functions

Avoid this pattern:

- env vars -> many customer-specific handwritten SQL files -> manual drift

If you use `psql`, you can template inserts like this:

```sql
INSERT INTO "cdc_management"."environment_profile" (
    "environment_name",
    "mssql_host",
    "mssql_port",
    "mssql_username",
    "mssql_password"
)
VALUES (
    :'env_name',
    :'mssql_host',
    :'mssql_port',
    :'mssql_user',
    :'mssql_password'
)
ON CONFLICT ("environment_name") DO UPDATE
SET
    "mssql_host" = EXCLUDED."mssql_host",
    "mssql_port" = EXCLUDED."mssql_port",
    "mssql_username" = EXCLUDED."mssql_username",
    "mssql_password" = EXCLUDED."mssql_password";
```

Then run `psql -v env_name=nonprod -v mssql_host=...` from your deployment layer.

---

## Recommended Generic Runtime Flow

Once the metadata exists, the runtime should become generic.

### Bootstrap Phase

Run once or whenever metadata changes:

1. read enabled rows from `environment_profile`
2. read enabled rows from `source_instance`
3. create missing FDW schemas
4. create or refresh `CREATE SERVER` objects
5. create or refresh `CREATE USER MAPPING` objects
6. create or refresh foreign tables from `source_table_registration`

This should be done by one bootstrap procedure or one generated SQL script, not manually per customer.

For the generator-driven SQL path, `cdc fdw sql` is now the canonical generated-script option.

### Pull Phase

Run per batch:

1. choose environment, or all enabled environments
2. choose source instance, or all enabled source instances
3. find the customer UUID through `customer_registry`
4. find the remote table through `source_table_registration`
5. pull rows from the right FDW table
6. insert into shared `stg_Actor`
7. merge into shared `Actor`
8. update checkpoint for that specific source instance plus logical table

### Naming Convention

Use deterministic names so the bootstrap process can generate them consistently:

- FDW server: `mssql_<env>_<customer>`
- FDW schema: `fdw_<env>_<customer>`
- foreign table: `<logical_table_name>_CT`
- source instance key: `<env>_<customer>`

That gives you the same scaffold for every environment and customer without rewriting the logic.

---

## What You Should Not Duplicate

Do not duplicate per customer:

- pull function code
- merge procedure code
- checkpoint table design
- target table DDL
- staging table DDL

Only metadata rows should vary.

---

## Practical Outcome

With this model, onboarding a new customer becomes:

1. insert one row into `customer_registry` if the customer is new
2. insert one row into `source_instance` for each enabled environment
3. insert rows into `source_table_registration` for the tables you want
4. run the bootstrap procedure or generated SQL once
5. start generic pull and merge jobs

No new handwritten per-customer FDW script should be needed.

---

## Worked Example With `cdc fdw`

This is the practical command-driven path for the case discussed:

- service: `adopus`
- source environment key in `source-groups.yaml`: `default`
- customer source entries: `Test`, `FretexDev`
- tracked tables: `Actor` first, then `Soknad`

### Command 1: Preview The Plan

```bash
cdc fdw plan \
    --service adopus \
    --source-env default \
    --customer Test \
    --customer FretexDev \
    --table Actor
```

Expected practical outcome:

- source instance `default_test` for database `AdOpusTest`
- source instance `default_fretexdev` for database `AdOpusFretexDev`
- FDW server `mssql_default_test`
- FDW schema `fdw_default_test`
- FDW server `mssql_default_fretexdev`
- FDW schema `fdw_default_fretexdev`
- tracked foreign table `Actor_CT` for both source databases

### Command 2: Generate The SQL For `Actor`

```bash
cdc fdw sql \
    --service adopus \
    --source-env default \
    --customer Test \
    --customer FretexDev \
    --table Actor \
    --output generated/fdw/adopus-default-actor.sql
```

Apply it:

```bash
psql "$PG_DSN" -f generated/fdw/adopus-default-actor.sql
```

### Command 3: Later Add `Soknad`

Once `dbo.Soknad` is already tracked in `services/adopus.yaml` and the schema file exists, rerun with the extra table:

```bash
cdc fdw sql \
    --service adopus \
    --source-env default \
    --customer Test \
    --customer FretexDev \
    --table Actor \
    --table Soknad \
    --output generated/fdw/adopus-default-actor-soknad.sql
```

This extends the generated SQL to include:

- `fdw_default_test."Soknad_CT"`
- `fdw_default_test."cdc_min_lsn_Soknad"`
- `fdw_default_fretexdev."Soknad_CT"`
- `fdw_default_fretexdev."cdc_min_lsn_Soknad"`
- matching metadata rows in `source_table_registration`

### Why This Is Better In Practice

Compared with the handwritten path, the command-driven flow keeps these consistent automatically:

- FDW object naming
- metadata inserts
- customer-to-database mapping
- table-to-capture-instance mapping
- type mapping from MSSQL schema YAML into PostgreSQL FDW column definitions

That is the main operational gain. You still review SQL, but you stop re-authoring the same customer-specific object boilerplate.

---

## Worked Example: Two Nonprod Customers, One Registered Table (Manual SQL Reference)

This is the practical example for the case discussed:

- remote MSSQL environment: `nonprod`
- local PostgreSQL environment: the nonprod/dev PostgreSQL database where this runtime lives
- customer 1 source database: `AdOpusTest`
- customer 2 source database: `AdOpusFretexDev`
- first registered table: `Actor`
- later registered table: `Soknad`

Important:

- you run the following SQL once in the target PostgreSQL database
- you do not write one custom script per customer
- you add metadata rows, then rerun the bootstrap step

### Real Customer IDs From Current Adopus Source Config

Based on the current Adopus `source-groups.yaml`:

- `AdOpusTest` (`Test`) -> `4d43855c-afa9-45ca-9e31-382dbde9681b`
- `AdOpusFretexDev` (`FretexDev`) -> `04ed3971-ea9a-49e0-a0ba-5170c16a8d64`

### Step A: Register Customers Once

```sql
INSERT INTO "cdc_management"."customer_registry" (
    "customer_key",
    "customer_id",
    "customer_name"
)
VALUES
    (
        'test',
        '4d43855c-afa9-45ca-9e31-382dbde9681b',
        'AdOpusTest'
    ),
    (
        'fretexdev',
        '04ed3971-ea9a-49e0-a0ba-5170c16a8d64',
        'AdOpusFretexDev'
    )
ON CONFLICT ("customer_key") DO UPDATE
SET
    "customer_id" = EXCLUDED."customer_id",
    "customer_name" = EXCLUDED."customer_name";
```

### Step B: Register The Nonprod Source Environment Once

```sql
INSERT INTO "cdc_management"."environment_profile" (
    "environment_name",
    "mssql_host",
    "mssql_port",
    "mssql_username",
    "mssql_password",
    "tds_version",
    "enabled"
)
VALUES (
    'nonprod',
    '10.90.37.9',
    49852,
    'cdc_pipeline_admin',
    'REPLACE_ME_NONPROD_PASSWORD',
    '7.4',
    true
)
ON CONFLICT ("environment_name") DO UPDATE
SET
    "mssql_host" = EXCLUDED."mssql_host",
    "mssql_port" = EXCLUDED."mssql_port",
    "mssql_username" = EXCLUDED."mssql_username",
    "mssql_password" = EXCLUDED."mssql_password",
    "tds_version" = EXCLUDED."tds_version",
    "enabled" = EXCLUDED."enabled";
```

### Step C: Register The Two Source Databases

This creates two source instances in metadata, but still no handwritten per-table FDW SQL.

```sql
INSERT INTO "cdc_management"."source_instance" (
    "source_instance_key",
    "environment_name",
    "customer_key",
    "source_database",
    "fdw_server_name",
    "fdw_schema_name",
    "enabled"
)
VALUES
    (
        'nonprod_test',
        'nonprod',
        'test',
        'AdOpusTest',
        'mssql_nonprod_test',
        'fdw_nonprod_test',
        true
    ),
    (
        'nonprod_fretexdev',
        'nonprod',
        'fretexdev',
        'AdOpusFretexDev',
        'mssql_nonprod_fretexdev',
        'fdw_nonprod_fretexdev',
        true
    )
ON CONFLICT ("source_instance_key") DO UPDATE
SET
    "environment_name" = EXCLUDED."environment_name",
    "customer_key" = EXCLUDED."customer_key",
    "source_database" = EXCLUDED."source_database",
    "fdw_server_name" = EXCLUDED."fdw_server_name",
    "fdw_schema_name" = EXCLUDED."fdw_schema_name",
    "enabled" = EXCLUDED."enabled";
```

### Step D: Register `Actor` For Both Customers

This is the important runtime registration step. You are not creating a new pull function or a new merge procedure. You are only adding metadata rows that say: these two source instances should expose the `Actor` CDC table.

```sql
INSERT INTO "cdc_management"."source_table_registration" (
    "source_instance_key",
    "logical_table_name",
    "remote_schema_name",
    "remote_table_name",
    "target_schema_name",
    "target_table_name",
    "enabled"
)
VALUES
    (
        'nonprod_test',
        'Actor',
        'cdc',
        'dbo_Actor_CT',
        'adopus',
        'Actor',
        true
    ),
    (
        'nonprod_fretexdev',
        'Actor',
        'cdc',
        'dbo_Actor_CT',
        'adopus',
        'Actor',
        true
    )
ON CONFLICT ("source_instance_key", "logical_table_name") DO UPDATE
SET
    "remote_schema_name" = EXCLUDED."remote_schema_name",
    "remote_table_name" = EXCLUDED."remote_table_name",
    "target_schema_name" = EXCLUDED."target_schema_name",
    "target_table_name" = EXCLUDED."target_table_name",
    "enabled" = EXCLUDED."enabled";
```

### What Happens After Step D

After those inserts, your bootstrap step should create or refresh:

- FDW schema `fdw_nonprod_test`
- FDW server `mssql_nonprod_test`
- foreign table `fdw_nonprod_test."Actor_CT"`
- FDW schema `fdw_nonprod_fretexdev`
- FDW server `mssql_nonprod_fretexdev`
- foreign table `fdw_nonprod_fretexdev."Actor_CT"`

Your runtime pull layer then reads both registered `Actor` foreign tables and lands rows into the same shared local table:

- `adopus."stg_Actor"`

The merge layer then writes to:

- `adopus."Actor"`

where the rows are separated by `customer_id`.

### Later: Add `Soknad`

When you later want to add `Soknad`, you do not create new FDW servers for these customers.

You only add two more rows to `source_table_registration` and rerun the bootstrap step.

```sql
INSERT INTO "cdc_management"."source_table_registration" (
    "source_instance_key",
    "logical_table_name",
    "remote_schema_name",
    "remote_table_name",
    "target_schema_name",
    "target_table_name",
    "enabled"
)
VALUES
    (
        'nonprod_test',
        'Soknad',
        'cdc',
        'dbo_Soknad_CT',
        'adopus',
        'Soknad',
        true
    ),
    (
        'nonprod_fretexdev',
        'Soknad',
        'cdc',
        'dbo_Soknad_CT',
        'adopus',
        'Soknad',
        true
    )
ON CONFLICT ("source_instance_key", "logical_table_name") DO UPDATE
SET
    "remote_schema_name" = EXCLUDED."remote_schema_name",
    "remote_table_name" = EXCLUDED."remote_table_name",
    "target_schema_name" = EXCLUDED."target_schema_name",
    "target_table_name" = EXCLUDED."target_table_name",
    "enabled" = EXCLUDED."enabled";
```

Then the bootstrap step creates or refreshes:

- `fdw_nonprod_test."Soknad_CT"`
- `fdw_nonprod_fretexdev."Soknad_CT"`

No new server objects are needed because both source databases are already registered.

### Runtime View Of The Same Example

With the metadata above, the runtime thinks in terms of registrations, not handwritten customer scripts:

For `Actor`:

- source instance `nonprod_test` -> foreign table `fdw_nonprod_test."Actor_CT"` -> shared target `adopus."Actor"`
- source instance `nonprod_fretexdev` -> foreign table `fdw_nonprod_fretexdev."Actor_CT"` -> shared target `adopus."Actor"`

Later for `Soknad`:

- source instance `nonprod_test` -> foreign table `fdw_nonprod_test."Soknad_CT"` -> shared target `adopus."Soknad"`
- source instance `nonprod_fretexdev` -> foreign table `fdw_nonprod_fretexdev."Soknad_CT"` -> shared target `adopus."Soknad"`

This is the practical benefit of the metadata model: the code stays the same, only the registrations change.

---

## Production Scale Guidance

For the scale you described, the design can still work, but only if polling is tiered and centrally scheduled.

Current generator state:

- the generated native runtime already includes schedule metadata, due-work claiming, and success/failure procedures
- the current behavior is still effectively fixed-interval polling, not true adaptive polling

For the concrete gap analysis and implementation plan to move from fixed tiers to adaptive polling, see:

- `../development/ADAPTIVE_NATIVE_CDC_POLLING_GAP_ANALYSIS_AND_IMPLEMENTATION_PLAN.md`

### Recommended Starting Polling Tiers

Your suggested pattern is reasonable:

- hot tables: every `30 seconds`
- warm tables: every `60 seconds`

That is a much better starting point than trying to poll everything every `5 seconds`.

Recommended initial classification:

- hot tables: business-critical workflow tables, user tables, request tables, status tables
- warm tables: secondary operational tables, lower-change tables
- cold tables: optional future tier, every `5-15 minutes` for low-change or reference-like tables

### Why This Works Better

At larger scale, the scheduler must think in terms of due work, not fixed per-table loops.

Good model:

- one scheduler cycle runs every few seconds
- it selects only registrations where `next_pull_at <= now()`
- it executes a bounded number of pulls
- it updates `last_pull_at` and computes the next execution time

Bad model:

- one job per table
- one cron entry per source registration
- every table polled at the same aggressive interval

### Scheduling Policy And Runtime State

The generated native runtime now keeps scheduling identity, policy, and hot runtime state in separate tables:

- `cdc_management.source_table_registration` for stable registration identity and target mapping
- `cdc_management.native_cdc_schedule_policy` for base interval, bounds, batch size, lease defaults, priority, and `jitter_millis`
- `cdc_management.native_cdc_runtime_state` for `current_poll_interval_seconds`, `next_pull_at`, lease ownership, failures, and last-run metrics

Policy and runtime rows are kept in sync from registration metadata by the generated helper:

- `cdc_management.sync_native_cdc_registration_state(...)`

If service metadata includes `source.tables.<schema.table>.native_cdc`, the generated SQL seeds those policy defaults automatically.

### Example Policy Updates

Set `Actor` to a hot profile:

```sql
UPDATE "cdc_management"."native_cdc_schedule_policy"
SET
    "schedule_profile" = 'hot',
    "base_poll_interval_seconds" = 30,
    "min_poll_interval_seconds" = 5,
    "max_poll_interval_seconds" = 300,
    "max_rows_per_pull" = 2000,
    "lease_seconds" = 120,
    "poll_priority" = 10,
    "jitter_millis" = 0,
    "max_backoff_seconds" = 900,
    "updated_at" = now(),
    "config_version" = "config_version" + 1
WHERE "logical_table_name" = 'Actor';
```

Set all other tables to a slower default:

```sql
UPDATE "cdc_management"."native_cdc_schedule_policy"
SET
    "schedule_profile" = 'warm',
    "base_poll_interval_seconds" = 60,
    "min_poll_interval_seconds" = 15,
    "max_poll_interval_seconds" = 300,
    "max_rows_per_pull" = 1000,
    "lease_seconds" = 120,
    "poll_priority" = 100,
    "jitter_millis" = 500,
    "max_backoff_seconds" = 900,
    "updated_at" = now(),
    "config_version" = "config_version" + 1
WHERE "logical_table_name" <> 'Actor';
```

If you change base policy and want runtime state to restart from it for one table:

```sql
UPDATE "cdc_management"."native_cdc_runtime_state" runtime
SET
    "current_poll_interval_seconds" = policy."base_poll_interval_seconds",
    "next_pull_at" = now(),
    "updated_at" = now()
FROM "cdc_management"."native_cdc_schedule_policy" policy
WHERE runtime."source_instance_key" = policy."source_instance_key"
  AND runtime."logical_table_name" = policy."logical_table_name"
  AND runtime."logical_table_name" = 'Actor';
```

### Scheduler Query Pattern

For manual smoke tests, call the generated claim helper instead of querying registration rows directly.

```sql
SELECT *
FROM "cdc_management"."claim_due_native_cdc_work"(100, NULL, 'manual_test');
```

For operations, use the generated health view.

```sql
SELECT *
FROM "cdc_management"."v_native_cdc_health"
ORDER BY "poll_priority", "next_pull_at";
```

After a successful pull plus merge:

```sql
CALL "cdc_management"."mark_native_cdc_success"(
    'nonprod_test',
    'Actor',
    123,
    842
);
```

After a failed pull or merge:

```sql
CALL "cdc_management"."mark_native_cdc_failure"(
    'nonprod_test',
    'Actor',
    'example error text',
    120
);
```

If a pull or merge is still running and needs more lease time:

```sql
CALL "cdc_management"."renew_native_cdc_lease"(
    'nonprod_test',
    'Actor',
    'manual_test',
    120
);
```

### Merge Scheduling

Do not merge inline for every tiny pull if many tables are active.

Recommended:

- pull scheduler runs every `5-10 seconds`
- merge scheduler runs every `10-30 seconds`
- merge only tables with pending staging rows

This keeps remote MSSQL read work separate from local PostgreSQL merge work.

### What Is Reasonable Today

For roughly 40 customers, this approach remains realistic if:

- only a smaller subset of tables is in the `30 second` tier
- the rest are `60 seconds` or slower
- the scheduler processes due work in batches
- concurrency is capped

Recommended first cap:

- max 5-10 concurrent source pulls per environment

That is conservative and gives room for tuning.

### What To Plan For At 100+ Customers

Once customer count moves above 100, plan for these additional controls:

- partition staging tables by logical table and optionally by time
- cap per-environment concurrent pull workers
- cap per-source-database parallel pulls to avoid hammering one MSSQL database
- add lag monitoring per registration
- add automatic pause for repeatedly failing registrations
- separate hot-table worker pools from warm-table worker pools

At that stage, the approach still can work, but it must behave like an orchestrated polling platform, not just a collection of functions and cron jobs.

### Practical Recommendation

Start with:

- `Actor` and other hot tables: `30 seconds`
- medium-change tables: `60 seconds`
- no cold tier initially unless you need it
- one central scheduler loop
- one central merge loop
- metrics from day one

Then adjust based on:

- average batch size
- pull duration
- merge duration
- checkpoint lag
- source database load

This is the safest path to make the design work now while still leaving room for 100+ customers later.

---

## Summary

Recommended command-assisted order:

1. keep `source-groups.yaml`, service YAML, and schema YAML current
2. run `cdc fdw plan --service <service> --source-env <env>`
3. run `cdc fdw sql --service <service> --source-env <env> --output <file>`
4. apply the generated SQL in PostgreSQL
5. verify FDW reads using `SELECT ... LIMIT 1`
6. run `cdc manage-migrations generate --service <service> --topology fdw`
7. apply the generated migrations in PostgreSQL
8. let the external scheduler use the generated claim/pull/merge helpers
9. run pull/merge verification

Manual reference order:

1. enable PostgreSQL extensions
2. create schemas
3. create one FDW server for one source database
4. create user mapping
5. create one foreign table per source CDC table
6. test reads from foreign table
7. create shared final table
8. create shared staging table
9. create source registry
10. create checkpoint table
11. create pull function
12. create merge procedure
13. run pull manually
14. inspect staging
15. run merge manually
16. inspect final table
