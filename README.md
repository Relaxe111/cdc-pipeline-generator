# CDC Pipeline Generator

**Generate pipeline configurations for Change Data Capture (CDC) workflows.**

A CLI-first tool that reads YAML service definitions and produces streaming pipeline configurations, SQL migrations, and deployment artifacts. Supports **db-per-tenant** and **db-shared** multi-tenancy patterns with configurable data transport backends.

---

## Architecture

The generator sits at the centre of a CDC pipeline — it reads source database schemas, produces sink table definitions and pipeline configurations, and renders the runtime artifacts consumed by the streaming layer.

### Data Transport Options

CDC data can be moved from source to sink through one of two paths:

| Path | Transport | Typical Use |
|------|-----------|-------------|
| **Streaming** | Redpanda / Kafka | High-throughput, low-latency CDC with exactly-once semantics |
| **FDW** | PostgreSQL Foreign Data Wrappers | Direct MSSQL→PG pull without an external message broker |

#### Streaming (Redpanda / Kafka)

Source change events are captured, streamed through a message broker, and consumed by sink processors that write to the target PostgreSQL database.

```text
MSSQL → CDC capture → Redpanda/Kafka → Bento sink → PostgreSQL
```

#### FDW (Foreign Data Wrapper)

The generator can produce configurations that use PostgreSQL Foreign Data Wrappers (`tds_fdw`) to pull data directly from MSSQL into staging tables, followed by merge procedures that apply changes to the target tables.

```text
MSSQL ← tds_fdw ← PostgreSQL (staging → merge → target)
```

#### Native PostgreSQL-to-PostgreSQL

For PostgreSQL source databases, native logical replication or polling-based CDC can be used without an external broker.

```text
PostgreSQL → native CDC polling → PostgreSQL target
```

All three paths are configuration-driven — the generator produces the correct pipeline YAML, SQL migrations, and runtime helpers based on the chosen transport and source database type.

---

## Installation

### Option A: Docker (zero host dependencies)

```bash
docker pull asmacarma/cdc-pipeline-generator:latest
```

### Option B: Host install via pip

```bash
# Editable install for active development
pip install -e .

# Or install directly from the repository
pip install .
```

After host install, the `cdc` command is available on your shell PATH.

---

## Quick Start

### 1. Create a project and initialize

```bash
mkdir my-cdc-project && cd my-cdc-project
cdc init
```

This creates the project structure: `source-groups.yaml`, `services/`, `pipelines/`, directories.

### 2. Scaffold a server group

```bash
# db-per-tenant (one database per customer)
cdc scaffold my-group \
  --pattern db-per-tenant \
  --source-type mssql \
  --extraction-pattern "^myapp_(?P<customer>[^_]+)$"

# db-shared (single database, multi-tenant)
cdc scaffold my-group \
  --pattern db-shared \
  --source-type postgres \
  --extraction-pattern "^myapp_(?P<service>[^_]+)_(?P<env>(dev|stage|prod))$" \
  --environment-aware
```

### 3. Configure services and tables

```bash
# Create a service
cdc manage-services config --create-service my-service

# Add source tables
cdc manage-services config --service my-service --add-source-table dbo.Users --primary-key id
cdc manage-services config --service my-service --add-source-table dbo.Orders --primary-key order_id

# Inspect and save source schemas
cdc manage-services config --service my-service --inspect --all --save
```

### 4. Manage schemas and migrations

```bash
# Generate DDL migrations for the sink database
cdc manage-migrations generate

# Review changes
cdc manage-migrations diff

# Apply migrations
cdc manage-migrations apply
```

### 5. Generate pipeline configurations

```bash
# Generate for a single service
cdc generate --service my-service --environment dev

# Generate for all services
cdc generate --all --environment dev
```

---

## Bounded editor RBAC compiler

`cdc rbac` compiles the ASMA-8350 admission subset locally: official OpenDD v1
`ModelPermissions` + `TypePermissions`, real catalog names, and flat `_eq`/`and`
filters on tenant/actor UUID columns. The platform roles are `recipient`,
`super_user`, and `therapist`. The prepared four-column `editor.qnrs` inputs are
pinned from artifact commit `af7fa36775ef994bb66f9f125042ee71e5d26758` in
`tests/fixtures/rbac/`. This is a SELECT admission slice; full service policy,
owner writes, transport closure and G5 installation remain separately owned.

```bash
cdc rbac doctor
cdc rbac schema --catalog rbac/asma8350/inputs/editor-qnrs.schema.json
cdc rbac validate --source rbac/asma8350/inputs/editor-qnrs.opendd.json \
  --catalog rbac/asma8350/inputs/editor-qnrs.schema.json
cdc rbac generate --source rbac/asma8350/inputs/editor-qnrs.opendd.json \
  --catalog rbac/asma8350/inputs/editor-qnrs.schema.json
cdc rbac emit-migration --hsr . --migration-version 1800000000000 \
  --source rbac/asma8350/inputs/editor-qnrs.opendd.json \
  --catalog rbac/asma8350/inputs/editor-qnrs.schema.json
cdc rbac check --hsr . --source rbac/asma8350/inputs/editor-qnrs.opendd.json \
  --catalog rbac/asma8350/inputs/editor-qnrs.schema.json
```

`generate` prints reviewable policy-preparation SQL and metadata. `emit-migration`
requires an explicit 13-digit migration version after the owner's existing
migrations. It emits `migrations/default/<version>_rbac_editor_qnrs/{up,down}.sql`,
merges only SELECT into the existing canonical table YAML, and records input and
migration byte hashes plus the owned SELECT structural hash in lock format 3
`rbac/.rbac-lock.json`. Each immutable history entry retains lossless base64
source/catalog bytes, their byte and canonical JSON SHA256 hashes, the semantic
contract hash, predecessor hash and regenerated output hashes. The lock pins the
owned RBAC implementation/schema hashes and the exactly pinned `jsonschema` version.
Shared CDC helpers and unpinned dependency versions are not fingerprint gates;
the latter are recorded separately as environment observations. Checks revalidate every input receipt and regenerate every historical
up/down and SELECT projection, then require the complete owned migration
inventory and active table include. Output-identical changes to allowed
dependencies or shared helpers remain checkable and upgradeable. Genuine SQL
or canonical SELECT changes, deleted history, orphan SQL, rewritten output
hashes, and noncanonical or inconsistent locks fail before writes. Owned
compiler or pinned-validator upgrades require reviewed re-attestation. A canonical payload hash binds the whole lock; the reviewed
publication SHA256 is the external provenance anchor. This is reproducibility
and drift evidence, not a signature or protection against replacing an entire
internally consistent input/output/lock set. No timestamps, host paths or Git
checkout are required at runtime. A changed source requires a new migration.
Existing history is immutable; unchanged inputs produce no writes.
Reader-only rollback restores the previous generated policies; its fresh rollback drops only
those policies. Qualified writer rollback requires a qualified writer predecessor;
otherwise it refuses before writes rather than removing writer fences. This subset emits no GRANT or REVOKE statements: schema, table
and column ACLs remain unchanged in either direction. Neither direction changes
RLS enable/force flags. Activation is
outside this admission subset and requires separately owned owner-write, worker
and Hasura-connection coverage first.

Granting schema USAGE and column SELECT while RLS is disabled opens un-isolated
reads across both tenants, even with no `app.*` context: prepared policies are
inert. Activation must ship before, or atomically with, any such grants. Earlier
grant-bearing output must not be committed to the artifact's master migration
chain (which auto-applies it), or applied anywhere, until that ordering and the
activation coverage are satisfied. Current policy-only output gives `editor_app`
no new access.

Lock format 2 lacks exact input receipts for complete historical regeneration.
It is refused with an explicit format diagnostic; no missing history is inferred.
No artifact lock or installation has been admitted. Retain any earlier review
lock and SQL, reproduce each original input/version in a clean review directory,
and compare every historical SQL and current SELECT before a separately reviewed
provenance replacement. Deleting the lock where compiler migrations exist is
refused. This does not restrict another owner's G4, relationship or format edits.

`cdc rbac reattest` provides a lock-only recovery path for compiler or pinned
validator upgrades, including the previously reviewed six-file/six-version v3
receipt. Supply `--hsr`, the exact `--expected-lock-sha256`, and a nonempty
`--review-reference`. The default command prints a candidate and its
`candidate_sha256` without writing. It revalidates the original byte receipts,
regenerates every historical up/down SQL byte hash and canonical SELECT JSON
byte hash under the new implementation, and checks the actual migration
inventory, SQL bytes, active SELECT, includes and G4 identity. Any output or
artifact drift refuses the proposal; this is not a baseline reset for changed
outputs. SELECT remains structural so other owners' formatting and relationship
edits remain valid, and G4 never enters the write set.

Review the exact candidate through the owning workflow. To apply, repeat the
same command with `--apply-reviewed-sha256` set to that candidate's SHA256.
An old-lock change or a changed candidate (including code or environment
observations) invalidates the supplied hash and causes zero writes. Successful
application writes only the lock, retains the old implementation/pins/observations
verbatim, and appends the prior lock hash, reviewed history-prefix hash/count,
new implementation and review reference. Checks validate review-chain continuity;
subsequent migrations preserve reviewed prefixes. No timestamp or host path is
added. The CLI binds the reviewed bytes; it does not certify who approved the
external review. This command gives no target installation, activation or
auto-apply authority. The installation holds below remain unchanged.

The structured merge serializes only the SELECT block in Hasura CLI export
format, preserving all other table-file bytes, including relationship comments
and explicit nulls. Hasura's omitted false aggregation default and explicit
false are equivalent during ownership checks. Relationship edits and format-only
re-exports remain checkable and upgradeable. The table identity and include index
are checked. Separately owned `public.queries` G4 metadata is verified by table
identity and excluded from the compiler write set; its owner can evolve legacy
write permissions without re-baselining the compiler lock. The compiler
never emits Hasura mutations, creates roles, changes creator defaults, or grants
write/table-wide privileges. Missing/privileged/owning `editor_app`, unexpected
policies, catalog mismatch, and preexisting broad privileges abort generated SQL
transactionally. In particular, the preflight still rejects a real target where
`editor_app` holds its REQ-001 table SELECT/INSERT/UPDATE/DELETE grants. Target
installation remains held for owner-write, worker and Hasura-connection work;
merging the compiler does not release that hold. This four-column slice cannot
repair or replace the full owner's write envelope.

Validation uses the vendored official schema at Hasura revision
`94915fe51d6d21bd7f6d4452dc16221bef8cfefd` (SHA256
`3ff0d2a5680d57c8a042c0c577b7dd68aab7f71ab1b6dd9b81e6569702cf7b20`) and
`jsonschema==4.25.1`. Its exact permission slice removes nested `$id` annotations
so local `#/definitions` references resolve correctly; tests compare every
validation keyword to upstream. External schema retrieval is disabled. Local
semantic checks validate role/model/type/column references, session mapping,
mandatory tenant equality, and equal column envelopes across roles. Relationships,
OR/NOT, literals, presets, mutations and other versions in unqualified reader input fail closed. This is the
local validator authorized by the ROOT disposition; it makes no hosted DDN build
or full DDN semantic-validation claim.

```bash
python -m pytest tests/test_rbac.py tests/test_rbac_repairs.py tests/test_rbac_provenance.py tests/test_rbac_reattest.py
# Explicit disposable local PostgreSQL and Hasura only:
RBAC_TEST_DSN='host=127.0.0.1 port=58350 dbname=postgres user=postgres' \
RBAC_TEST_HASURA_URL='http://127.0.0.1:58351' \
RBAC_TEST_HASURA_CLI='/opt/homebrew/bin/hasura' \
  python -m pytest tests/test_rbac_postgres.py
```

The integration suite creates disposable databases, uses the canonical
`editor_app` connection identity, and executes generated SQL plus real Hasura
queries across two synthetic tenants. It tests fresh and upgrade failures,
negative ACLs, context reset and rollback. Enforcement tests activate RLS before
provisioning column SELECT, only inside disposable fixtures. Preparation/rollback
tests verify ACLs and all enable/force states remain unchanged, `editor_app` gets
no reads with absent or supplied context, and preexisting inactive owner/worker
contexts still work. Fresh and upgrade REQ-001 ACL refusals leave state intact.
A real Hasura CLI fresh/upgrade export round trip checks canonical byte output.
RBAC commands skip workdir usage statistics so they do not create `_docs/_stats`
in another repository. These tests qualify this compiler
subset; they do not discharge the artifact owner's full 1,177-migration L3
oracles or prove target installation. Publication is PR-only for independent
exact-head review.

### Source candidate: explicit writer and retained-policy composition

The ASMA-8350 source continuation implements the finite `ownerEnvelope` catalog
extension adopted by ROOT disposition
`QNR-ROOT-8350-OWNER-ENVELOPE-COMPATIBILITY-DISPOSITION-20261004-1`
(SHA256 `6badca45f5659342616b1232466bf088fbedaa9fc994f65ce87fc60233b69be1`).
Its exact schema is copied from artifact attachment `dc3006c5`, SHA256
`4a3eb93017637557b7c5cb37bdbad494b4a3c80b5c2e9aa167363d2835bb8ea5`;
no artifact metadata, migrations, G4 or installation inputs are modified here.

The pinned official OpenDD `ModelPermission` supports `relationalInsert`,
`relationalUpdate` and `relationalDelete` opt-ins, each defined as an empty object
with `additionalProperties: false`. These markers cannot express row/column
authority. The compiler therefore binds explicit per-role/command/column and
old/new-row rules in the referenced owning declarative source, retaining the
official OpenDD bytes and offline validator unchanged. SELECT rules and ACLs
never supply missing writer rules. Every required command needs independently
generated coverage; a role need not receive every command. The fixture grants
I/U/D only to `therapist`, with tenant **and actor** comparisons, while its read
rule is tenant-only. This fixture is test data, not genuine owner or worker rights.

Inputs use the existing `--source` and `--catalog` interfaces. The catalog retains
the exact finite `ownerEnvelope` and offline `sourceSnapshots` containing each
commit/path/SHA256 reference and base64 original bytes. The source review must
match an admitted source/review identity; arbitrary self-consistent references
and hashes never confer authority. **No actual owning source/review is admitted.**
The current admission pins are disclosed `TEST_ONLY_NONOWNER_ISO` fixtures.
Generated snapshot paths prefixed `generated:ISO/` identify embedded fixture
receipts, not files claimed to exist in a historical Git commit. The declaration
and review paths identify exact committed fixture files. Compiler context binds
the owned implementation and validator hashes. A generation receipt binds all
source/context/policy references and the complete writer output bytes; validation
regenerates those bytes through the same renderer used by `cdc rbac generate`.
SQL/hand-authored policy text cannot substitute for a declaration or witness.

The supported column envelope is explicit whole-relation INSERT/UPDATE authority
and no DELETE column assignment. Per-role partial changed-column authority is
refused: PostgreSQL row policies cannot implement it. Qualified table SELECT
also requires explicit complete reader column coverage. The finite full-table
catalog types are `uuid`, `text`, `int4`, `date`, `timestamptz`; predicates still
require real UUID tenant/actor columns. Default reader-only admission retains
its original four-column type/profile and byte-identical historical SQL/SELECT.

Retained policies are compatibility inputs only and never enter CREATE/ALTER/DROP
output. The compiler parses the supported positive flat SQL language: typed
tenant/actor session equalities, exact existing role equalities, parentheses,
AND/OR and Boolean constants, including PostgreSQL 17 deparse text casts.
Unknown functions, types, contexts, literals, NOT, subqueries and unsupported
definitions refuse. It enumerates the complete symbolic equality/context states
and expands ALL for each command, proving permissive OR/restrictive AND cannot
widen SELECT, INSERT new-row CHECK, UPDATE old-row USING/new-row CHECK, or DELETE
USING. It also refuses loss of authorized positives; universal denial is not
compatibility. Retained UPDATE/ALL with null WITH CHECK is outside the exact
attachment schema, so implicit fallback is refused rather than fabricated.

Before policy writes, qualified SQL compares exact complete originated/effective
CRUD, grant options, table owner, schema USAGE, column ACLs, recursive membership
and role attributes/options, actual named creator global/schema defaults, and
every generated/retained policy command/role/permissiveness/USING/CHECK definition.
Names and hashes alone do not pass. Missing/excess/changed rights or definitions
abort without revoking rights or rewriting retained policies. A relation lock
serializes relation changes during preflight. It does not qualify global role/
creator change control: genuine installation staging remains an actual prerequisite.

Only owned disposable databases named `asma8350_writer_<32 lowercase hex digits>`
can execute this test-only composed SQL. Other database names refuse before
policy writes. The compiler emits **policies only**, with no grants, RLS activation,
new principal, Hasura mutations or guard/worker implementation. Schema USAGE and
SELECT/CRUD grants while RLS is inactive open un-isolated reads; activation must
precede or be atomic with exposure after complete genuine writer/read/guard/
worker coverage. The test fixture activates RLS before test ACL exposure and
authenticates independently as nonowner, NOSUPERUSER/NOBYPASSRLS `editor_app`.
Bootstrap/table-owner success is never its positive oracle.

History retains every original source snapshot and generated output. Reviewed
re-attestation can admit an output-identical compiler change while retaining the
prior implementation context; it cannot replace writer authority, policy bytes
or previous history. Inputs without qualified composition retain strict REQ-001
CRUD and retained-policy refusal. G4 remains separate and excluded from writes.

The focused fixture tests include unfiltered INSERT/UPDATE/DELETE without
WHERE/RETURNING alongside filtered/RETURNING forms. Seeded permissive policies
demonstrate that SELECT masking can hide widening in filtered probes. Same-tenant
positive writes, foreign/missing/malformed/reader/unknown-worker denials, committed
row readback, transaction-local context reset and final-row trigger mutation
are exercised. The invoker test trigger is not the real guard/definer contract.
The original **18 oracles / 34 variants** remain unexecuted as complete canonical
cases; the **22 runtime spec IDs** retain distinct per-case fixture/blocked status
in the [source evidence ledger](tests/fixtures/rbac/writer-composition-cases.json).
No source test or partial fixture result becomes
a guard/permit/worker/revocation/Hasura-connection qualification.

All ten `targetInputs` remain null. Actual connection/owner/default creators,
guard identities and full helper ACLs, worker lease/bootstrap/executor/issuer/
revoker facts and actual Hasura context are unqualified. PostgreSQL and Hasura
metadata operations are separate; no shared transaction is claimed. Publication
is a compiler SOURCE PR for exact-head review. Target install, owner-qualified
production emission, RLS activation, artifact-master auto-install and A6/8350
completion stay **HELD**.

### Guard-owner and installation contract supplement

The compiler-owned [installation contract](tests/fixtures/rbac/installation-contract.json)
is a review specification, not an installer. It maps to artifact
`af7fa36775ef994bb66f9f125042ee71e5d26758`:
`rbac/asma8350/install-contract.json` (SHA256
`378be16cc26356670f5c40e09955cc0bf669ae1978fdf7269b29225edc3402cd`),
its read-only `readback.sql`, the legacy projection spec and L3 fact packet.
The [exact owning contract reference](tests/fixtures/rbac/owning-install-contract.json)
is retained without changing that separately owned source. The historical
artifact statements about the missing compiler/hosted DDN qualification remain
historical: merged compiler `c3469aec19b02984326b4865f7d1550cad0a8914` now supplies
the authorized local admission slice, not full service/worker closure.

Guard owner means the exact existing PostgreSQL owner of both
`public.qnr_enforce_legacy_instance_writer_rule()` and
`public.qnr_enforce_legacy_cache_writer_rule()`. Creator means the actual
`current_role` creating each object, including alternate deploy creators.
Connection identities and Hasura session permission names are distinct.
`editor_app` in generated policies supplies no evidence of the target Bun,
Hasura, creator, bootstrap or guard-owner mapping.

| Contract requirement | Required evidence / installation boundary |
| --- | --- |
| Guard owner | NOLOGIN, NOSUPERUSER, NOBYPASSRLS, NOCREATEDB, NOCREATEROLE, NOREPLICATION; only two guard functions owned, no data-table ownership; no product SET/INHERIT/ADMIN path to owner, including transitive membership. |
| Guard data and helper ACLs | Exact SELECT columns, locking UPDATE(qnr_id) on sync links, UPDATE(id) on collab documents, and UPDATE(consumed_at,consumed_properties_digest,changed_by_kind) on permits, as enumerated in the supplement. Only its three named helper EXECUTEs; no permit INSERT/DELETE/revocation, table-wide writes or grant option. |
| Search path and triggers | Fixed pg_catalog,public,editor; schema USAGE and no untrusted/ongoing CREATE; any temporary CREATE for ALTER OWNER revoked in the same transaction. Both canonical BEFORE ROW INSERT/UPDATE/DELETE triggers must be ALWAYS and attached to the exact guard. |
| Function ACLs | Revoke effective PUBLIC EXECUTE on guards and named helpers; enumerate explicit helper callers and inherited rights. Trigger execution is distinct from bootstrap CREATE/TRIGGER rights. Qualify Bun's invoker-helper closure. |
| Creator defaults | Enumerate every actual creator, global and schema defaults, plus effective rights. Global creator REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC is required; schema-only revocation cannot undo it. Objects, scoped ACLs/defaults and owner changes must be atomic; no inherited creator assumption or broad table/sequence grants. |
| Fresh and upgrade | Absent mapped roles fail before writes. A future separately authorized unit supplies a new scoped migration; accepted history and unrelated mirror rights remain intact. Failure restores reviewed snapshots and never removes writer fences to open a write. |
| Grant/activation ordering | Policies are inert while RLS is off; granting SELECT then opens un-isolated reads with no context. Require complete owner-write, worker and Hasura connection coverage, then activation before or atomically with exposure. This compiler changes neither grants nor activation. |

PostgreSQL requires an UPDATE right on at least one column for row-locking
SELECT, and applies default privileges for the actual creator. See the
[SELECT contract](https://www.postgresql.org/docs/17/sql-select.html) and
[creator-default contract](https://www.postgresql.org/docs/17/sql-alterdefaultprivileges.html).
These requirements are checked under an actual nonowner; the superuser setup
fixture cannot substitute for guard, creator or target proof.

The supplement defines 18 negative oracles with setup, action, expected outcome,
required redacted evidence, fresh/upgrade applicability and exact owning JSON
clause. Its gate map covers G1-G5 and existing L3-T01 through L3-T08. Acceptance
requires both the negative oracles and their declared positive controls on the
same pinned package: guard locking and consumption, creator/default failures,
atomic rollback, memberships, excess ACLs, trigger defects, second writers,
actual Hasura transport and get_non_read_queries, invalid/revoked/concurrent
permits, worker bootstrap and connection reset, activation ordering, REQ-001
upgrade refusal, and preservation of unrelated rights. Offline tests check
source pins, clause mappings, exact envelopes and the absence of qualification
claims; they do not execute these guard-owner installation oracles.

All actual target/connection/owner/creator/default-privilege inputs remain
unknown, including alternate creators, bootstrap exposure, worker authority and
issuer/revoker facts. Every installation oracle is BLOCKED_NOT_EXECUTED.
The compiler still refuses existing REQ-001 SELECT/INSERT/UPDATE/DELETE grants;
it does not repair the owner's write envelope. The consumer transport co-requisite,
actual Hasura function-read qualification, worker/revocation facts and staging
controls remain installation gates. Preparation is READY for exact-head review;
target admission, activation and A6/ASMA-8350 completion remain held. Publication
of this continuation is compiler PR only: no artifact migration emission or
auto-apply, target install, new principal or worker authority.

---

## Multi-Tenancy Patterns

### db-per-tenant

Each customer has a dedicated source database. The generator creates one source+sink pipeline per customer database.

```
Extraction pattern: ^myapp_(?P<customer>[^_]+)$
Matches: myapp_customer_a, myapp_customer_b
```

### db-shared

All customers share a single database, differentiated by a column (e.g. `customer_id`) or schema. Requires `--environment-aware`.

```
Extraction pattern: ^myapp_(?P<service>[^_]+)_(?P<env>(dev|stage|prod))$
Matches: myapp_users_dev, myapp_users_prod
```

---

## Command Reference

| Command | Description |
| ------- | ----------- |
| `cdc init` | Initialize a new CDC project |
| `cdc scaffold <name>` | Scaffold a server group with database services |
| `cdc manage-services config` | Create, list, inspect services and tables |
| `cdc manage-services config --inspect-sink` | Inspect and save target sink schemas |
| `cdc manage-migrations generate` | Generate PostgreSQL DDL migrations |
| `cdc manage-migrations diff` | Show pending schema changes |
| `cdc manage-migrations apply` | Apply migrations to target database |
| `cdc generate` | Generate pipeline YAML configurations |
| `cdc manage-source-groups` | Manage source database groups |
| `cdc manage-sink-groups` | Manage sink/target groups |
| `cdc validate` | Validate all configurations |

---

## Project Structure

```text
cdc-pipeline-generator/
├── cdc_generator/           # Core library
│   ├── cli/                # Click command groups
│   ├── core/               # Pipeline generation, migration engine
│   ├── helpers/            # Database, FDW, MSSQL utilities
│   ├── service-schemas/    # YAML schema definitions and type adapters
│   ├── templates/          # Jinja2 pipeline templates
│   └── validators/         # Configuration and schema validation
├── tests/                   # Test suite
├── _docs/                   # Architecture, getting started, CLI reference
├── examples/                # db-per-tenant and db-shared reference implementations
├── setup.py / pyproject.toml  # Package metadata
└── Dockerfile               # Docker runtime image
```

---

## Development

See `_docs/getting-started/` for setup instructions, `_docs/architecture/` for design decisions, and `_docs/cli/` for the full CLI command reference.

The CDC CLI runs directly on the host. Install once and use `cdc` from any directory.

- ✅ `cdc` command available everywhere on your host
- ✅ Access to source and target databases
- ✅ Fish shell with auto-completions (reload with `cdc reload-cdc-autocompletions`)
- ✅ Git and SSH keys available

Optionally, a dev container is available if you prefer an isolated environment:
```bash
docker compose exec dev fish
```

---

## 📁 Project Structure

---

## 📁 Project Structure

After running `cdc scaffold`, your project will have:

```
my-cdc-project/
├── docker-compose.yml           # Optional infrastructure (databases, streaming)
├── Dockerfile.dev               # Optional dev container image
├── .env.example                 # Environment variables template
├── .env                         # Your credentials (git-ignored)
├── .gitignore                   # Git ignore rules
├── source-groups.yaml           # Server group config (generated by cdc)
├── README.md                    # Quick start guide
├── services/                    # Service definitions (generated by cdc)
│   └── my-service.yaml
├── pipelines/                   # Pipeline templates + generated YAML
│   ├── templates/               # source-pipeline.yaml, sink-pipeline.yaml
│   └── generated/
│       ├── sources/
│       └── sinks/
└── generated/                   # Generated non-pipeline output (git-ignored)
  ├── schemas/                 # PostgreSQL schemas
  └── pg-migrations/           # PostgreSQL migrations
```

---

## 🔧 Advanced Usage

### Using as Python Library

```python
from cdc_generator.core.pipeline_generator import generate_pipelines

# Generate pipelines programmatically
generate_pipelines(
  service='my-service',
  environment='dev',
  output_dir='./pipelines/generated'
)
```

### Custom Pipeline Templates

Place custom Jinja2 templates in `pipelines/templates/`:

```yaml
# pipelines/templates/source-pipeline.yaml
input:
  mssql_cdc:
    dsn: "{{ dsn }}"
    tables: {{ tables | tojson }}
    # Your custom configuration
```

### Environment-Specific Configuration

Use environment variables in source-groups.yaml:

```yaml
server:
  host: ${MSSQL_HOST}        # Replaced at runtime
  port: ${MSSQL_PORT}
  user: ${MSSQL_USER}
  password: ${MSSQL_PASSWORD}
```

### SQL-Based Source Custom Keys (Source + Sink)

Use custom keys to compute per-database values during `--update` and write them
into each source environment entry (for example `customer_id`).

```bash
# Source groups: persist SQL custom key definition
cdc manage-source-groups \
  --add-source-custom-key customer_id \
  --custom-key-value "SELECT customer_id FROM dbo.settings" \
  --custom-key-exec-type sql

# Run update to execute the SQL per discovered database
cdc manage-source-groups --update
```

```bash
# Sink groups: same custom key model
cdc manage-sink-groups \
  --sink-group sink_analytics \
  --add-source-custom-key customer_id \
  --custom-key-value "SELECT customer_id FROM public.settings" \
  --custom-key-exec-type sql

# Run sink update to execute SQL per discovered sink database
cdc manage-sink-groups --update --sink-group sink_analytics
```

Generated shape (simplified):

```yaml
sources:
  directory:
    schemas: [public]
    nonprod:
      server: default
      database: directory_db
      table_count: 42
      customer_id: cust-001
```

If a key returns no value for a specific server/database, the update continues and
prints a warning with that server/database context.

---

## 🤝 Contributing

### For Library Contributors

If you want to contribute to the cdc-pipeline-generator library itself:

```bash
# Clone repository
git clone https://github.com/Relaxe111/cdc-pipeline-generator.git
cd cdc-pipeline-generator

# Install in editable mode with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black .
ruff check .
```

### For Users

If you're using the library in your project, just install from PyPI as shown in [Installation](#-installation).

---

## 📚 Resources
