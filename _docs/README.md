# CDC Pipeline Generator - Documentation

This documentation covers the **CDC Pipeline Generator library** - a reusable tool for creating Bento CDC pipelines.

## 📁 Documentation Structure

### [Getting Started](./getting-started/)

Quick setup guides and troubleshooting:

- **[TEST_SETUP.md](./getting-started/TEST_SETUP.md)** - Setting up development environment
- **[TROUBLESHOOTING.md](./getting-started/TROUBLESHOOTING.md)** - Common issues and solutions
- **[MONITORING.md](./getting-started/MONITORING.md)** - Web UIs and monitoring tools
- **[UI_ACCESS.md](./getting-started/UI_ACCESS.md)** - Accessing Redpanda Console, Adminer, etc.

### [Architecture](./architecture/)

Core CDC concepts and system design:

- **[PIPELINES.md](./architecture/PIPELINES.md)** - Pipeline structure and flow
- **[TEMPLATES.md](./architecture/TEMPLATES.md)** - Template system and variables
- **[FIELD_MAPPING.md](./architecture/FIELD_MAPPING.md)** - MSSQL → PostgreSQL field mapping
- **[EVENT_DRIVEN_MERGE_SYSTEM.md](./architecture/EVENT_DRIVEN_MERGE_SYSTEM.md)** - Merge/upsert strategy
- **[CDC_PRODUCTION_SAFEGUARDS.md](./architecture/CDC_PRODUCTION_SAFEGUARDS.md)** - Production deployment checklist
- **[STREAMING_ALTERNATIVES.md](./architecture/STREAMING_ALTERNATIVES.md)** - Bento, Bytewax, Materialize, and Vector comparison
- **[POSTGRES_NATIVE_CDC_OPTION.md](./architecture/POSTGRES_NATIVE_CDC_OPTION.md)** - Proposed PostgreSQL-native CDC runtime using `tds_fdw` for MSSQL pull and logical replication for PostgreSQL fan-out
- **[TOPOLOGY_RUNTIME_COMPOSITION.md](./architecture/TOPOLOGY_RUNTIME_COMPOSITION.md)** - Proposed simplification of the architecture model into a hierarchy of pattern, source type, and topology, with the target user-facing topology names `redpanda`, `fdw`, and `pg_native`
- **[TDS_FDW_IMPLEMENTATION_GUIDE.md](./architecture/TDS_FDW_IMPLEMENTATION_GUIDE.md)** - Copy-paste practical guide for creating FDW servers, foreign tables, staging tables, checkpoints, and merge flow for MSSQL CDC into PostgreSQL
- **[BENTO_MIGRATION_DECISION_PLAN.md](./architecture/BENTO_MIGRATION_DECISION_PLAN.md)** - Decision gates and phased plan for legacy runtime → Bento migration in generator-driven source/sink pipelines
- **[CDC_EVENT_TRACING.md](./architecture/CDC_EVENT_TRACING.md)** - End-to-end event latency tracing with embedded timestamps, sampling strategies (≤5% permanent + 100% debug mode), bottleneck detection queries, and performance mitigations

### [Development](./development/)

Contributing and coding standards:

- **[CODING_STANDARDS.md](./development/CODING_STANDARDS.md)** - Code style, patterns, best practices
- **[CLI_COLORS.md](./development/CLI_COLORS.md)** - Terminal output formatting
- **[PIPELINE_TEST_MODES.md](./development/PIPELINE_TEST_MODES.md)** - AI-friendly test mode definitions (`--fast-pipelines` vs `--full-pipelines`)
- **[GAP_ANALYSIS_COMMAND_GROUPING.md](./development/GAP_ANALYSIS_COMMAND_GROUPING.md)** - Pipeline/migration command grouping gaps and rollout plan
- **[ADAPTIVE_NATIVE_CDC_POLLING_GAP_ANALYSIS_AND_IMPLEMENTATION_PLAN.md](./development/ADAPTIVE_NATIVE_CDC_POLLING_GAP_ANALYSIS_AND_IMPLEMENTATION_PLAN.md)** - Concrete gap analysis and rollout plan for turning the native MSSQL FDW runtime from fixed-interval polling into adaptive central scheduling
- **[ASMA_CDC_ORCHESTRATOR_ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md](./development/ASMA_CDC_ORCHESTRATOR_ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md)** - Long-term architecture, best practices, cache boundaries, replica model, and phased implementation plan for the ASMA CDC orchestrator

### [CLI Reference](./cli/)

Command-line tool documentation (coming soon):

- `cdc generate` - Generate pipelines from config
- `cdc manage-service` - Add/remove tables, inspect schemas
- `cdc manage-source-groups` - Manage server group configs
- **[SOURCE_CUSTOM_KEYS.md](./cli/SOURCE_CUSTOM_KEYS.md)** - SQL-based computed keys for source/sink updates

## 🏗️ Architecture Patterns

This library supports two CDC patterns:

### db-per-tenant (e.g., Adopus)

- One server, one service
- N customer databases → N source pipelines
- Each customer has own MSSQL database
- Example: 26 adopus customers, 26 separate databases

### db-shared (e.g., Asma)

- One server, multiple services
- Multi-tenancy at table level (customer_id)
- 1 shared database → 1 source pipeline
- All customers in same database with `customer_id` field
- Example: shared directory data in one owner database, with selected tables fanned out downstream to other services

**Note:** Environment differentiation (dev/staging/prod) is handled by each implementation, not by the generator library.

See [examples/](../examples/) for reference implementations.

## 🔗 Related Projects

**Implementations** (separate repositories):

- `adopus-cdc-pipeline/` - Production implementation (db-per-tenant)
- `asma-cdc-pipeline/` - Future implementation (db-shared)

Implementation docs are kept in their respective repos for deployment-specific content.

## 📖 Additional Resources

- **Main README**: [../README.md](../README.md)
- **Migration Plan**: [../MIGRATION_PLAN.md](../MIGRATION_PLAN.md)
- **Examples**: [../examples/](../examples/)
- **Pattern + CLI Audit Summary**: [./PATTERN_AND_CLI_AUDIT_SUMMARY.md](./PATTERN_AND_CLI_AUDIT_SUMMARY.md)
