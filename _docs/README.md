# CDC Pipeline Generator - Documentation

This documentation covers the **CDC Pipeline Generator library** - a reusable tool for creating Redpanda Connect CDC pipelines.

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
- **[BENTO_MIGRATION_DECISION_PLAN.md](./architecture/BENTO_MIGRATION_DECISION_PLAN.md)** - Decision gates and phased plan for Redpanda Connect → Bento migration in generator-driven source/sink pipelines
- **[CDC_EVENT_TRACING.md](./architecture/CDC_EVENT_TRACING.md)** - End-to-end event latency tracing with embedded timestamps, sampling strategies (≤5% permanent + 100% debug mode), bottleneck detection queries, and performance mitigations

### [Development](./development/)
Contributing and coding standards:
- **[CODING_STANDARDS.md](./development/CODING_STANDARDS.md)** - Code style, patterns, best practices
- **[CLI_COLORS.md](./development/CLI_COLORS.md)** - Terminal output formatting
- **[PIPELINE_TEST_MODES.md](./development/PIPELINE_TEST_MODES.md)** - AI-friendly test mode definitions (`--fast-pipelines` vs `--full-pipelines`)
- **[GAP_ANALYSIS_COMMAND_GROUPING.md](./development/GAP_ANALYSIS_COMMAND_GROUPING.md)** - Pipeline/migration command grouping gaps and rollout plan

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
- Example: All directory data in one database

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
