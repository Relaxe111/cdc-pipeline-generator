# CDC Pipeline Generator - AI Agent Instructions

## 🎯 Project Purpose

Reusable library for generating Redpanda Connect CDC pipelines. Supports two architectural patterns:
- **db-per-tenant**: N databases → N pipelines (e.g., Adopus - 26 customers, each has own MSSQL database)
- **db-shared**: 1 database → 1 pipeline (e.g., Asma - all customers in shared PostgreSQL with customer_id)

**Data Flow:** `Source DB (MSSQL/Postgres) → CDC → Kafka → Sink Pipeline → PostgreSQL`

## 🏗️ Architecture (REVERSED APPROACH)

**This generator is the MAIN development environment:**

```
~/carasent/
├── cdc-pipeline-generator/          # THIS PROJECT - Main dev environment
│   ├── docker-compose.yml           # Dev container (mounts implementations)
│   ├── Dockerfile.dev               # Full dev tools (MSSQL, Postgres, Fish)
│   ├── cdc_generator/               # Python package
│   │   ├── core/                    # Pipeline generation logic
│   │   ├── helpers/                 # Batch, MSSQL, config helpers
│   │   ├── validators/              # Schema validation
│   │   └── cli/                     # CLI commands
│   └── examples/
│       ├── db-per-tenant/           # Adopus pattern reference
│       └── db-shared/               # Asma pattern reference
│
├── adopus-cdc-pipeline/             # Implementation 1 - INFRASTRUCTURE ONLY
│   ├── docker-compose.yml           # Postgres, Redpanda, MSSQL (NO dev container)
│   ├── server-groups.yaml           # Single group: adopus
│   └── 2-services/adopus.yaml       # 26 customers, db-per-tenant
│
└── asma-cdc-pipeline/               # FUTURE: Implementation 2 - INFRASTRUCTURE ONLY
    ├── docker-compose.yml           # Infrastructure only
    ├── server-groups.yaml           # Single group: asma
    └── 2-services/directory.yaml    # Shared database pattern
```

**Developer Workflow:**
1. Start adopus infrastructure: `cd ~/carasent/adopus-cdc-pipeline && docker compose up -d`
2. Start THIS dev container: `cd ~/carasent/cdc-pipeline-generator && docker compose up -d`
3. Enter container: `docker compose exec dev fish`
4. Edit generator code: `/workspace/cdc_generator/...`
5. Test against adopus: `cd /implementations/adopus && cdc generate`
6. Changes in `/workspace` sync to `~/carasent/cdc-pipeline-generator/` (host)
7. Changes in `/implementations/adopus` sync to `~/carasent/adopus-cdc-pipeline/` (host)

**Key: ONE dev container, access to ALL implementations**

## ⚠️⚠️⚠️ CRITICAL: Development Container Context ⚠️⚠️⚠️

**ALL development happens inside THIS project's dev container:**

**To enter dev container:**
```bash
# From host (macOS)
cd ~/carasent/cdc-pipeline-generator
docker compose exec dev fish
```

**Inside container you have:**
- `/workspace/` - This generator library (editable)
- `/implementations/adopus/` - Adopus implementation (mounted rw)
- `/implementations/asma/` - Asma implementation (will exist later)
- `network_mode: host` - Access to implementation infrastructure (Postgres, Kafka on localhost)

**When user asks to run commands/scripts:**
1. If already inside container: Run directly
2. If on host: Say "Enter dev container first: `docker compose exec dev fish`"
3. Then run commands from appropriate directory

## Critical Patterns

### File Size Limit (ALWAYS ENFORCE)

**Maximum file size is 500 lines of code**

When any file exceeds 500 lines:
1. Refactor into smaller, focused modules
2. Create a package structure (folder with `__init__.py`)
3. Each module must have a **single, clear responsibility**
4. Document the module structure

### PostgreSQL Quoting (ALWAYS)
```sql
-- ✅ CORRECT
SELECT "actno", "Navn" FROM avansas."Actor" WHERE "actno" = 123;
INSERT INTO avansas."stg_Actor" ("FraverId") VALUES (1);

-- ❌ WRONG (relation does not exist)
SELECT actno FROM avansas.Actor;
```
**Why:** MSSQL uses PascalCase, PostgreSQL needs quotes to preserve case.

### Fish Shell (no bash syntax)

**⚠️ CRITICAL: Heredocs are NOT supported in Fish shell**
- **NEVER use heredoc syntax (`<< 'EOF'`, `<< EOF`, etc.)** - Fish will fail
- **Alternatives:**
  - Use `sed -i` for multi-line file edits
  - Use `printf '%s\n' "line1" "line2"`
  - Write Python/other tools for complex content
  
**Other Fish differences:**
- No `&&`: Use `; and` or separate commands  
- Variables: `$VAR`, not `${VAR}`

## Directory Structure

**This Generator Project:**
```
cdc-pipeline-generator/
├── cdc_generator/
│   ├── core/
│   │   └── pipeline_generator.py      # Main generation logic (from 3-generate-pipelines.py)
│   ├── helpers/
│   │   ├── helpers_batch.py           # Batch operations, map_pg_type
│   │   ├── helpers_mssql.py           # MSSQL connectivity
│   │   └── service_config.py          # YAML config loading/validation
│   ├── validators/
│   │   └── manage_service/            # 18 validator modules
│   └── cli/
│       ├── service.py                 # manage-service command
│       └── server_group.py            # manage-server-group command
├── examples/
│   ├── db-per-tenant/                 # Adopus pattern reference
│   │   ├── README.md                  # Pattern documentation
│   │   ├── server-groups.yaml         # Example config
│   │   ├── services/adopus.yaml       # 26-customer example
│   │   └── templates/*.yaml           # Pipeline templates
│   └── db-shared/                     # Asma pattern reference
│       ├── README.md
│       ├── server-groups.yaml
│       ├── services/directory.yaml
│       └── templates/*.yaml
└── tests/                             # Future: unit tests
```

**Implementation Projects (mounted at /implementations/):**
```
/implementations/adopus/               # db-per-tenant implementation
├── server-groups.yaml                 # Single group: adopus
├── 2-services/adopus.yaml             # 26 customers
├── 3-pipeline-templates/              # Templates with {{VARS}}
└── generated/pipelines/               # Auto-generated (read-only)

/implementations/asma/                 # Future: db-shared implementation
├── server-groups.yaml                 # Single group: asma
├── 2-services/directory.yaml          # Shared database config
└── 3-pipeline-templates/
```

## Common Tasks

**Add table to service:**
```bash
cd /implementations/adopus
cdc manage-service --service adopus --add-table Actor --primary-key actno
cdc generate
```

**List available tables:**
```bash
cd /implementations/adopus
cdc manage-service --service adopus --inspect --schema dbo
```

**Generate pipelines:**
```bash
cd /implementations/adopus
cdc generate  # Uses /workspace/cdc_generator/core/pipeline_generator.py
```

**Edit generator code:**
```bash
# From inside dev container
vim /workspace/cdc_generator/core/pipeline_generator.py
# Changes sync to ~/carasent/cdc-pipeline-generator/ on host
```

## Testing Workflow

**Unit tests (future):**
```bash
cd /workspace
pytest tests/
```

**Integration tests:**
```bash
# Test against adopus implementation
cd /implementations/adopus
cdc generate
# Verify output in generated/pipelines/

# Compare with examples
diff /implementations/adopus/generated/pipelines/local/ \
     /workspace/examples/db-per-tenant/generated/pipelines/local/
```

## Python Development

**Before creating shared functions:**
1. Check existing `helpers_*.py` files for similar logic
2. If found, abstract and reuse - don't duplicate
3. If new domain needed, create `helpers_{domain}.py`
4. Keep related utilities grouped by domain prefix

**Module structure:**
- `core/` - Pipeline generation, main logic
- `helpers/` - Reusable utilities (batch ops, type mapping, MSSQL)
- `validators/` - Schema validation, config validation
- `cli/` - Command-line interface commands

## Server Groups & Patterns

**Server groups** control CDC architecture patterns. The generator is **environment-agnostic** - each implementation handles its own environment differentiation (dev/staging/prod).

| server_group_type | Example | Architecture | Multi-tenancy |
|-------------------|---------|--------------|---------------|
| `db-per-tenant` | adopus | One server, one service. N databases → N pipelines (1 per customer) | Database-level isolation |
| `db-shared` | asma | One server, multiple services. 1 database → 1 pipeline (all customers) | Table-level with `customer_id` |

**Required fields in service YAML:**
- `server_group`: Reference to server group name
- `cdc_tables`: Tables for CDC (always at root level)
- `reference`: Reference customer/database for validation

**For db-per-tenant only:**
- `customers`: Array of customer configs

**Note:** Environment configurations (connection strings, credentials, etc.) are implementation-specific and handled outside the generator library.

## Version Control

**This project uses Git with master branch:**
```bash
cd /workspace
git add .
git commit -m "feat: add support for X"
git push origin master
```

**Semantic versioning:**
- Major: Breaking changes (v2.0.0)
- Minor: New features (v1.1.0)
- Patch: Bug fixes (v1.0.1)

**Release process (Phase 6):**
```bash
git tag v1.0.0
git push origin master --tags
```

## Migration Status

**Current Phase:** Phase 4 complete ✅

**Completed:**
- ✅ Phase 1: Generator library structure created
- ✅ Phase 2: Scripts extracted from adopus-cdc-pipeline
- ✅ Phase 3: Reference implementations (db-per-tenant + db-shared)
- ✅ Phase 4: Reversed architecture - generator is main dev environment

**Next:**
- Phase 5: Prepare for asma-cdc-pipeline (documentation)
- Phase 6: Version and publish generator (tag v1.0.0)

See `/implementations/adopus/MIGRATION_TO_GENERATOR_LIBRARY.md` for full plan.

## Future Plans

**Not yet implemented:**
- Field mappings + transformations (column renaming, value conversion)
- Fan-out pattern (1 record → N records based on conditions)
- Tenant ID pattern (common staging with `customer_id` for db-per-tenant)
- Multi-sink support (1 source → N sink databases per customer)
- PyPI publication for easier distribution
- Automated testing in CI/CD
- GitHub Actions for release automation
