# Migration Plan: Extract CDC Generator to Separate Library

**Status:** 🚧 In Progress  
**Started:** 2026-01-31  
**Goal:** Extract pipeline generation scripts into reusable library with reference implementations

## Architecture Overview

**REVERSED APPROACH (Updated 2026-01-31):**

```
~/carasent/
├── cdc-pipeline-generator/          # NEW: Reusable library + DEV CONTAINER
│   ├── cdc_generator/               # Python package
│   ├── examples/
│   │   ├── db-per-tenant/          # Reference: Adopus pattern
│   │   └── db-shared/              # Reference: Asma pattern
│   ├── docker-compose.yml          # MAIN dev container (mounts implementations)
│   └── Dockerfile.dev              # Full dev environment (MSSQL, Postgres, Fish)
│
├── adopus-cdc-pipeline/            # Implementation 1 - INFRASTRUCTURE ONLY
│   ├── source-groups.yaml          # Single server group (adopus)
│   ├── services/                 # Service configs
│   └── docker-compose.yml          # Infrastructure only (NO dev container)
│
└── asma-cdc-pipeline/              # FUTURE: Implementation 2 - INFRASTRUCTURE ONLY
    ├── source-groups.yaml          # Single server group (asma)
    ├── services/                 # Service configs
    └── docker-compose.yml          # Infrastructure only (NO dev container)
```

**Developer Workflow:**
1. Start infrastructure: `cd ~/carasent/adopus-cdc-pipeline && docker compose up -d`
2. Start dev container: `cd ~/carasent/cdc-pipeline-generator && docker compose up -d`
3. Enter dev container: `docker compose exec dev fish`
4. Edit generator: `/workspace/cdc_generator/...`
5. Test against adopus: `cd /implementations/adopus && cdc generate`
6. Test against asma: `cd /implementations/asma && cdc generate`

## Key Principles

1. **Generator scripts live ONLY in cdc-pipeline-generator**
2. **ONE dev container in generator** - Mounts both implementation projects
3. **Reference implementations** - Each pattern documented in examples/
4. **Single source of truth** - source-groups.yaml per implementation
5. **Full dev environment in generator** - MSSQL tools, Postgres, Fish, all deps
6. **Infrastructure only in implementations** - Databases and Kafka only

---

## Phase 1: Create Generator Library Structure

### Step 1.1: Create Repository Structure
- [x] Create `~/carasent/cdc-pipeline-generator/` directory
- [x] Initialize Git repository
- [x] Create basic Python package structure

```bash
cd ~/carasent
mkdir cdc-pipeline-generator
cd cdc-pipeline-generator
git init
```

**Directory structure:**
```
cdc-pipeline-generator/
├── README.md
├── pyproject.toml
├── setup.py
├── .gitignore
├── Dockerfile.dev
├── docker-compose.yml
├── cdc_generator/
│   ├── __init__.py
│   ├── core/
│   │   ├── __init__.py
│   │   └── pipeline_generator.py
│   ├── helpers/
│   │   ├── __init__.py
│   │   ├── helpers_batch.py
│   │   ├── helpers_mssql.py
│   │   └── service_config.py
│   ├── validators/
│   │   ├── __init__.py
│   │   └── service_validator.py
│   └── cli/
│       ├── __init__.py
│       └── commands.py
├── examples/
│   ├── db-per-tenant/
│   │   ├── README.md
│   │   ├── source-groups.yaml
│   │   ├── services/
│   │   │   └── adopus.yaml
│   │   └── templates/
│   │       ├── source-pipeline.yaml
│   │       └── sink-pipeline.yaml
│   └── db-shared/
│       ├── README.md
│       ├── source-groups.yaml
│       ├── services/
│       │   └── asma.yaml
│       └── templates/
│           ├── source-pipeline.yaml
│           └── sink-pipeline.yaml
└── tests/
    └── __init__.py
```

### Step 1.2: Create Package Configuration Files
- [x] Create `pyproject.toml`
- [x] Create `setup.py`
- [x] Create `.gitignore`
- [x] Create `README.md`
- [x] Create `LICENSE` (MIT)

**Validation:** ✅ Package structure correct, will install in Python 3.11+ container

###x] Create `Dockerfile.dev` (Python 3.11 + Fish)
- [x] Create `docker-compose.yml` (dev + optional test-postgres)
- [ ] Test dev container startup

**Validation:** Next - test container startup
**Validation:** `docker compose up -d && docker compose exec dev fish` works

---

## Phase 2: Extract Scripts from adopus-cdc-pipeline

### Step 2.1: Identify Files to Move
- [x] `scripts/3-generate-pipelines.py` → `cdc_generator/core/pipeline_generator.py`
- [x] `scripts/helpers_batch.py` → `cdc_generator/helpers/helpers_batch.py`
- [x] `scripts/helpers_mssql.py` → `cdc_generator/helpers/helpers_mssql.py`
- [x] `scripts/service_config.py` → `cdc_generator/helpers/service_config.py`
- [x] `scripts/manage_service/*.py` → `cdc_generator/validators/`
- [x] `scripts/manage-source-groups.py` → `cdc_generator/cli/server_group.py`
- [x] `scripts/manage-service.py` → `cdc_generator/cli/service.py`

### Step 2.2: Move and Refactor Core Generator
- [x] Copy `3-generate-pipelines.py` to generator
- [x] Update imports to use package structure
- [x] Add `__init__.py` exports
- [x] Remove workspace-specific hardcoded paths

**Files affected:**
- `cdc_generator/core/pipeline_generator.py`
- `cdc_generator/core/__init__.py`

**Validation:** ✅ Import works: `from cdc_generator.core import generate_pipelines`

### Step 2.3: Move Helper Modules
- [x] Copy helper files to `cdc_generator/helpers/`
- [x] Update cross-imports between helpers
- [x] Add package exports

**Files affected:**
- `cdc_generator/helpers/helpers_batch.py`
- `cdc_generator/helpers/helpers_mssql.py`
- `cdc_generator/helpers/service_config.py`
- `cdc_generator/helpers/__init__.py`

**Validation:** ✅ Import works: `from cdc_generator.helpers import map_pg_type`

### Step 2.4: Move CLI Commands
- [x] Copy manage-service.py logic
- [x] Copy manage-source-groups.py logic
- [ ] Create unified CLI entry point
- [ ] Add argparse/click CLI interface

**Files affected:**
- `cdc_generator/cli/commands.py`
- `cdc_generator/cli/service.py`
- `cdc_generator/cli/server_group.py`
- `cdc_generator/__main__.py` (for `python -m cdc_generator`)

**Validation:** `python -m cdc_generator --help` shows commands

---

## Phase 3: Create Reference Implementations

### Step 3.1: Create db-per-tenant Example (Adopus Pattern)
- [x] Copy current `source-groups.yaml` (adopus group only)
- [x] Copy `services/adopus.yaml` as template
- [x] Copy `pipeline-templates/*.yaml`
- [x] Create README.md explaining the pattern
- [x] Add example customer configurations

**Location:** `examples/db-per-tenant/`

**Content:**
- How db-per-tenant works
- When to use this pattern
- Configuration examples
- Expected output structure

### Step 3.2: Create db-shared Example (Asma Pattern)
- [x] Create source-groups.yaml with asma group
- [x] Create example service config for asma pattern
- [x] Create appropriate pipeline templates
- [x] Create README.md explaining the pattern
- [x] Add example configurations

**Location:** `examples/db-shared/`

**Content:**
- How db-shared works
- When to use this pattern
- Differences from db-per-tenant
- Configuration examples

**Validation:** ✅ Both examples have complete, working configs

---

## Phase 4: Update adopus-cdc-pipeline to Use Library

**REVERSED APPROACH: Remove dev container, keep only infrastructure**

### Step 4.1: Update Docker Compose
- [x] Remove dev container entirely from docker-compose.yml
- [x] Keep only infrastructure services (postgres, redpanda, mssql, etc.)
- [x] Update header comments to reflect infrastructure-only purpose

**Files affected:**
- `docker-compose.yml`

**Validation:** Only infrastructure services remain

### Step 4.2: Update Generator Docker Compose
- [x] Add volume mounts for both implementations:
  - `../adopus-cdc-pipeline:/implementations/adopus:rw`
  - `../asma-cdc-pipeline:/implementations/asma:rw`
- [x] Add `network_mode: host` to access infrastructure databases
- [x] Set PYTHONPATH to `/workspace`

**Files affected:**
- `~/carasent/cdc-pipeline-generator/docker-compose.yml`

**Validation:** Generator can access both implementations

### Step 4.3: Update Generator Dockerfile
- [x] Add MSSQL tools from adopus Dockerfile.dev
- [x] Add PostgreSQL client
- [x] Add Fish shell configuration
- [x] Add Docker CLI
- [x] Install all Python dependencies (yaml, pgcli, etc.)

**Files affected:**
- `~/carasent/cdc-pipeline-generator/Dockerfile.dev`

**Validation:** Generator container has all dev tools

### Step 4.4: Clean Up adopus-cdc-pipeline
- [x] Remove Dockerfile.dev (no longer needed)
- [x] Remove requirements-dev.txt (no longer needed)  
- [x] Keep `cdc` wrapper for backwards compatibility (delegates to generator)
- [x] Backup old scripts in scripts.backup/

**Files affected:**
- `Dockerfile.dev` (remove)
- `requirements-dev.txt` (remove)
- `cdc` (keep as thin wrapper)

**Validation:** No dev container files remain

### Step 4.5: Clean Up source-groups.yaml
- [x] Keep ONLY adopus server group
- [x] Remove asma and other groups
- [x] Update comments to reflect single-group usage

**Files affected:**
- `source-groups.yaml`

**Validation:** Only one server group present

### Step 4.6: Test Full Workflow
- [ ] Start infrastructure: `cd adopus-cdc-pipeline && docker compose up -d`
- [ ] Start dev container: `cd cdc-pipeline-generator && docker compose up -d`
- [ ] Enter dev container: `docker compose exec dev fish`
- [ ] Test generator: `cd /implementations/adopus && cdc generate`
- [ ] Verify bidirectional sync (edit in container, check host files)
- [ ] Generate pipelines: `cdc generate`
- [ ] Validate output matches previous version
- [ ] Test all CLI commands work
- [ ] Verify LSN cache works
- [ ] Test local pipeline execution

**Validation:** All existing functionality works

---

## Phase 5: Prepare for asma-cdc-pipeline (Future)

### Step 5.1: Document Asma Setup Process
- [ ] Create guide in generator: `examples/db-shared/SETUP.md`
- [ ] Document source-groups.yaml for asma
- [ ] Document service configuration
- [ ] List required environment variables

**Validation:** Documentation is clear and complete

### Step 5.2: Create asma-cdc-pipeline Skeleton (Optional)
- [ ] Create project directory structure
- [ ] Copy docker-compose.yml from adopus (modify for asma)
- [ ] Copy source-groups.yaml from examples/db-shared
- [ ] Create initial service configs
- [ ] Add generator mount

**Validation:** Project structure ready for asma implementation

---

## Phase 6: Version and Publish Generator

### Step 6.1: Tag First Stable Version
- [ ] Clean up code
- [ ] Add comprehensive README
- [ ] Create CHANGELOG.md
- [ ] Tag v1.0.0
- [ ] Push to GitHub

**Commands:**
```bash
cd ~/carasent/cdc-pipeline-generator
git add .
git commit -m "Initial stable release"
git tag v1.0.0
git push origin master --tags
```

### Step 6.2: Update Production References
- [ ] In `adopus-cdc-pipeline/requirements.txt` (production):
  ```
  cdc-generator @ git+https://github.com/carasent/cdc-pipeline-generator.git@v1.0.0
  ```
- [ ] Test production install
- [ ] Update CI/CD if applicable

**Validation:** Production deployment uses pinned version

---

## Current Progress Tracker

### Completed ✅
- Phase 1: Generator library structure created
- Phase 2: Scripts extracted and refactored  
- Phase 3: Reference implementations created (db-per-tenant + db-shared)

### In Progress 🚧
- Phase 4: Update adopus-cdc-pipeline to use library

### Blocked ⛔
- None

### Next Up 📋
- Phase 4, Step 4.1: Update docker-compose.yml with generator mount

---

## Rollback Plan

If migration fails at any point:

1. **Keep backups:** `scripts.backup/` directory with all original scripts
2. **Git branches:** Work on `feature/generator-library` branch
3. **Docker tags:** Keep old Dockerfile as `Dockerfile.dev.backup`
4. **Quick restore:** Symlink scripts back to original locations

**Rollback command:**
```bash
cd ~/carasent/adopus-cdc-pipeline
git checkout master
docker compose down
docker compose up -d --build
```

---

## Success Criteria

- [ ] Generator library works standalone
- [ ] Both reference implementations (db-per-tenant, db-shared) documented
- [ ] adopus-cdc-pipeline uses generator via mount
- [ ] All existing functionality preserved
- [ ] Changes in generator auto-reflect in implementations
- [ ] Dev workflow: Edit in /generator, changes sync everywhere
- [ ] Production: Pinned version from GitHub
- [ ] Documentation complete for future asma-cdc-pipeline setup

---

## Notes & Decisions

### 2026-01-31: Initial Planning
- Decided on bidirectional mount strategy for development
- Generator repo will be lightweight (no heavy databases)
- Implementation repos keep full infrastructure stack
- Reference implementations serve as documentation
- **Open Source:** Generator will be published as open source on GitHub
- **Git branch:** Using `master` as default branch

### Future Considerations
- Add LICENSE file (MIT or Apache 2.0 recommended for open source)
- Add CONTRIBUTING.md guidelines
- Consider PyPI publication for easier distribution
- Add automated testing in generator repo
- Create GitHub Actions for release automation
- Add integration tests using example configurations
