# Migration Plan (Remaining Work Only)

**Status:** 🚧 In Progress  
**Goal:** Finish migration to reusable generator + production-ready release.

---

## 1) Validate Runtime Workflow (Adopus)

### 1.1 End-to-end workflow validation
- [ ] Start infra in adopus implementation
- [ ] Start generator dev container
- [ ] Enter dev container
- [ ] Run generation from `/implementations/adopus`
- [ ] After generation, run simple isolated pipeline scenario (mock source/http → generated source pipeline → MQ/connect → sink files → expected output assertions)
- [ ] Verify output correctness against expected/generated artifacts
- [ ] Verify local dev flow still works (pipeline execution + LSN behavior)

### 1.2 Sync behavior validation
- [ ] Verify bidirectional sync between mounted host paths and container paths
- [ ] Confirm edits in generator reflect immediately in implementation workflow

### 1.3 Post-generate test modes for pipelines
- [ ] Add fast pipeline test mode: `cdc test --fast-pipelines`
- [ ] Add full pipeline test mode: `cdc test --full-pipelines`
- [ ] Ensure both modes are runnable right after `cdc generate`
- [ ] Document scope/differences between fast vs full pipeline validation (see `_docs/development/PIPELINE_TEST_MODES.md`)

**Done when:** generation + local runtime checks pass with no regressions.

---

## 2) Prepare Asma Implementation Docs/Setup

### 2.1 Documentation
- [ ] Add `examples/db-shared/SETUP.md`
- [ ] Document `source-groups.yaml` for asma/db-shared
- [ ] Document service config shape and required env vars

### 2.2 Scaffold-based bootstrap validation
- [ ] Validate `cdc scaffold` can bootstrap db-shared starter structure for asma use-case
- [ ] Verify scaffold output includes/aligns required compose + config files
- [ ] Document any post-scaffold manual steps (if any)

**Done when:** a new contributor can follow docs and run db-shared setup successfully.

---

## 3) Release Readiness (v1.0.0)

### 3.1 Repository release prep
- [ ] Final cleanup pass
- [ ] Ensure README is complete for install + usage
- [ ] Add/refresh `CHANGELOG.md`
- [ ] Tag and push `v1.0.0`

### 3.2 Production pinning
- [ ] Update production dependency in adopus to pinned git tag
- [ ] Test production install path
- [ ] Update CI/CD references if required

**Done when:** production can install exact tagged version and run without manual fixes.

---

## 4) Final Acceptance Checklist

- [ ] Generator works standalone
- [ ] Both patterns (db-per-tenant, db-shared) are documented and runnable
- [ ] Implementations consume generator via mount/dev workflow
- [ ] No functional regressions in CLI/generation flow
- [ ] Release tag published and production references updated

---

## Suggested Execution Order (AI-friendly)

1. Run section **1** (runtime validation first).  
2. Complete section **2** (docs/setup for db-shared).  
3. Complete section **3** (release + production pinning).  
4. Verify section **4** fully checked.
