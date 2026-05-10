# Development Workflow

## ⚙️ Development Environment

**The CDC CLI runs directly on the host.** Install it once and use `cdc` from any
implementation directory. Docker is optional and only needed for local database
infrastructure.

### Host-first setup:
```bash
cd ~/carasent/asma-modules/_tools/cdc_cli
pip install -e .
```

The `cdc` command is now available everywhere on your host.

### Optional: Dev container (isolated environment):
```bash
cd ~/carasent/asma-modules/_tools/cdc_cli
docker compose exec dev fish
```
Inside the container:
- `/workspace/` - This generator (editable)
- `/implementations/adopus/` - Adopus implementation (mounted rw)
- `/implementations/asma/` - Asma implementation (mounted rw)

### Edit and test workflow:
1. Edit generator code in `_tools/cdc_cli/cdc_generator/...`
2. Test against adopus: `cd /implementations/adopus && cdc generate`
3. Verify output in `pipelines/generated/`

---

## 🗂️ Implementation File Structure

**What generator creates/expects:**

| Path | Purpose | Edit? |
|------|---------|-------|
| `source-groups.yaml` | Server group definitions | ⚠️ USE CLI |
| `services/{service}.yaml` | Service config | ⚠️ USE CLI |
| `pipelines/templates/*.yaml` | Templates with `{{VARS}}` | ✅ EDIT |
| `pipelines/generated/` | Auto-generated | ❌ READ-ONLY |

---

## 🎯 Common Tasks

### Add table:
```bash
cd /implementations/adopus
cdc manage-service --service adopus --add-table Actor --primary-key actno
cdc generate
```

### Inspect database:
```bash
cdc manage-service --service adopus --inspect --schema dbo
```

### Test generator changes:
```bash
# Edit code in /workspace/
cd /implementations/adopus
cdc generate  # Uses your modified generator code
```

### Reload Fish shell completions (after modifying cdc.fish):
```bash
# ✅ ALWAYS use this after modifying cdc_generator/templates/init/cdc.fish
cdc reload-cdc-autocompletions
```

**⚠️ IMPORTANT: When modifying Fish completions:**
- Edit: `cdc_generator/templates/init/cdc.fish`
- Reload: `cdc reload-cdc-autocompletions`
- The reload command copies updated completions to system directory and reloads them
- Test with `cdc <subcommand> <TAB>` to verify completions work

---

## 📖 Documentation References

**Always read before coding:**
- **[Coding Guidelines](.github/copilot-instructions-coding-guidelines.md)** - Code organization, style, naming conventions, function/file size limits, type hints

**For Bento / Bloblang transformations:**
- **[Bloblang Runtime Docs](_docs/bento-bloblang/README.md)** - Complete Bloblang reference and pipeline patterns

| Document | Use Case |
|----------|----------|
| [Bloblang Fundamentals](_docs/bento-bloblang/01-BLOBLANG-FUNDAMENTALS.md) | Core syntax: assignment, variables, conditionals, maps |
| [Bloblang Methods](_docs/bento-bloblang/02-BLOBLANG-METHODS.md) | String, number, timestamp, array, object, JWT methods |
| [Bloblang Functions](_docs/bento-bloblang/03-BLOBLANG-FUNCTIONS.md) | Built-in functions: uuid, now, env, content, metadata |
| [HTTP Inputs](_docs/bento-bloblang/04-HTTP-INPUTS.md) | Webhook receivers, JWT/signature validation, API polling |
| [SQL Patterns](_docs/bento-bloblang/05-SQL-PATTERNS.md) | PostgreSQL integration, UPSERT, batching, connection pools |
| [Error Handling](_docs/bento-bloblang/06-ERROR-HANDLING.md) | try/catch, DLQ, fallback outputs, error routing |
| [Pipeline Patterns](_docs/bento-bloblang/07-PIPELINE-PATTERNS.md) | Complete CDC pipeline examples, multi-input/output |
| [**Pipeline Templating**](_docs/bento-bloblang/08-PIPELINE-TEMPLATING.md) | **Template structure, .blobl files, generation flow** |

---

## 📚 Additional Reference

- **Architecture index:** `_docs/README.md`
- **Bento migration decision plan:** `_docs/architecture/BENTO_MIGRATION_DECISION_PLAN.md`
- **Pattern examples:** `examples/db-per-tenant/`, `examples/db-shared/`
- **API documentation:** `_docs/`
- **Implementation guides:** Implementation repos' copilot-instructions
