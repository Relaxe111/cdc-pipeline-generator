# Development Workflow

## ⚙️ Development Environment

**This generator is the main dev environment:**

**Dev container location:** This project (`cdc-pipeline-generator/`)  
**Mounted implementations:** `/implementations/adopus/`, `/implementations/asma/`  
**Network access:** Host mode - access to implementation infrastructure

### To enter dev container:
```bash
cd ~/carasent/cdc-pipeline-generator
docker compose exec dev fish
```

### Inside container:
- `/workspace/` - This generator (editable)
- `/implementations/adopus/` - Adopus implementation (mounted rw)
- `/implementations/asma/` - Asma implementation (mounted rw)

### Edit and test workflow:
1. Edit generator code: `/workspace/cdc_generator/...`
2. Test against adopus: `cd /implementations/adopus && cdc generate`
3. Verify output in `generated/pipelines/`

---

## 🗂️ Implementation File Structure

**What generator creates/expects:**

| Path | Purpose | Edit? |
|------|---------|-------|
| `source-groups.yaml` | Server group definitions | ⚠️ USE CLI |
| `services/{service}.yaml` | Service config | ⚠️ USE CLI |
| `pipeline-templates/*.yaml` | Templates with `{{VARS}}` | ✅ EDIT |
| `generated/pipelines/` | Auto-generated | ❌ READ-ONLY |

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
- Reload: `cdc reload-cdc-autocompletions` (in dev container)
- The reload command copies updated completions to system directory and reloads them
- Test with `cdc <subcommand> <TAB>` to verify completions work

---

## 📖 Documentation References

**Always read before coding:**
- **[Coding Guidelines](.github/copilot-coding-guidelines.md)** - Code organization, style, naming conventions, function/file size limits, type hints

**For Redpanda Connect / Bloblang transformations:**
- **[Redpanda Connect Docs](_docs/redpanda-connect/README.md)** - Complete Bloblang reference and pipeline patterns

| Document | Use Case |
|----------|----------|
| [Bloblang Fundamentals](_docs/redpanda-connect/01-BLOBLANG-FUNDAMENTALS.md) | Core syntax: assignment, variables, conditionals, maps |
| [Bloblang Methods](_docs/redpanda-connect/02-BLOBLANG-METHODS.md) | String, number, timestamp, array, object, JWT methods |
| [Bloblang Functions](_docs/redpanda-connect/03-BLOBLANG-FUNCTIONS.md) | Built-in functions: uuid, now, env, content, metadata |
| [HTTP Inputs](_docs/redpanda-connect/04-HTTP-INPUTS.md) | Webhook receivers, JWT/signature validation, API polling |
| [SQL Patterns](_docs/redpanda-connect/05-SQL-PATTERNS.md) | PostgreSQL integration, UPSERT, batching, connection pools |
| [Error Handling](_docs/redpanda-connect/06-ERROR-HANDLING.md) | try/catch, DLQ, fallback outputs, error routing |
| [Pipeline Patterns](_docs/redpanda-connect/07-PIPELINE-PATTERNS.md) | Complete CDC pipeline examples, multi-input/output |
| [**Pipeline Templating**](_docs/redpanda-connect/08-PIPELINE-TEMPLATING.md) | **Template structure, .blobl files, generation flow** |

---

## 📚 Additional Reference

- **Architecture details:** `_docs/ARCHITECTURE.md`
- **Pattern examples:** `examples/db-per-tenant/`, `examples/db-shared/`
- **API documentation:** `_docs/`
- **Implementation guides:** Implementation repos' copilot-instructions
