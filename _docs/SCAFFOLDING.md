# Project Scaffolding

The CDC Pipeline Generator includes automatic project scaffolding to create a complete directory structure and template files for new implementations.

## Features

When you create a new server group with `--create`, the generator automatically scaffolds:

### Directory Structure

```
.
├── services/              # Service YAML configurations
├── pipeline-templates/    # Redpanda Connect pipeline templates
├── scripts/                 # Helper scripts
│   ├── postgres-init/      # PostgreSQL initialization scripts
│   └── mssql-init/         # MSSQL initialization scripts (if using MSSQL)
├── generated/              # Auto-generated files
│   ├── pipelines/          # Redpanda Connect pipelines
│   ├── schemas/            # Database schemas
│   ├── table-definitions/  # Table definitions
│   └── pg-migrations/      # PostgreSQL migrations
├── docs/                   # Documentation
├── argocd/                 # ArgoCD configuration
├── kubernetes/             # Kubernetes manifests
│   ├── base/
│   └── overlays/
│       ├── nonprod/
│       └── prod/
├── .vscode/                # VS Code settings
├── service-schemas/        # Service schema definitions
└── source-groups.yaml       # Server group configuration
```

### Generated Files

1. **docker-compose.yml**
   - PostgreSQL (target database)
   - Redpanda (Kafka-compatible streaming)
   - Redpanda Console (monitoring UI)
   - Adminer (database UI)
   - Optional local MSSQL server
   - Uses server_group name in container/image naming

2. **.env.example**
   - Template for environment variables
   - Source database configuration
   - Target database configuration
   - Redpanda/Kafka configuration
   - Customized for your source type (MSSQL/PostgreSQL)

3. **README.md**
   - Quick start guide
   - Pattern description (db-per-tenant or db-shared)
   - Monitoring access information
   - Directory structure overview

4. **.gitignore**
   - Python cache files
   - Generated files
   - .env file
   - Common IDE files

5. **.vscode/settings.json**
   - YAML file associations
   - Schema validation
   - File/search exclusions

6. **Pipeline templates**
   - Source pipeline template stub
   - Sink pipeline template stub

7. **Helper scripts**
   - Fish shell completions script
   - .gitkeep files for generated directories

## Usage

### Creating a New Implementation

```bash
# From the cdc-pipeline-generator dev container
cd /workspace

# For db-per-tenant pattern (e.g., adopus)
cdc manage-source-groups \
  --create adopus \
  --pattern db-per-tenant \
  --source-type mssql \
  --extraction-pattern '^AdOpus(?P<customer>.+)$' \
  --host '${MSSQL_SOURCE_HOST}' \
  --port '${MSSQL_SOURCE_PORT}' \
  --user '${MSSQL_SOURCE_USER}' \
  --password '${MSSQL_SOURCE_PASSWORD}'

# For db-shared pattern (e.g., asma)
cdc manage-source-groups \
  --create asma \
  --pattern db-shared \
  --source-type postgres \
  --environment-aware \
  --extraction-pattern '^(?P<service>[a-z_]+?)_db_(?P<env>dev|stage|prod)(?:_(?P<suffix>[a-z]+))?$' \
  --host '${POSTGRES_SOURCE_HOST}' \
  --port '${POSTGRES_SOURCE_PORT}' \
  --user '${POSTGRES_SOURCE_USER}' \
  --password '${POSTGRES_SOURCE_PASSWORD}'
```

### What Gets Created

The generator will:

1. ✅ Create `source-groups.yaml` with your configuration
2. ✅ Scaffold complete directory structure
3. ✅ Generate `docker-compose.yml` with server_group-specific naming
4. ✅ Create `.env.example` template
5. ✅ Generate `README.md` with quick start guide
6. ✅ Create `.gitignore` and `.vscode/settings.json`
7. ✅ Add pipeline template stubs
8. ✅ Create helper scripts

### Next Steps After Scaffolding

```bash
# 1. Copy environment template
cp .env.example .env

# 2. Edit .env with your actual database credentials
nano .env

# 3. Update server group from database
cdc manage-source-groups --update

# 4. Start infrastructure
docker compose up -d

# 5. Create service configurations
cdc manage-service --service <service-name> --create-service

# 6. Generate pipelines
cdc generate
```

## Automatic Directory Creation

The `--update` command also ensures critical directories exist:

```bash
cdc manage-source-groups --update
```

If directories like `services/`, `scripts/`, or `generated/` are missing, they'll be created automatically with a friendly notification.

## Docker Container Naming

The scaffolding uses your server_group name to create unique container and image names:

**Server group: `adopus`**
```yaml
# Containers
- adopus-postgres (adopus-replica)
- adopus-redpanda
- adopus-console
- adopus-adminer
- adopus-source
- adopus-sink
- adopus-mssql (optional)

# Network
- adopus-network

# Volumes
- adopus-postgres-data
- adopus-mssql-data
```

**Server group: `asma`**
```yaml
# Containers
- asma-postgres (asma-replica)
- asma-redpanda
- asma-console
# ... etc
```

This allows multiple implementations to run simultaneously without conflicts.

## Customization

### Modifying Templates

All template generation is in:
```
cdc-pipeline-generator/cdc_generator/validators/manage_server_group/scaffolding.py
```

You can customize:
- `_get_docker_compose_template()` - Docker Compose structure
- `_get_env_example_template()` - Environment variables
- `_get_readme_template()` - README content
- `_create_vscode_settings()` - VS Code configuration

### Skipping Existing Files

The scaffolding **never overwrites existing files**. If a file already exists, it's skipped with a message:
```
⊘ Skipped (exists): docker-compose.yml
```

## Troubleshooting

### Missing Directories Warning

If you run `--update` and see:
```
⚠️  Missing core files detected. Consider running scaffolding:
   cdc manage-source-groups --create ...
```

This means you're missing `.env.example` or other core files. Run the suggested `--create` command to scaffold everything.

### Permission Issues

If scaffolding fails with permission errors, ensure you're running inside the dev container:
```bash
docker compose exec dev fish
cd /workspace
```

### Existing Implementation

If you have an existing implementation without scaffolding, you can:

1. **Manually add missing files** - Copy from examples
2. **Re-run --create** - It will only create missing files
3. **Create directories manually** - They'll be used automatically

## Pattern-Specific Templates

### db-per-tenant (adopus)

- Expects MSSQL source (configurable)
- Each customer has own database
- Service name at server group level
- README emphasizes multi-tenancy

### db-shared (asma)

- Expects PostgreSQL source (configurable)
- Multiple services share databases
- Environment-aware grouping
- README emphasizes environment separation

## See Also

- [Server Group Management](SERVER_GROUP.md)
- [Service Management](SERVICE.md)
- [Development Container](../DEV_CONTAINER.md)
- [Project Structure](../PROJECT_STRUCTURE.md)
