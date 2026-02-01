# CDC Pipeline Project Initialization Guide

## Overview

This guide covers how to initialize new CDC pipeline projects and how the codebase works in both **development** and **production** environments.

## Installation Methods

### Option 1: pipx (Recommended for Production)

`pipx` is like `npx` for Python - it installs tools in isolated environments.

```bash
# Install pipx if you don't have it
python3 -m pip install --user pipx
python3 -m pipx ensurepath

# Install cdc-pipeline-generator globally
pipx install cdc-pipeline-generator

# Or install from Git (development/testing)
pipx install git+https://github.com/yourorg/cdc-pipeline-generator.git

# Or install from private PyPI
pipx install --index-url https://your-pypi.org/simple cdc-pipeline-generator
```

### Option 2: pip (Traditional)

```bash
# Install globally or in a virtual environment
pip install cdc-pipeline-generator

# Or from Git
pip install git+https://github.com/yourorg/cdc-pipeline-generator.git
```

### Option 3: Direct from Repository (Development)

```bash
git clone https://github.com/yourorg/cdc-pipeline-generator.git
cd cdc-pipeline-generator
pip install -e .  # Editable install
```

## Initializing a New Project

### Quick Start

```bash
# Create project directory
mkdir my-cdc-pipeline
cd my-cdc-pipeline

# Initialize project
cdc init --name my-project --type adopus --git-init

# What gets created:
# ├── 2-services/              # Service configurations
# ├── 2-customers/             # Customer configurations (legacy)
# ├── 3-pipeline-templates/    # Custom pipeline templates
# ├── generated/               # Generated pipelines and schemas
# │   ├── pipelines/
# │   ├── schemas/
# │   ├── table-definitions/
# │   └── pg-migrations/
# ├── service-schemas/         # Saved table schemas
# ├── kubernetes/              # K8s manifests
# │   ├── base/
# │   └── overlays/
# ├── argocd/                  # ArgoCD configurations
# ├── docs/                    # Documentation
# ├── scripts/                 # Custom scripts
# ├── server-groups.yaml       # Server group definitions
# ├── docker-compose.yml       # Development environment
# ├── Dockerfile.dev           # Development container
# ├── .env.example             # Environment variables template
# ├── .gitignore               # Git ignore rules
# └── README.md                # Project documentation
```

### Project Types

**Adopus Type (db-per-tenant)**
```bash
cdc init --name adopus-cdc --type adopus
```
- Each customer has their own database
- CDC pipelines are customer-specific
- `server_group_type: db-per-tenant`

**Asma Type (db-shared)**
```bash
cdc init --name asma-cdc --type asma
```
- All customers share the same database
- CDC pipelines use shared configuration
- `server_group_type: db-shared`

### Options

```bash
cdc init --name PROJECT_NAME \
         --type {adopus|asma} \
         --target-dir /path/to/dir \
         --git-init
```

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `--name` | Project name | Yes | - |
| `--type` | Implementation type (adopus/asma) | Yes | - |
| `--target-dir` | Target directory | No | Current directory |
| `--git-init` | Initialize git repository | No | false |

## Path Resolution: Development vs Production

The codebase automatically detects its environment and adapts path resolution accordingly.

### How It Works

```python
def get_implementation_root() -> Path:
    """Search upward from CWD for implementation markers."""
    current = Path.cwd()
    
    # Search upwards for 2-services/ directory
    for parent in [current] + list(current.parents):
        if (parent / "2-services").exists():
            return parent
    
    # Fallback to current directory
    return current
```

### Development Environment

**Structure:**
```
cdc-pipelines-development/
├── cdc-pipeline-generator/          # Generator library
│   └── cdc_generator/
└── adopus-cdc-pipeline/              # Implementation
    ├── 2-services/
    ├── server-groups.yaml
    └── ...
```

**How it works:**
1. You run commands FROM the implementation directory: `cd /implementations/adopus`
2. Code searches upward from CWD, finds `2-services/` in current directory
3. Uses that as project root

**Example:**
```bash
# In dev container
cd /implementations/adopus
cdc manage-service --service proxy --add-source-table public.users
# ✓ Finds: /implementations/adopus/2-services/proxy.yaml
```

### Production Environment

**Structure:**
```
my-cdc-pipeline/                      # User's project
├── 2-services/
├── server-groups.yaml
└── ...
# cdc-pipeline-generator installed as library via pip/pipx
```

**How it works:**
1. Generator is installed as Python package
2. User runs commands FROM their project directory
3. Code searches upward from CWD, finds `2-services/` in current directory
4. Uses that as project root

**Example:**
```bash
cd ~/my-cdc-pipeline
cdc manage-service --service myservice --add-source-table dbo.Users
# ✓ Finds: ~/my-cdc-pipeline/2-services/myservice.yaml
```

### Why This Works

The key insight: **Commands always run FROM the implementation directory**, regardless of whether it's development or production.

**Development:**
- Generator is in `/workspace/cdc-pipeline-generator`
- Implementation is in `/implementations/adopus`
- You `cd /implementations/adopus` before running commands
- CWD = `/implementations/adopus`, finds `2-services/` there ✓

**Production:**
- Generator is installed as package (anywhere Python can find it)
- Implementation is in `~/my-project`
- You `cd ~/my-project` before running commands
- CWD = `~/my-project`, finds `2-services/` there ✓

## After Initialization

### 1. Configure Environment

```bash
# Copy and edit environment variables
cp .env.example .env
nano .env

# Configure your database credentials
SOURCE_DB_HOST=your-mssql-server
SOURCE_DB_PORT=1433
SOURCE_DB_NAME=YourDatabase
# ... etc
```

### 2. Start Development Container

```bash
# Start container
docker compose up -d

# Enter container
docker compose exec dev fish
```

### 3. Create Your First Service

```bash
# Inside container
cdc manage-service --service myservice --create-service

# Add tables
cdc manage-service --service myservice --add-source-table dbo.Users
cdc manage-service --service myservice --add-source-table dbo.Orders

# Generate pipelines
cdc generate
```

### 4. Inspect Available Tables

```bash
# Inspect all schemas
cdc manage-service --service myservice --inspect --all

# Inspect specific schema
cdc manage-service --service myservice --inspect --schema dbo

# Save detailed schemas
cdc manage-service --service myservice --inspect --schema dbo --save
```

## Publishing to PyPI

### For Internal Use (Private PyPI)

```bash
# Build package
python -m build

# Upload to private PyPI
twine upload --repository-url https://your-pypi.org/simple dist/*
```

### For Public Use (PyPI)

```bash
# Build package
python -m build

# Upload to PyPI
twine upload dist/*
```

## Best Practices

### 1. Version Pinning

**In production:**
```bash
pipx install cdc-pipeline-generator==0.1.0
```

**In requirements.txt:**
```
cdc-pipeline-generator==0.1.0
```

### 2. Environment Variables

- Always use `.env` files, never commit credentials
- Use different `.env` files for different environments
- Template: `.env.example` in repo, actual `.env` in `.gitignore`

### 3. Project Structure

- Keep `2-services/` for service definitions
- Keep `service-schemas/` for saved table schemas (autocomplete source)
- Keep `generated/` in `.gitignore` (regenerated on each build)
- Keep `kubernetes/` for deployment manifests

### 4. Development Workflow

**In development (multi-workspace):**
```bash
# Work from implementation directory
cd /implementations/adopus

# Edit generator code
vim /workspace/cdc_generator/...

# Changes take effect immediately (editable install)
cdc manage-service --service proxy --add-source-table public.users
```

**In production (installed package):**
```bash
# Work from project directory
cd ~/my-cdc-pipeline

# Use installed generator
cdc manage-service --service myservice --add-source-table dbo.Users
```

## Troubleshooting

### "Service config not found"

**Problem:** Command can't find `2-services/` directory

**Solution:**
```bash
# Make sure you're in the project root
cd /path/to/your/project
pwd  # Should show directory containing 2-services/

# Or check if 2-services/ exists
ls -la 2-services/
```

### "Command not found: cdc"

**Problem:** Generator not installed or not in PATH

**Solution:**
```bash
# Check installation
which cdc

# If not found, reinstall
pipx install cdc-pipeline-generator

# Or ensure PATH includes pipx binaries
pipx ensurepath
```

### Autocomplete not working

**Problem:** Fish completions not loaded

**Solution:**
```bash
# Reload Fish completions
fish -c "fish_update_completions"

# Or restart Fish shell
exec fish
```

## Summary

| Aspect | Development | Production |
|--------|-------------|------------|
| **Generator Location** | `/workspace/cdc-pipeline-generator` | Installed via pip/pipx |
| **Implementation Location** | `/implementations/{adopus,asma}` | User's project directory |
| **Working Directory** | `/implementations/adopus` | `~/my-project` |
| **Path Resolution** | Search upward from CWD | Search upward from CWD |
| **Installation** | `pip install -e .` | `pipx install cdc-pipeline-generator` |
| **Initialization** | Manual (already exists) | `cdc init --name ... --type ...` |

Both environments use the **same path resolution logic**, making the code portable and predictable.
