# Docker Version Sync Fix

## Problem
The Docker container was stuck on version 0.1.3 even when the host had version 0.1.12 installed via pipx.

## Root Cause
The Dockerfile was copying `cdc_generator/` and running `pip install -e .` during **image build time**, which baked the old version into the Docker image layer. Even though the code was volume-mounted, the package metadata was cached in the image.

## Solution
1. **Dockerfile.dev**: Now only installs dependencies during build, NOT the package itself
2. **docker-entrypoint.sh**: Installs the package from volume-mounted `/workspace` at runtime
3. This ensures the Docker container always uses the current version from your workspace

## How to Apply the Fix

### Step 1: Rebuild the Docker Image
```fish
# From the host (NOT inside container)
cd /Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator
docker compose build dev
```

### Step 2: Restart the Container
```fish
docker compose down
docker compose up -d
```

### Step 3: Verify Version Inside Container
```fish
# Enter the container
docker compose exec dev fish

# Check version
pip show cdc-pipeline-generator | grep "^Version:"
# Should show: Version: 0.1.12 (or whatever is in pyproject.toml)

# Also check cdc command
cdc --version
```

## Future Version Updates

After changing the version in `pyproject.toml`:

1. **You do NOT need to rebuild** - the entrypoint script will pick up the new version automatically
2. Just restart the container: `docker compose restart dev`
3. The version in Docker will match your `pyproject.toml`

## Why This Works

- Volume mount: `-  .:/workspace` ensures code changes are instantly available
- Editable install: `pip install -e .` at runtime reads from the mounted workspace
- No build cache: Dependencies are cached in image, but package version comes from runtime

