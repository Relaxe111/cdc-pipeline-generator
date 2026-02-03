# =============================================================================
# Multi-stage build for CDC Pipeline Generator
# Stage 1: Build dependencies with compilation tools
# Stage 2: Runtime with only necessary components
# =============================================================================

# Build stage - compile Python packages
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    freetds-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy package files
COPY pyproject.toml setup.py MANIFEST.in README.md LICENSE ./
COPY cdc_generator ./cdc_generator

# Build wheel
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

# =============================================================================
# Runtime stage - minimal size with all functionality
# =============================================================================
FROM python:3.11-slim

# Install only runtime dependencies (no build tools)
RUN apt-get update && apt-get install -y \
    git \
    fish \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install from wheel (no compilation needed)
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl \
    && rm -rf /wheels

# Set up Fish completions (no fisher needed - Fish auto-loads from this directory)
RUN mkdir -p /root/.config/fish/completions
COPY cdc_generator/templates/init/cdc.fish /root/.config/fish/completions/cdc.fish

# Set working directory for user projects
WORKDIR /workspace

# Use ENTRYPOINT for cdc command, but allow shell override
ENTRYPOINT ["cdc"]
CMD ["--help"]
