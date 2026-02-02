FROM python:3.11-slim

# Set working directory
WORKDIR /generator

# Install system dependencies (Fish, database clients, build tools)
RUN apt-get update && apt-get install -y \
    git \
    fish \
    curl \
    postgresql-client \
    freetds-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy package files
COPY pyproject.toml setup.py MANIFEST.in README.md LICENSE ./
COPY cdc_generator ./cdc_generator

# Install the package
RUN pip install --no-cache-dir .

# Set up Fish shell with completions
RUN fish -c "curl -sL https://raw.githubusercontent.com/jorgebucaran/fisher/main/functions/fisher.fish | source && fisher install jorgebucaran/fisher" \
    && mkdir -p /root/.config/fish/completions

# Copy Fish completions for cdc command
COPY cdc_generator/templates/init/cdc.fish /root/.config/fish/completions/cdc.fish

# Set working directory for user projects
WORKDIR /workspace

# Default to Fish shell for interactive use, but keep cdc as entrypoint for one-off commands
ENTRYPOINT ["cdc"]
CMD ["--help"]
