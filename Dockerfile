FROM python:3.11-slim

# Set working directory
WORKDIR /generator

# Install system dependencies including Fish shell
RUN apt-get update && apt-get install -y \
    git \
    fish \
    && rm -rf /var/lib/apt/lists/*

# Copy package files
COPY pyproject.toml setup.py MANIFEST.in README.md LICENSE ./
COPY cdc_generator ./cdc_generator

# Install the package
RUN pip install --no-cache-dir .

# Set entry point
ENTRYPOINT ["cdc"]
CMD ["--help"]
