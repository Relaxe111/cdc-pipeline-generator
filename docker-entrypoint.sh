#!/usr/bin/fish
# Docker entrypoint for cdc-pipeline-generator dev container
# Ensures cdc command is always available with latest code changes

# Install/reinstall the cdc-pipeline-generator package in editable mode
# This ensures any code changes are immediately reflected
cd /workspace
pip install -e . --quiet 2>/dev/null

# Execute the main command (usually tail -f /dev/null to keep container running)
exec $argv
