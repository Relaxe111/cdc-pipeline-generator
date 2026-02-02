#!/usr/bin/fish
# Docker entrypoint for cdc-pipeline-generator dev container
# Ensures cdc command is always available with latest code changes

# Install/reinstall the cdc-pipeline-generator package in editable mode
# This ensures any code changes are immediately reflected and version is current
cd /workspace
echo "Installing cdc-pipeline-generator from /workspace..."
pip install -e . --quiet

# Show installed version for verification
echo "Installed version:"
pip show cdc-pipeline-generator | grep "^Version:"

# Execute the main command (usually tail -f /dev/null to keep container running)
exec $argv
