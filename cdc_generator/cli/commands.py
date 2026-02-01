#!/usr/bin/env python3
"""
CDC Pipeline CLI - Main Entry Point

This CLI can run in two modes:
1. Inside dev container (/workspace): Uses absolute paths to generator and implementations
2. In implementation directory (adopus-cdc-pipeline, asma-cdc-pipeline): Auto-detects context

Usage:
    cdc <command> [options]

Commands:
    init                  Initialize a new CDC Pipeline project with dev container
    validate              Validate all customer configurations  
    manage-service        Manage service definitions (from generator)
    manage-server-group   Manage server groups (from generator)
    generate [customer]   Generate pipelines (from generator)
    setup-local          Set up local development environment
    enable <customer> <env>      Enable CDC on MSSQL tables
    migrate-replica      Apply PostgreSQL migrations to replica
    verify <customer> <env>      Verify pipeline connections
    verify-sync [options]        Verify CDC synchronization and detect gaps
    stress-test [options]        CDC stress test with real-time monitoring
    reset-local          Reset local environment
    nuke-local           Complete cleanup of local environment
    clean                Clean CDC change tracking tables
    schema-docs          Generate database schema documentation
    reload-pipelines     Regenerate and reload Redpanda Connect pipelines
    help                 Show this help message
"""

import sys
import subprocess
from pathlib import Path
import os

def detect_environment():
    """
    Detect the current working environment.
    
    Returns:
        tuple: (workspace_root, implementation_name, is_dev_container)
            - workspace_root: Path to workspace root (generator or implementation)
            - implementation_name: 'adopus' or 'asma' or None
            - is_dev_container: True if running inside dev container
    """
    cwd = Path.cwd()
    
    # Check if we're in the dev container
    if cwd.parts[:2] == ('/', 'workspace'):
        # In dev container at /workspace (generator root)
        return Path('/workspace'), None, True
    elif cwd.parts[:3] == ('/', 'implementations', 'adopus'):
        # In dev container at /implementations/adopus
        return Path('/implementations/adopus'), 'adopus', True
    elif cwd.parts[:3] == ('/', 'implementations', 'asma'):
        # In dev container at /implementations/asma
        return Path('/implementations/asma'), 'asma', True
    
    # Check if we're in an implementation directory on host
    # Look for server_group.yaml as indicator
    if (cwd / 'server_group.yaml').exists():
        # We're in an implementation root
        # Try to detect which one from path
        if 'adopus-cdc-pipeline' in str(cwd):
            return cwd, 'adopus', False
        elif 'asma-cdc-pipeline' in str(cwd):
            return cwd, 'asma', False
        else:
            # Unknown implementation, use current dir
            return cwd, None, False
    
    # Try to find implementation root by walking up
    current = cwd
    while current != current.parent:
        if (current / 'server_group.yaml').exists():
            impl_name = None
            if 'adopus-cdc-pipeline' in str(current):
                impl_name = 'adopus'
            elif 'asma-cdc-pipeline' in str(current):
                impl_name = 'asma'
            return current, impl_name, False
        current = current.parent
    
    # Fallback: assume we're in current directory
    return cwd, None, False


def get_script_paths(workspace_root, is_dev_container):
    """
    Get paths to scripts based on environment.
    
    Args:
        workspace_root: Path to workspace root
        is_dev_container: Whether running in dev container
    
    Returns:
        dict: Mapping of script types to paths
    """
    if is_dev_container:
        # In dev container
        generator_root = Path('/workspace')
        return {
            'generator': generator_root / 'cdc_generator',
            'scripts': workspace_root / 'scripts',
            'root': workspace_root,
        }
    else:
        # On host - try to find generator
        # Look for generator as sibling directory
        parent = workspace_root.parent
        generator_candidate = parent / 'cdc-pipeline-generator'
        
        if generator_candidate.exists():
            return {
                'generator': generator_candidate / 'cdc_generator',
                'scripts': workspace_root / 'scripts',
                'root': workspace_root,
            }
        else:
            # Fallback: assume generator is installed as package
            # Scripts will be run via python -m
            return {
                'generator': None,  # Use installed package
                'scripts': workspace_root / 'scripts',
                'root': workspace_root,
            }


# Commands that use generator library
GENERATOR_COMMANDS = {
    "init": {
        "module": "cdc_generator.cli.init_project",
        "script": "cli/init_project.py",
        "description": "Initialize a new CDC pipeline project"
    },
    "generate": {
        "module": "cdc_generator.core.pipeline_generator",
        "script": "core/pipeline_generator.py",
        "description": "Generate Redpanda Connect pipelines"
    },
    "manage-service": {
        "module": "cdc_generator.cli.service",
        "script": "cli/service.py",
        "description": "Manage CDC service definitions"
    },
    "manage-server-group": {
        "module": "cdc_generator.cli.server_group",
        "script": "cli/server_group.py",
        "description": "Manage server groups"
    },
}

# Commands that use local scripts (implementation-specific)
LOCAL_COMMANDS = {
    "validate": {
        "script": "scripts/1-validate-customers.py",
        "description": "Validate all customer YAML configurations"
    },
    "setup-local": {
        "script": "scripts/4-setup-local.py",
        "description": "Set up local development environment"
    },
    "enable": {
        "script": "scripts/5-enable-cdc-mssql.py",
        "description": "Enable CDC on MSSQL tables",
        "usage": "cdc enable <customer> <env>"
    },
    "migrate-replica": {
        "script": "scripts/10-migrate-replica.py",
        "description": "Apply PostgreSQL migrations to replica databases",
        "usage": "cdc migrate-replica <customer> --env <env>"
    },
    "verify": {
        "script": "scripts/6-verify-pipeline.py",
        "description": "Verify pipeline connections",
        "usage": "cdc verify <customer> <env>"
    },
    "reset-local": {
        "script": "scripts/7-reset-local.py",
        "description": "Reset local development environment"
    },
    "nuke-local": {
        "script": "scripts/8-nuke-local.py",
        "description": "Complete cleanup of local environment"
    },
    "clean": {
        "script": "scripts/9-clean-cdc-tables.py",
        "description": "Clean CDC change tracking tables",
        "usage": "cdc clean --env <env> [--table <table>] [--all]"
    },
    "verify-sync": {
        "script": "scripts/13-verify-cdc-sync.py",
        "description": "Verify CDC synchronization and detect gaps",
        "usage": "cdc verify-sync [--customer <name>] [--env <env>] [--table <table>] [--fix]"
    },
    "stress-test": {
        "script": "scripts/7-stress-test.py",
        "description": "CDC stress test with real-time monitoring",
        "usage": "cdc stress-test --env <env> [customer...] [--tables <table...>] [--records N]"
    },
    "schema-docs": {
        "script": "generate_schema_docs.py",
        "description": "Generate database schema documentation YAML files"
    },
    "reload-pipelines": {
        "script": "scripts/9-reload-pipelines.py",
        "description": "Regenerate and reload Redpanda Connect pipelines",
        "usage": "cdc reload-pipelines [customer...]"
    },
}


def print_help(workspace_root, implementation_name, is_dev_container):
    """Print help message with all available commands."""
    print(__doc__)
    
    # Show environment info
    if is_dev_container:
        if implementation_name:
            print(f"📍 Environment: Dev container - /implementations/{implementation_name}")
        else:
            print(f"📍 Environment: Dev container - /workspace (generator)")
    else:
        print(f"📍 Environment: Host - {workspace_root}")
        if implementation_name:
            print(f"   Implementation: {implementation_name}")
    
    print("\n📦 Commands using generator library:")
    for cmd, info in GENERATOR_COMMANDS.items():
        print(f"  {cmd:20} - {info['description']}")
    
    print("\n🔧 Commands using local scripts:")
    for cmd, info in LOCAL_COMMANDS.items():
        desc = info['description']
        if 'usage' in info:
            desc += f"\n  {' ' * 20}   Usage: {info['usage']}"
        print(f"  {cmd:20} - {desc}")
    
    print("\n💡 Tip: Run commands from implementation directory or dev container")


def run_generator_command(command, paths, extra_args, workspace_root):
    """
    Run a command from the generator library.
    
    Args:
        command: Command name
        paths: Dictionary of script paths
        extra_args: Additional command-line arguments
        workspace_root: Current workspace root
    
    Returns:
        int: Exit code
    """
    cmd_info = GENERATOR_COMMANDS[command]
    
    if paths['generator'] is None:
        # Use installed package
        cmd = ["python3", "-m", cmd_info['module']] + extra_args
    else:
        # Use local generator files
        script_path = paths['generator'] / cmd_info['script']
        if not script_path.exists():
            print(f"❌ Error: Generator script not found: {script_path}")
            print(f"   Make sure cdc-pipeline-generator is properly set up.")
            return 1
        cmd = ["python3", str(script_path)] + extra_args
    
    # Execute from implementation workspace root
    try:
        result = subprocess.run(cmd, cwd=workspace_root)
        return result.returncode
    except Exception as e:
        print(f"❌ Error running {command}: {e}")
        return 1


def run_local_command(command, paths, extra_args, workspace_root):
    """
    Run a command from local scripts directory.
    
    Args:
        command: Command name
        paths: Dictionary of script paths
        extra_args: Additional command-line arguments
        workspace_root: Current workspace root
    
    Returns:
        int: Exit code
    """
    cmd_info = LOCAL_COMMANDS[command]
    script_path = workspace_root / cmd_info['script']
    
    if not script_path.exists():
        print(f"❌ Error: Script not found: {script_path}")
        print(f"   This command requires an implementation workspace.")
        return 1
    
    cmd = ["python3", str(script_path)] + extra_args
    
    try:
        result = subprocess.run(cmd, cwd=workspace_root)
        return result.returncode
    except Exception as e:
        print(f"❌ Error running {command}: {e}")
        return 1


def main():
    """Main CLI entry point."""
    # Show help if requested or no command given
    if len(sys.argv) < 2 or sys.argv[1] in ["help", "--help", "-h"]:
        workspace_root, implementation_name, is_dev_container = detect_environment()
        print_help(workspace_root, implementation_name, is_dev_container)
        return 0
    
    command = sys.argv[1]
    extra_args = sys.argv[2:] if len(sys.argv) > 2 else []
    
    # Special handling for 'init' - runs anywhere without environment detection
    if command == "init":
        from cdc_generator.cli.init_project import init_project
        return init_project(extra_args)
    
    # For other commands, detect environment
    workspace_root, implementation_name, is_dev_container = detect_environment()
    
    # Get script paths
    paths = get_script_paths(workspace_root, is_dev_container)
    
    # Execute command
    if command in GENERATOR_COMMANDS:
        return run_generator_command(command, paths, extra_args, workspace_root)
    elif command in LOCAL_COMMANDS:
        return run_local_command(command, paths, extra_args, workspace_root)
    else:
        print(f"❌ Unknown command: {command}")
        print(f"\nRun 'cdc help' to see available commands.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
