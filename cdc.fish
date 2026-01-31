# Fish shell completions for cdc command
# Auto-generated completions for CDC Pipeline CLI

# Main command description
complete -c cdc -f -d "CDC Pipeline Management CLI"

# Generator commands (work from generator or implementation)
complete -c cdc -n "__fish_use_subcommand" -a "validate" -d "Validate all customer configurations"
complete -c cdc -n "__fish_use_subcommand" -a "manage-service" -d "Manage service definitions"
complete -c cdc -n "__fish_use_subcommand" -a "manage-server-group" -d "Manage server groups"
complete -c cdc -n "__fish_use_subcommand" -a "generate" -d "Generate pipelines"

# Local commands (implementation-specific)
complete -c cdc -n "__fish_use_subcommand" -a "setup-local" -d "Set up local development environment"
complete -c cdc -n "__fish_use_subcommand" -a "enable" -d "Enable CDC on MSSQL tables"
complete -c cdc -n "__fish_use_subcommand" -a "migrate-replica" -d "Apply PostgreSQL migrations to replica"
complete -c cdc -n "__fish_use_subcommand" -a "verify" -d "Verify pipeline connections"
complete -c cdc -n "__fish_use_subcommand" -a "verify-sync" -d "Verify CDC synchronization and detect gaps"
complete -c cdc -n "__fish_use_subcommand" -a "stress-test" -d "CDC stress test with real-time monitoring"
complete -c cdc -n "__fish_use_subcommand" -a "reset-local" -d "Reset local environment"
complete -c cdc -n "__fish_use_subcommand" -a "nuke-local" -d "Complete cleanup of local environment"
complete -c cdc -n "__fish_use_subcommand" -a "clean" -d "Clean CDC change tracking tables"
complete -c cdc -n "__fish_use_subcommand" -a "schema-docs" -d "Generate database schema documentation"
complete -c cdc -n "__fish_use_subcommand" -a "reload-pipelines" -d "Regenerate and reload Redpanda Connect pipelines"
complete -c cdc -n "__fish_use_subcommand" -a "help" -d "Show help message"

# Common flags
complete -c cdc -l help -s h -d "Show help message"
complete -c cdc -l version -s v -d "Show version"

# manage-service subcommand options
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add -d "Add a new service"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l edit -d "Edit an existing service"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l delete -d "Delete a service"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l list -d "List all services"

# manage-server-group subcommand options
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l add -d "Add a new server group"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l edit -d "Edit an existing server group"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l delete -d "Delete a server group"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l list -d "List all server groups"

# generate subcommand - complete with customer names dynamically
complete -c cdc -n "__fish_seen_subcommand_from generate" -l all -d "Generate for all customers"
complete -c cdc -n "__fish_seen_subcommand_from generate" -l force -d "Force regeneration"

# verify-sync options
complete -c cdc -n "__fish_seen_subcommand_from verify-sync" -l customer -d "Specify customer"
complete -c cdc -n "__fish_seen_subcommand_from verify-sync" -l service -d "Specify service"
complete -c cdc -n "__fish_seen_subcommand_from verify-sync" -l table -d "Specify table"
complete -c cdc -n "__fish_seen_subcommand_from verify-sync" -l all -d "Check all tables"

# stress-test options
complete -c cdc -n "__fish_seen_subcommand_from stress-test" -l customer -d "Specify customer"
complete -c cdc -n "__fish_seen_subcommand_from stress-test" -l service -d "Specify service"
complete -c cdc -n "__fish_seen_subcommand_from stress-test" -l duration -d "Test duration in seconds"
complete -c cdc -n "__fish_seen_subcommand_from stress-test" -l interval -d "Monitoring interval in seconds"
