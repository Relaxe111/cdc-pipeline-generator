# Fish shell completions for cdc command
# Auto-generated completions for CDC Pipeline CLI

# Fish shell completions for cdc command
# Auto-generated completions for CDC Pipeline CLI
# Note: Flag validation is handled by Python. Fish completions show all available options.

function __cdc_last_token_is --description "Check if last token equals given value"
    set -l expected $argv[1]
    set -l tokens (commandline -opc)
    
    if test (count $tokens) -gt 0
        test "$tokens[-1]" = "$expected"
        return $status
    end
    return 1
end

# Main command description
complete -c cdc -f -d "CDC Pipeline Management CLI"

# Generator commands (work from generator or implementation)
complete -c cdc -n "__fish_use_subcommand" -a "init" -d "Initialize a new CDC pipeline project"
complete -c cdc -n "__fish_use_subcommand" -a "scaffold" -d "Scaffold a new CDC pipeline project"
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
complete -c cdc -n "__fish_use_subcommand" -a "reload-cdc-autocompletions" -d "Reload Fish shell completions after modifying cdc.fish"
complete -c cdc -n "__fish_use_subcommand" -a "help" -d "Show help message"

# Global flags (only show when no subcommand is active)
complete -c cdc -n "__fish_use_subcommand" -l help -s h -d "Show help message"
complete -c cdc -n "__fish_use_subcommand" -l version -s v -d "Show version"

# Subcommand-specific help (show -h/--help for each subcommand)
complete -c cdc -n "__fish_seen_subcommand_from scaffold" -s h -l help -d "Show scaffold help"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -s h -l help -d "Show manage-service help"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -s h -l help -d "Show manage-server-group help"
complete -c cdc -n "__fish_seen_subcommand_from generate" -s h -l help -d "Show generate help"
complete -c cdc -n "__fish_seen_subcommand_from validate" -s h -l help -d "Show validate help"
complete -c cdc -n "__fish_seen_subcommand_from setup-local" -s h -l help -d "Show setup-local help"

# init subcommand options
complete -c cdc -n "__fish_seen_subcommand_from init" -l name -d "Project name (e.g., adopus-cdc-pipeline)" -r
complete -c cdc -n "__fish_seen_subcommand_from init" -l type -d "Implementation type" -r -f -a "adopus asma"
complete -c cdc -n "__fish_seen_subcommand_from init" -l target-dir -d "Target directory (default: current)" -r -F
complete -c cdc -n "__fish_seen_subcommand_from init" -l git-init -d "Initialize git repository"

# scaffold subcommand options (all flags shown, validation in Python)
complete -c cdc -n "__fish_seen_subcommand_from scaffold" -l update -d "Update existing project scaffold with latest structure"
complete -c cdc -n "__fish_seen_subcommand_from scaffold" -l pattern -d "Server group pattern (db-per-tenant|db-shared)" -r
complete -c cdc -n "__fish_seen_subcommand_from scaffold; and __cdc_last_token_is --pattern" -f -a "db-per-tenant db-shared"
complete -c cdc -n "__fish_seen_subcommand_from scaffold" -l source-type -d "Source database type (postgres|mssql)" -r
complete -c cdc -n "__fish_seen_subcommand_from scaffold; and __cdc_last_token_is --source-type" -f -a "postgres mssql"
complete -c cdc -n "__fish_seen_subcommand_from scaffold" -l extraction-pattern -d "Regex pattern with named groups (empty string for fallback)" -r
complete -c cdc -n "__fish_seen_subcommand_from scaffold" -l environment-aware -d "Enable environment-aware grouping (required for db-shared)"
complete -c cdc -n "__fish_seen_subcommand_from scaffold" -l kafka-topology -d "Kafka topology (shared or per-server)" -r
complete -c cdc -n "__fish_seen_subcommand_from scaffold" -l host -d "Database host (use \${VAR} for env vars)" -r
complete -c cdc -n "__fish_seen_subcommand_from scaffold" -l port -d "Database port" -r
complete -c cdc -n "__fish_seen_subcommand_from scaffold" -l user -d "Database user (use \${VAR} for env vars)" -r
complete -c cdc -n "__fish_seen_subcommand_from scaffold" -l password -d "Database password (use \${VAR} for env vars)" -r

# Scaffold flag values - only show when completing value for specific flag
complete -c cdc -n "__fish_seen_subcommand_from scaffold; and __cdc_last_token_is --pattern" -f -a "db-per-tenant" -d "One database per tenant"
complete -c cdc -n "__fish_seen_subcommand_from scaffold; and __cdc_last_token_is --pattern" -f -a "db-shared" -d "Shared database for all tenants"
complete -c cdc -n "__fish_seen_subcommand_from scaffold; and __cdc_last_token_is --source-type" -f -a "postgres" -d "PostgreSQL database"
complete -c cdc -n "__fish_seen_subcommand_from scaffold; and __cdc_last_token_is --source-type" -f -a "mssql" -d "Microsoft SQL Server"
complete -c cdc -n "__fish_seen_subcommand_from scaffold; and __cdc_last_token_is --kafka-topology" -f -a "shared" -d "Same Kafka cluster for all servers"
complete -c cdc -n "__fish_seen_subcommand_from scaffold; and __cdc_last_token_is --kafka-topology" -f -a "per-server" -d "Separate Kafka cluster per server"


# manage-service subcommand options
# Dynamic service completion - lists EXISTING services from services/*.yaml
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l service -d "Existing service name" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-existing-services 2>/dev/null
)"

# Dynamic --create-service completion - lists services from server_group.yaml
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l create-service -d "Create service from server_group.yaml" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-available-services 2>/dev/null
)"

# Dynamic table completion - lists available tables from service-schemas/{service}/{schema}/{TableName}.yaml
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add-source-table -d "Add single table to service (schema.table)" -r -f -a "(
    # Extract --service value from command line
    set -l cmd (commandline -opc)
    set -l service_name ''
    
    # Find --service argument value
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--service'
            set service_name \$cmd[(math \$i + 1)]
            break
        end
    end
    
    if test -n \"\$service_name\"
        python3 -m cdc_generator.helpers.autocompletions --list-tables \"\$service_name\" 2>/dev/null
    end
)"

# Completion for --list-source-tables (only when --service is present)
complete -c cdc -n "__fish_seen_subcommand_from manage-service; and string match -q -- '*--service*' (commandline -opc)" -l list-source-tables -d "List all source tables in service"

complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add-source-tables -d "Add multiple tables (space-separated)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l remove-table -d "Remove table from service" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l inspect -d "Inspect database schema and list tables"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l schema -d "Database schema to inspect or filter" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l save -d "Save detailed table schemas to YAML"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l generate-validation -d "Generate JSON Schema for validation"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l validate-hierarchy -d "Validate hierarchical inheritance"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l validate-config -d "Comprehensive configuration validation"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l all -d "Process all schemas"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l env -d "Environment (nonprod/prod)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l primary-key -d "Primary key column name" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l ignore-columns -d "Column to ignore (schema.table.column)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l track-columns -d "Column to track (schema.table.column)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l server -d "Server name for multi-server setups (default: 'default')" -r

# manage-server-group subcommand options
# Note: All flags are shown regardless of context. Python validation handles invalid combinations.

# General actions
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l update -d "Update server group from database inspection"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l info -d "Show detailed server group information"

# --update options: Server name completion (dynamic from server_group.yaml)
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_last_token_is --update" -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-server-names 2>/dev/null
)" -d "Server name to update"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l all -d "Update all servers (use with --update)"

# Exclude patterns
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l add-to-ignore-list -d "Add pattern(s) to database exclude list" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l list-ignore-patterns -d "List current database exclude patterns"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l add-to-schema-excludes -d "Add pattern(s) to schema exclude list" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l list-schema-excludes -d "List current schema exclude patterns"

# Environment mappings
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l add-env-mapping -d "Add env mapping(s) 'from:to,from:to' (e.g., 'staging:stage,production:prod')" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l list-env-mappings -d "List current environment mappings"

# Multi-server management
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l add-server -d "Add new server (e.g., 'analytics', 'reporting')" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l list-servers -d "List all configured servers"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l remove-server -d "Remove a server configuration" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l set-kafka-topology -d "Change Kafka topology (shared|per-server)" -r
# Kafka topology values - only show when completing value for --set-kafka-topology
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_last_token_is --set-kafka-topology" -f -a "shared" -d "Same Kafka cluster for all servers"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_last_token_is --set-kafka-topology" -f -a "per-server" -d "Separate Kafka cluster per server"

# Extraction pattern management (multi-pattern approach)
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l add-extraction-pattern -d "Add extraction pattern: SERVER PATTERN (with optional --env, --strip-suffixes, --description)" -r
# Dynamic server name completion for --add-extraction-pattern (hardcoded common values + parsing from YAML)
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_last_token_is --add-extraction-pattern" -f -a "default" -d "Default server"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_last_token_is --add-extraction-pattern" -f -a "prod" -d "Production server"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_last_token_is --add-extraction-pattern" -f -a "analytics" -d "Analytics server"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_last_token_is --add-extraction-pattern" -f -a "reporting" -d "Reporting server"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l env -d "Fixed environment for --add-extraction-pattern (overrides captured group)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l strip-suffixes -d "Comma-separated suffixes to strip from service name (e.g., '_db,_database')" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l description -d "Human-readable description for --add-extraction-pattern" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l list-extraction-patterns -d "List extraction patterns for all servers or specific server (optional)" -r
# Dynamic server name completion for --list-extraction-patterns
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_last_token_is --list-extraction-patterns" -f -a "default" -d "Default server"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_last_token_is --list-extraction-patterns" -f -a "prod" -d "Production server"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l remove-extraction-pattern -d "Remove extraction pattern: SERVER INDEX" -r
# Dynamic server name completion for --remove-extraction-pattern
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_last_token_is --remove-extraction-pattern" -f -a "default" -d "Default server"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_last_token_is --remove-extraction-pattern" -f -a "prod" -d "Production server"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l set-extraction-pattern -d "Set single extraction pattern: SERVER PATTERN (legacy, prefer --add-extraction-pattern)" -r

# Creation flags (used with --create)
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l pattern -d "Server group pattern (db-per-tenant|db-shared)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_last_token_is --pattern" -f -a "db-per-tenant db-shared"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l source-type -d "Source database type (postgres|mssql)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_last_token_is --source-type" -f -a "postgres mssql"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l host -d "Database host (use \${VAR} for env vars)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l port -d "Database port" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l user -d "Database user (use \${VAR} for env vars)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l password -d "Database password (use \${VAR} for env vars)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l extraction-pattern -d "Regex pattern with named groups (e.g., '^AdOpus(?P<customer>.+)\$')" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l environment-aware -d "Enable environment-aware grouping (required for db-shared)"

# Add server flags (used with --add-server, same as create but no pattern)
# Note: --source-type, --host, --port, --user, --password are already defined above and work for --add-server too

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
