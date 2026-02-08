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
complete -c cdc -n "__fish_use_subcommand" -a "manage-source-groups" -d "Manage server groups"
complete -c cdc -n "__fish_use_subcommand" -a "manage-sink-groups" -d "Manage sink server groups"
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
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -s h -l help -d "Show manage-source-groups help"
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

# Dynamic --create-service completion - lists services from source-groups.yaml
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l create-service -d "Create service from source-groups.yaml" -r -f -a "(
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

# --inspect-sink: Dynamic completion from service's existing sinks
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l inspect-sink -d "Inspect sink database schema" -r -f -a "(
    set -l cmd (commandline -opc)
    set -l service_name ''
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--service'
            set service_name \$cmd[(math \$i + 1)]
            break
        end
    end
    if test -n \"\$service_name\"
        python3 -m cdc_generator.helpers.autocompletions --list-sink-keys \"\$service_name\" 2>/dev/null
    end
)"

# Dynamic schema completion - lists schemas from source-groups.yaml for the current service
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l schema -d "Database schema to inspect or filter" -r -f -a "(
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
        python3 -m cdc_generator.helpers.autocompletions --list-schemas \"\$service_name\" 2>/dev/null
    end
)"

complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l save -d "Save detailed table schemas to YAML"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l generate-validation -d "Generate JSON Schema for validation"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l validate-hierarchy -d "Validate hierarchical inheritance"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l validate-config -d "Comprehensive configuration validation"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l all -d "Process all schemas"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l env -d "Environment (nonprod/prod)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l primary-key -d "Primary key column name" -r

# Dynamic column completion for --track-columns and --ignore-columns
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l ignore-columns -d "Column to ignore (schema.table.column)" -r -f -a "(
    # Extract --service and --add-source-table values from command line
    set -l cmd (commandline -opc)
    set -l service_name ''
    set -l table_spec ''
    
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--service'
            set service_name \$cmd[(math \$i + 1)]
        else if test \"\$cmd[\$i]\" = '--add-source-table'
            set table_spec \$cmd[(math \$i + 1)]
        end
    end
    
    if test -n \"\$service_name\" -a -n \"\$table_spec\"
        # Parse schema.table from table_spec
        set -l parts (string split -- '.' \"\$table_spec\")
        if test (count \$parts) -eq 2
            set -l schema \$parts[1]
            set -l table \$parts[2]
            python3 -m cdc_generator.helpers.autocompletions --list-columns \"\$service_name\" \"\$schema\" \"\$table\" 2>/dev/null
        end
    end
)"

complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l track-columns -d "Column to track (schema.table.column)" -r -f -a "(
    # Extract --service and --add-source-table values from command line
    set -l cmd (commandline -opc)
    set -l service_name ''
    set -l table_spec ''
    
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--service'
            set service_name \$cmd[(math \$i + 1)]
        else if test \"\$cmd[\$i]\" = '--add-source-table'
            set table_spec \$cmd[(math \$i + 1)]
        end
    end
    
    if test -n \"\$service_name\" -a -n \"\$table_spec\"
        # Parse schema.table from table_spec
        set -l parts (string split -- '.' \"\$table_spec\")
        if test (count \$parts) -eq 2
            set -l schema \$parts[1]
            set -l table \$parts[2]
            python3 -m cdc_generator.helpers.autocompletions --list-columns \"\$service_name\" \"\$schema\" \"\$table\" 2>/dev/null
        end
    end
)"

complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l server -d "Server name for multi-server setups (default: 'default')" -r

# ============================================================================
# Sink management completions for manage-service
# ============================================================================

# --add-sink: Dynamic completion from sink-groups.yaml (inherited_sources or sources keys)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add-sink -d "Add sink destination (sink_group.target_service)" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-available-sink-keys 2>/dev/null
)"

# --remove-sink: Dynamic completion from service's existing sinks
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l remove-sink -d "Remove sink destination" -r -f -a "(
    set -l cmd (commandline -opc)
    set -l service_name ''
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--service'
            set service_name \$cmd[(math \$i + 1)]
            break
        end
    end
    if test -n \"\$service_name\"
        python3 -m cdc_generator.helpers.autocompletions --list-sink-keys \"\$service_name\" 2>/dev/null
    end
)"

# --sink: Select existing sink for table operations
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l sink -d "Target sink for table operations (sink_group.target_service)" -r -f -a "(
    set -l cmd (commandline -opc)
    set -l service_name ''
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--service'
            set service_name \$cmd[(math \$i + 1)]
            break
        end
    end
    if test -n \"\$service_name\"
        python3 -m cdc_generator.helpers.autocompletions --list-sink-keys \"\$service_name\" 2>/dev/null
    end
)"

# --add-sink-table: Dynamic completion from sink's target service tables (service-schemas)
# Reads --sink from command line; if not specified, auto-defaults when service has only one sink
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add-sink-table -d "Add table to sink (schema.table)" -r -f -a "(
    set -l cmd (commandline -opc)
    set -l service_name ''
    set -l sink_key ''
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--service'
            set service_name \$cmd[(math \$i + 1)]
        else if test \"\$cmd[\$i]\" = '--sink'
            set sink_key \$cmd[(math \$i + 1)]
        end
    end
    # Auto-default --sink when service has only one sink
    if test -n \"\$service_name\" -a -z \"\$sink_key\"
        set sink_key (python3 -m cdc_generator.helpers.autocompletions --get-default-sink \"\$service_name\" 2>/dev/null)
    end
    if test -n \"\$sink_key\"
        python3 -m cdc_generator.helpers.autocompletions --list-sink-target-tables \"\$sink_key\" 2>/dev/null
    end
)"

# --remove-sink-table: Same as add (shows source tables to select from)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l remove-sink-table -d "Remove table from sink" -r -f -a "(
    set -l cmd (commandline -opc)
    set -l service_name ''
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--service'
            set service_name \$cmd[(math \$i + 1)]
            break
        end
    end
    if test -n \"\$service_name\"
        python3 -m cdc_generator.helpers.autocompletions --list-source-tables \"\$service_name\" 2>/dev/null
    end
)"

# --target: Dynamic completion from service-schemas/{target_service}/
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l target -d "Target table for mapping (schema.table)" -r -f -a "(
    set -l cmd (commandline -opc)
    set -l service_name ''
    set -l sink_key ''
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--service'
            set service_name \$cmd[(math \$i + 1)]
        else if test \"\$cmd[\$i]\" = '--sink'
            set sink_key \$cmd[(math \$i + 1)]
        end
    end
    if test -n \"\$service_name\" -a -n \"\$sink_key\"
        python3 -m cdc_generator.helpers.autocompletions --list-target-tables \"\$service_name\" \"\$sink_key\" 2>/dev/null
    end
)"

# --target-exists: Required for --add-sink-table (true = map existing, false = autocreate)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l target-exists -d "Table exists in target? (true=map, false=autocreate)" -r -f -a "true false"

# --target-schema: Dynamic completion from service-schemas/{target_service}/ schemas
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l target-schema -d "Override target schema for cloned sink table" -r -f -a "(
    set -l cmd (commandline -opc)
    set -l sink_key ''
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--sink'
            set sink_key \$cmd[(math \$i + 1)]
            break
        end
    end
    if test -n \"\$sink_key\"
        # Extract target_service from sink_key (after first dot)
        set -l target_service (string split -m 1 -- '.' \"\$sink_key\")[2]
        if test -n \"\$target_service\"
            python3 -m cdc_generator.helpers.autocompletions --list-schemas \"\$target_service\" 2>/dev/null
        end
    end
)"

# --map-column: Dynamic completion from service-schemas target columns
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l map-column -d "Map source column to target column (src tgt)" -r -f -a "(
    set -l cmd (commandline -opc)
    set -l sink_key ''
    set -l target_table ''
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--sink'
            set sink_key \$cmd[(math \$i + 1)]
        else if test \"\$cmd[\$i]\" = '--target'
            set target_table \$cmd[(math \$i + 1)]
        end
    end
    if test -n \"\$sink_key\" -a -n \"\$target_table\"
        python3 -m cdc_generator.helpers.autocompletions --list-target-columns \"\$sink_key\" \"\$target_table\" 2>/dev/null
    end
)"

# --include-sink-columns: Dynamic completion from source table columns
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l include-sink-columns -d "Only sync these columns to sink" -r -f -a "(
    set -l cmd (commandline -opc)
    set -l service_name ''
    set -l table_spec ''
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--service'
            set service_name \$cmd[(math \$i + 1)]
        else if test \"\$cmd[\$i]\" = '--add-sink-table'
            set table_spec \$cmd[(math \$i + 1)]
        end
    end
    if test -n \"\$service_name\" -a -n \"\$table_spec\"
        set -l parts (string split -- '.' \"\$table_spec\")
        if test (count \$parts) -eq 2
            set -l schema \$parts[1]
            set -l table \$parts[2]
            python3 -m cdc_generator.helpers.autocompletions --list-columns \"\$service_name\" \"\$schema\" \"\$table\" 2>/dev/null
        end
    end
)"

# --list-sinks and --validate-sinks (no value needed)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l list-sinks -d "List all sink configurations for service"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l validate-sinks -d "Validate sink configuration"

# ============================================================================
# Custom sink table completions for manage-service
# ============================================================================

# --add-custom-sink-table: Free text (user defines schema.table name)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add-custom-sink-table -d "Create custom table in sink (schema.table)" -r

# --column: Column definition with type autocompletion
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l column -d "Column def: name:type[:pk][:not_null][:default_X]" -r

# --modify-custom-table: Dynamic completion from custom tables in service
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l modify-custom-table -d "Modify custom table columns" -r -f -a "(
    set -l cmd (commandline -opc)
    set -l service_name ''
    set -l sink_key ''
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--service'
            set service_name \$cmd[(math \$i + 1)]
        else if test \"\$cmd[\$i]\" = '--sink'
            set sink_key \$cmd[(math \$i + 1)]
        end
    end
    # Auto-default --sink when service has only one sink
    if test -n \"\$service_name\" -a -z \"\$sink_key\"
        set sink_key (python3 -m cdc_generator.helpers.autocompletions --get-default-sink \"\$service_name\" 2>/dev/null)
    end
    if test -n \"\$service_name\" -a -n \"\$sink_key\"
        python3 -m cdc_generator.helpers.autocompletions --list-custom-tables \"\$service_name\" \"\$sink_key\" 2>/dev/null
    end
)"

# --add-column: Free text (column spec)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add-column -d "Add column: name:type[:pk][:not_null][:default_X]" -r

# --remove-column: Dynamic completion from custom table columns
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l remove-column -d "Remove column from custom table" -r -f -a "(
    set -l cmd (commandline -opc)
    set -l service_name ''
    set -l sink_key ''
    set -l table_key ''
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--service'
            set service_name \$cmd[(math \$i + 1)]
        else if test \"\$cmd[\$i]\" = '--sink'
            set sink_key \$cmd[(math \$i + 1)]
        else if test \"\$cmd[\$i]\" = '--modify-custom-table'
            set table_key \$cmd[(math \$i + 1)]
        end
    end
    if test -n \"\$service_name\" -a -z \"\$sink_key\"
        set sink_key (python3 -m cdc_generator.helpers.autocompletions --get-default-sink \"\$service_name\" 2>/dev/null)
    end
    if test -n \"\$service_name\" -a -n \"\$sink_key\" -a -n \"\$table_key\"
        python3 -m cdc_generator.helpers.autocompletions --list-custom-table-columns \"\$service_name\" \"\$sink_key\" \"\$table_key\" 2>/dev/null
    end
)"

# manage-source-groups subcommand options
# Note: All flags are shown regardless of context. Python validation handles invalid combinations.

# General actions
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l update -d "Update server group from database inspection"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l info -d "Show detailed server group information"

# --update options: Server name completion (dynamic from source-groups.yaml)
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups; and __cdc_last_token_is --update" -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-server-names 2>/dev/null
)" -d "Server name to update"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l all -d "Update all servers (use with --update)"

# Exclude patterns
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l add-to-ignore-list -d "Add pattern(s) to database exclude list" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l list-ignore-patterns -d "List current database exclude patterns"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l add-to-schema-excludes -d "Add pattern(s) to schema exclude list" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l list-schema-excludes -d "List current schema exclude patterns"

# Environment mappings
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l add-env-mapping -d "Add env mapping(s) 'from:to,from:to' (e.g., 'staging:stage,production:prod')" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l list-env-mappings -d "List current environment mappings"

# Multi-server management
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l add-server -d "Add new server (e.g., 'analytics', 'reporting')" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l list-servers -d "List all configured servers"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l remove-server -d "Remove a server configuration" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l set-kafka-topology -d "Change Kafka topology (shared|per-server)" -r
# Kafka topology values - only show when completing value for --set-kafka-topology
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups; and __cdc_last_token_is --set-kafka-topology" -f -a "shared" -d "Same Kafka cluster for all servers"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups; and __cdc_last_token_is --set-kafka-topology" -f -a "per-server" -d "Separate Kafka cluster per server"

# Extraction pattern management (multi-pattern approach)
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l add-extraction-pattern -d "Add extraction pattern: SERVER PATTERN (with optional --env, --strip-suffixes, --description)" -r
# Dynamic server name completion for --add-extraction-pattern (hardcoded common values + parsing from YAML)
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups; and __cdc_last_token_is --add-extraction-pattern" -f -a "default" -d "Default server"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups; and __cdc_last_token_is --add-extraction-pattern" -f -a "prod" -d "Production server"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups; and __cdc_last_token_is --add-extraction-pattern" -f -a "analytics" -d "Analytics server"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups; and __cdc_last_token_is --add-extraction-pattern" -f -a "reporting" -d "Reporting server"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l env -d "Fixed environment for --add-extraction-pattern (overrides captured group)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l strip-suffixes -d "Comma-separated suffixes to strip from service name (e.g., '_db,_database')" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l description -d "Human-readable description for --add-extraction-pattern" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l list-extraction-patterns -d "List extraction patterns for all servers or specific server (optional)" -r
# Dynamic server name completion for --list-extraction-patterns
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups; and __cdc_last_token_is --list-extraction-patterns" -f -a "default" -d "Default server"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups; and __cdc_last_token_is --list-extraction-patterns" -f -a "prod" -d "Production server"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l remove-extraction-pattern -d "Remove extraction pattern: SERVER INDEX" -r
# Dynamic server name completion for --remove-extraction-pattern
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups; and __cdc_last_token_is --remove-extraction-pattern" -f -a "default" -d "Default server"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups; and __cdc_last_token_is --remove-extraction-pattern" -f -a "prod" -d "Production server"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l set-extraction-pattern -d "Set single extraction pattern: SERVER PATTERN (legacy, prefer --add-extraction-pattern)" -r

# Type introspection
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l introspect-types -d "Introspect column types from database server"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l server -d "Server to use for --introspect-types (default: first available)" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-server-names 2>/dev/null
)"

# Creation flags (used with --create)
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l pattern -d "Server group pattern (db-per-tenant|db-shared)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups; and __cdc_last_token_is --pattern" -f -a "db-per-tenant db-shared"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l source-type -d "Source database type (postgres|mssql)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups; and __cdc_last_token_is --source-type" -f -a "postgres mssql"
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l host -d "Database host (use \${VAR} for env vars)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l port -d "Database port" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l user -d "Database user (use \${VAR} for env vars)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l password -d "Database password (use \${VAR} for env vars)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l extraction-pattern -d "Regex pattern with named groups (e.g., '^AdOpus(?P<customer>.+)\$')" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-source-groups" -l environment-aware -d "Enable environment-aware grouping (required for db-shared)"

# Add server flags (used with --add-server, same as create but no pattern)
# Note: --source-type, --host, --port, --user, --password are already defined above and work for --add-server too

# manage-sink-groups subcommand options
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -s h -l help -d "Show manage-sink-groups help"

# Create actions
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l create -d "Create sink groups (auto-scaffold or specific)"
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l source-group -d "Source group to inherit from (for inherited sink groups)" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-server-group-names 2>/dev/null
)"
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l add-new-sink-group -d "Add new standalone sink group (auto-prefixes with 'sink_')" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l type -d "Type of sink" -r -f -a "postgres\t'PostgreSQL database'
mssql\t'Microsoft SQL Server'
http_client\t'HTTP client sink'
http_server\t'HTTP server sink'"
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l pattern -d "Pattern for sink group" -r -f -a "db-shared\t'Shared database for all tenants'
db-per-tenant\t'One database per tenant'"
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l environment-aware -d "Enable environment-aware grouping (default)"
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l no-environment-aware -d "Disable environment-aware (single-environment server)"
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l for-source-group -d "Source group this standalone sink consumes from" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-server-group-names 2>/dev/null
)"

# List/Info actions
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l list -d "List all sink groups"
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l info -d "Show detailed information about a sink group" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-sink-group-names 2>/dev/null
)"

# Inspection actions (standalone sink groups only)
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l inspect -d "Inspect databases on sink server (standalone sink groups only)"
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l server -d "Server name to inspect (default: 'default')" -r -f -a "(
    # TODO: dynamic server completion per sink group
    echo 'default'
    echo 'prod'
)"
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l include-pattern -d "Only include databases matching regex pattern" -r

# Type introspection
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l introspect-types -d "Introspect column types from database server (requires --sink-group)"

# Validation
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l validate -d "Validate sink group configuration"

# Server management
# --sink-group: show only non-inherited sink groups when adding/removing servers
# (inherited sink groups get servers from their source group)
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l sink-group -d "Sink group to operate on" -r -f -a "(
    if contains -- --add-server (commandline -opc); or contains -- --remove-server (commandline -opc)
        python3 -m cdc_generator.helpers.autocompletions --list-non-inherited-sink-group-names 2>/dev/null
    else
        python3 -m cdc_generator.helpers.autocompletions --list-sink-group-names 2>/dev/null
    end
)"
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l add-server -d "Add a server to a sink group (requires --sink-group)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l remove-server -d "Remove a server from a sink group (requires --sink-group)" -r -f -a "(
    # Read --sink-group value from command line
    set -l cmd (commandline -opc)
    set -l sink_group ''
    for i in (seq (count \$cmd))
        if test \"\$cmd[\$i]\" = '--sink-group'
            set sink_group \$cmd[(math \$i + 1)]
        end
    end
    if test -n \"\$sink_group\"
        python3 -m cdc_generator.helpers.autocompletions --list-sink-group-servers \"\$sink_group\" 2>/dev/null
    end
)"
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l host -d "Server host" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l port -d "Server port" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l user -d "Server user" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l password -d "Server password" -r

# Sink group removal (only non-inherited — inherited are auto-generated)
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l remove -d "Remove a sink group" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-non-inherited-sink-group-names 2>/dev/null
)"

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
