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

function __cdc_get_flag_value --description "Extract value following a flag from the command line"
    set -l flag $argv[1]
    set -l cmd (commandline -opc)
    for i in (seq (count $cmd))
        if test "$cmd[$i]" = "$flag"
            set -l next (math $i + 1)
            if test $next -le (count $cmd)
                echo $cmd[$next]
                return
            end
        end
    end
end

function __cdc_get_service_name --description "Extract service name from --service flag or positional arg after manage-service"
    set -l cmd (commandline -opc)
    # Check --service flag first
    for i in (seq (count $cmd))
        if test "$cmd[$i]" = '--service'
            set -l next (math $i + 1)
            if test $next -le (count $cmd)
                echo $cmd[$next]
                return
            end
        end
    end
    # Fall back to positional arg: token right after 'manage-service' that doesn't start with '-'
    for i in (seq (count $cmd))
        if test "$cmd[$i]" = 'manage-service'
            set -l next (math $i + 1)
            if test $next -le (count $cmd)
                set -l candidate $cmd[$next]
                if not string match -q -- '-*' "$candidate"
                    echo $candidate
                    return
                end
            end
        end
    end
end

function __cdc_has_service --description "Check if service name is available (via --service or positional)"
    set -l svc (__cdc_get_service_name)
    test -n "$svc"
end

function __cdc_get_sink_key --description "Extract --sink value, with auto-default from service"
    set -l sink_key (__cdc_get_flag_value --sink)
    if test -z "$sink_key"
        set -l service_name (__cdc_get_service_name)
        if test -n "$service_name"
            set sink_key (python3 -m cdc_generator.helpers.autocompletions --get-default-sink "$service_name" 2>/dev/null)
        end
    end
    echo $sink_key
end

function __cdc_get_table_spec --description "Extract table spec from --add-source-table or --source-table"
    set -l spec (__cdc_get_flag_value --add-source-table)
    if test -z "$spec"
        set spec (__cdc_get_flag_value --source-table)
    end
    echo $spec
end

function __cdc_map_column_needs_sink_col --description "Check if we need the 2nd arg (sink column) for --map-column"
    set -l tokens (commandline -opc)
    set -l i (count $tokens)
    set -l args_after 0
    while test $i -ge 1
        switch $tokens[$i]
            case "--map-column"
                # Found --map-column; if exactly 1 non-flag arg follows → need 2nd arg
                test $args_after -eq 1
                return $status
            case "--*"
                # Hit another flag before finding --map-column with 1 arg
                return 1
            case "*"
                set args_after (math $args_after + 1)
        end
        set i (math $i - 1)
    end
    return 1
end

function __cdc_complete_columns --description "Complete columns for a service and table spec"
    set -l service_name (__cdc_get_service_name)
    set -l table_spec (__cdc_get_table_spec)
    if test -n "$service_name" -a -n "$table_spec"
        set -l parts (string split -- '.' "$table_spec")
        if test (count $parts) -eq 2
            python3 -m cdc_generator.helpers.autocompletions --list-columns "$service_name" "$parts[1]" "$parts[2]" 2>/dev/null
        end
    end
end

function __cdc_complete_sink_tables --description "Complete sink tables for current service and sink"
    set -l service_name (__cdc_get_service_name)
    set -l sink_key (__cdc_get_sink_key)
    if test -n "$service_name" -a -n "$sink_key"
        python3 -m cdc_generator.helpers.autocompletions --list-sink-tables "$service_name" "$sink_key" 2>/dev/null
    end
end

function __cdc_complete_templates_on_table --description "Complete column templates applied to a sink table"
    set -l service_name (__cdc_get_service_name)
    set -l sink_key (__cdc_get_sink_key)
    set -l sink_table (__cdc_get_flag_value --sink-table)
    if test -n "$service_name" -a -n "$sink_key" -a -n "$sink_table"
        python3 -m cdc_generator.helpers.autocompletions --list-column-templates-on-table "$service_name" "$sink_key" "$sink_table" 2>/dev/null
    end
end

function __cdc_complete_transforms_on_table --description "Complete transforms applied to a sink table"
    set -l service_name (__cdc_get_service_name)
    set -l sink_key (__cdc_get_sink_key)
    set -l sink_table (__cdc_get_flag_value --sink-table)
    if test -n "$service_name" -a -n "$sink_key" -a -n "$sink_table"
        python3 -m cdc_generator.helpers.autocompletions --list-table-transforms "$service_name" "$sink_key" "$sink_table" 2>/dev/null
    end
end

function __cdc_complete_map_column --description "Complete --map-column args (source or target columns)"
    set -l service_name (__cdc_get_service_name)
    set -l sink_key (__cdc_get_flag_value --sink)
    set -l target_table (__cdc_get_flag_value --target)
    set -l sink_table (__cdc_get_flag_value --sink-table)
    set -l add_sink_table (__cdc_get_flag_value --add-sink-table)

    # Count args after last --map-column
    set -l cmd (commandline -opc)
    set -l last_mc_idx 0
    for i in (seq (count $cmd))
        if test "$cmd[$i]" = '--map-column'
            set last_mc_idx $i
        end
    end
    set -l args_after_mc (math (count $cmd) - $last_mc_idx)

    if test -n "$sink_table" -a -n "$sink_key" -a -n "$service_name"
        if test $args_after_mc -le 1
            python3 -m cdc_generator.helpers.autocompletions --list-source-columns-for-sink-table "$service_name" "$sink_key" "$sink_table" 2>/dev/null
        else
            python3 -m cdc_generator.helpers.autocompletions --list-target-columns "$sink_key" "$sink_table" 2>/dev/null
        end
    else if test -n "$sink_key" -a -n "$target_table"
        python3 -m cdc_generator.helpers.autocompletions --list-target-columns "$sink_key" "$target_table" 2>/dev/null
    else if test -n "$sink_key" -a -n "$add_sink_table"
        python3 -m cdc_generator.helpers.autocompletions --list-target-columns "$sink_key" "$add_sink_table" 2>/dev/null
    end
end

function __cdc_complete_map_column_2nd --description "Complete 2nd arg for --map-column (sink column)"
    set -l sink_key (__cdc_get_flag_value --sink)
    set -l sink_table (__cdc_get_flag_value --sink-table)
    if test -n "$sink_key" -a -n "$sink_table"
        python3 -m cdc_generator.helpers.autocompletions --list-target-columns "$sink_key" "$sink_table" 2>/dev/null
    end
end

function __cdc_complete_add_sink_table --description "Complete tables available to add to a sink"
    set -l service_name (__cdc_get_service_name)
    set -l sink_key (__cdc_get_flag_value --sink)
    if test -n "$service_name" -a -z "$sink_key"
        set sink_key (python3 -m cdc_generator.helpers.autocompletions --get-default-sink "$service_name" 2>/dev/null)
    end
    if test -n "$sink_key"
        python3 -m cdc_generator.helpers.autocompletions --list-sink-target-tables "$sink_key" 2>/dev/null
    end
end

function __cdc_complete_remove_sink_table --description "Complete tables to remove from a sink"
    set -l service_name (__cdc_get_service_name)
    set -l sink_key (__cdc_get_sink_key)
    if test -n "$service_name" -a -n "$sink_key"
        python3 -m cdc_generator.helpers.autocompletions --list-sink-tables "$service_name" "$sink_key" 2>/dev/null
    end
end

function __cdc_complete_target_tables --description "Complete target tables for a sink"
    set -l service_name (__cdc_get_service_name)
    set -l sink_key (__cdc_get_flag_value --sink)
    if test -n "$service_name" -a -n "$sink_key"
        python3 -m cdc_generator.helpers.autocompletions --list-target-tables "$service_name" "$sink_key" 2>/dev/null
    end
end

function __cdc_complete_target_schema --description "Complete schemas for a sink's target service"
    set -l sink_key (__cdc_get_flag_value --sink)
    if test -n "$sink_key"
        set -l target_service (string split -m 1 -- '.' "$sink_key")[2]
        if test -n "$target_service"
            python3 -m cdc_generator.helpers.autocompletions --list-schemas "$target_service" 2>/dev/null
        end
    end
end

function __cdc_complete_custom_tables --description "Complete custom tables for a sink"
    set -l service_name (__cdc_get_service_name)
    set -l sink_key (__cdc_get_sink_key)
    if test -n "$service_name" -a -n "$sink_key"
        python3 -m cdc_generator.helpers.autocompletions --list-custom-tables "$service_name" "$sink_key" 2>/dev/null
    end
end

function __cdc_complete_custom_table_columns --description "Complete columns for a custom table"
    set -l service_name (__cdc_get_service_name)
    set -l sink_key (__cdc_get_sink_key)
    set -l table_key (__cdc_get_flag_value --modify-custom-table)
    if test -n "$service_name" -a -n "$sink_key" -a -n "$table_key"
        python3 -m cdc_generator.helpers.autocompletions --list-custom-table-columns "$service_name" "$sink_key" "$table_key" 2>/dev/null
    end
end

function __cdc_complete_sink_group_servers --description "Complete servers for a sink group"
    set -l sink_group (__cdc_get_flag_value --sink-group)
    if test -n "$sink_group"
        python3 -m cdc_generator.helpers.autocompletions --list-sink-group-servers "$sink_group" 2>/dev/null
    end
end

function __cdc_complete_include_sink_columns --description "Complete columns for --add-sink-table table spec"
    set -l service_name (__cdc_get_service_name)
    set -l table_spec (__cdc_get_flag_value --add-sink-table)
    if test -n "$service_name" -a -n "$table_spec"
        set -l parts (string split -- '.' "$table_spec")
        if test (count $parts) -eq 2
            python3 -m cdc_generator.helpers.autocompletions --list-columns "$service_name" "$parts[1]" "$parts[2]" 2>/dev/null
        end
    end
end

function __cdc_complete_available_tables --description "Complete available tables from service-schemas"
    set -l service_name (__cdc_get_service_name)
    if test -n "$service_name"
        python3 -m cdc_generator.helpers.autocompletions --list-tables "$service_name" 2>/dev/null
    end
end

function __cdc_complete_source_tables --description "Complete existing source tables in service"
    set -l service_name (__cdc_get_service_name)
    if test -n "$service_name"
        python3 -m cdc_generator.helpers.autocompletions --list-source-tables "$service_name" 2>/dev/null
    end
end

function __cdc_complete_sink_keys --description "Complete sink keys for current service"
    set -l service_name (__cdc_get_service_name)
    if test -n "$service_name"
        python3 -m cdc_generator.helpers.autocompletions --list-sink-keys "$service_name" 2>/dev/null
    end
end

function __cdc_complete_schemas --description "Complete schemas for current service"
    set -l service_name (__cdc_get_service_name)
    if test -n "$service_name"
        python3 -m cdc_generator.helpers.autocompletions --list-schemas "$service_name" 2>/dev/null
    end
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
complete -c cdc -n "__fish_use_subcommand" -a "manage-column-templates" -d "Manage column template definitions"
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
complete -c cdc -n "__fish_use_subcommand" -a "test" -d "Run project tests"
complete -c cdc -n "__fish_use_subcommand" -a "test-coverage" -d "Show test coverage report by cdc command"
complete -c cdc -n "__fish_use_subcommand" -a "help" -d "Show help message"

# ── cdc test flags ──────────────────────────────────────────────────────────
complete -c cdc -n "__fish_seen_subcommand_from test" -l cli -d "Run CLI end-to-end tests only"
complete -c cdc -n "__fish_seen_subcommand_from test" -l all -d "Run all tests (unit + CLI e2e)"
complete -c cdc -n "__fish_seen_subcommand_from test" -s v -d "Verbose pytest output"
complete -c cdc -n "__fish_seen_subcommand_from test" -s k -d "Filter tests by expression"

# ── cdc test-coverage flags ─────────────────────────────────────────────────
complete -c cdc -n "__fish_seen_subcommand_from test-coverage" -s v -d "Verbose: show individual test names"
complete -c cdc -n "__fish_seen_subcommand_from test-coverage" -l json -d "Output as JSON"

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

# Positional service name shorthand: `cdc manage-service directory` = `cdc manage-service --service directory`
# Only suggest when the token after manage-service is being completed and no --service/--create-service given
complete -c cdc -n "__fish_seen_subcommand_from manage-service; and not __fish_contains_opt service create-service; and not __cdc_has_service" -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-existing-services 2>/dev/null
)"

# Dynamic --create-service completion - lists services from source-groups.yaml
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l create-service -d "Create service from source-groups.yaml" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-available-services 2>/dev/null
)"

# Dynamic table completion - lists available tables from service-schemas/{service}/{schema}/{TableName}.yaml
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add-source-table -d "Add single table to service (schema.table)" -r -f -a "(__cdc_complete_available_tables)"

# Completion for --list-source-tables (only when service is available)
complete -c cdc -n "__fish_seen_subcommand_from manage-service; and __cdc_has_service" -l list-source-tables -d "List all source tables in service"

complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add-source-tables -d "Add multiple tables (space-separated)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l remove-table -d "Remove table from service" -r -f -a "(__cdc_complete_source_tables)"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l inspect -d "Inspect database schema and list tables"

# --source-table: Dynamic completion from existing source tables in service YAML
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l source-table -d "Manage existing source table (use with --track-columns/--ignore-columns)" -r -f -a "(__cdc_complete_source_tables)"

# --inspect-sink: Dynamic completion from service's existing sinks
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l inspect-sink -d "Inspect sink database schema" -r -f -a "(__cdc_complete_sink_keys)"

# Dynamic schema completion - lists schemas from source-groups.yaml for the current service
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l schema -d "Database schema to inspect or filter" -r -f -a "(__cdc_complete_schemas)"

complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l save -d "Save detailed table schemas to YAML"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l generate-validation -d "Generate JSON Schema for validation"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l validate-hierarchy -d "Validate hierarchical inheritance"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l validate-config -d "Comprehensive configuration validation"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l all -d "Process all schemas"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l env -d "Environment (nonprod/prod)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l primary-key -d "Primary key column name" -r

# Dynamic column completion for --track-columns and --ignore-columns
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l ignore-columns -d "Column to ignore (schema.table.column)" -r -f -a "(__cdc_complete_columns)"

complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l track-columns -d "Column to track (schema.table.column)" -r -f -a "(__cdc_complete_columns)"

complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l server -d "Server name for multi-server setups (default: 'default')" -r

# ============================================================================
# Sink management completions for manage-service
# ============================================================================

# --add-sink: Dynamic completion from sink-groups.yaml (inherited_sources or sources keys)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add-sink -d "Add sink destination (sink_group.target_service)" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-available-sink-keys 2>/dev/null
)"

# --remove-sink: Dynamic completion from service's existing sinks
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l remove-sink -d "Remove sink destination" -r -f -a "(__cdc_complete_sink_keys)"

# --sink: Select existing sink for table operations
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l sink -d "Target sink for table operations (sink_group.target_service)" -r -f -a "(__cdc_complete_sink_keys)"

# --add-sink-table: Dynamic completion from sink's target service tables (service-schemas)
# Reads --sink from command line; if not specified, auto-defaults when service has only one sink
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add-sink-table -d "Add table to sink (schema.table)" -r -f -a "(__cdc_complete_add_sink_table)"

# --remove-sink-table: List tables currently configured in the sink
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l remove-sink-table -d "Remove table from sink" -r -f -a "(__cdc_complete_remove_sink_table)"

# --target: Dynamic completion from service-schemas/{target_service}/
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l target -d "Target table for mapping (schema.table)" -r -f -a "(__cdc_complete_target_tables)"

# --target-exists: Required for --add-sink-table (true = map existing, false = autocreate)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l target-exists -d "Table exists in target? (true=map, false=autocreate)" -r -f -a "true false"

# --from: Dynamic completion from service source tables (for explicit source reference)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l from -d "Source table reference (defaults to sink table name)" -r -f -a "(__cdc_complete_source_tables)"

# --replicate-structure: Boolean flag for auto-generating sink table DDL
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l replicate-structure -d "Auto-generate sink table DDL from source schema"

# --sink-schema: Override sink table schema (for custom tables)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l sink-schema -d "Override sink table schema (saves to custom-tables/)" -r

# --sink-table: Target existing sink table for update operations
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l sink-table -d "Target sink table for update operations (schema.table)" -r -f -a "(__cdc_complete_sink_tables)"

# --update-schema: Update schema of an existing sink table
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l update-schema -d "Update schema of sink table (requires --sink and --sink-table)" -r

# --target-schema: Dynamic completion from service-schemas/{target_service}/ schemas
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l target-schema -d "Override target schema for cloned sink table" -r -f -a "(__cdc_complete_target_schema)"

# --map-column: Dynamic completion for column mapping
# Context-aware: with --sink-table (existing table) → source columns then sink columns
#                with --add-sink-table → target columns from service-schemas
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l map-column -d "Map source column to target column (src tgt)" -r -f -a "(__cdc_complete_map_column)"

# --map-column 2nd arg: sink table columns (bare token after source column)
# Fires when exactly 1 arg follows the last --map-column (the source column was typed)
complete -c cdc -n "__fish_seen_subcommand_from manage-service; and __cdc_map_column_needs_sink_col" -f -a "(__cdc_complete_map_column_2nd)"

# --include-sink-columns: Dynamic completion from source table columns
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l include-sink-columns -d "Only sync these columns to sink" -r -f -a "(__cdc_complete_include_sink_columns)"

# --list-sinks and --validate-sinks (no value needed)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l list-sinks -d "List all sink configurations for service"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l validate-sinks -d "Validate sink configuration"

# ============================================================================
# Column templates & transforms completions for manage-service
# ============================================================================

# --add-column-template: Dynamic completion from column-templates.yaml
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add-column-template -d "Add column template to sink table" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-column-templates 2>/dev/null
)"

# --remove-column-template: Dynamic completion from table's existing column templates
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l remove-column-template -d "Remove column template from sink table" -r -f -a "(__cdc_complete_templates_on_table)"

# --list-column-templates (no value needed)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l list-column-templates -d "List column templates on sink table"

# --column-name: Override column name for column template (free text)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l column-name -d "Override column name for column template" -r

# --value: Override column value for column template (free text or source-group reference)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l value -d "Override column value (supports {group.sources.*.key} references)" -r

# --add-transform: Dynamic completion from transform-rules.yaml
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add-transform -d "Add transform rule to sink table" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-transform-rules 2>/dev/null
)"

# --remove-transform: Dynamic completion from table's existing transforms
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l remove-transform -d "Remove transform rule from sink table" -r -f -a "(__cdc_complete_transforms_on_table)"

# --list-transforms (no value needed)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l list-transforms -d "List transforms on sink table"

# --list-column-templates (no value needed, global)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l list-column-templates -d "List all available column templates"

# --list-transform-rules (no value needed, global)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l list-transform-rules -d "List all available transform rules"

# --skip-validation: Boolean flag to skip DB schema validation
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l skip-validation -d "Skip database schema validation when adding templates/transforms"

# ============================================================================
# Custom sink table completions for manage-service
# ============================================================================

# --add-custom-sink-table: Free text (user defines schema.table name)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add-custom-sink-table -d "Create custom table in sink (schema.table)" -r

# --column: Column definition with type autocompletion
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l column -d "Column def: name:type[:pk][:not_null][:default_X]" -r

# --modify-custom-table: Dynamic completion from custom tables in service
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l modify-custom-table -d "Modify custom table columns" -r -f -a "(__cdc_complete_custom_tables)"

# --add-column: Free text (column spec)
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l add-column -d "Add column: name:type[:pk][:not_null][:default_X]" -r

# --remove-column: Dynamic completion from custom table columns
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l remove-column -d "Remove column from custom table" -r -f -a "(__cdc_complete_custom_table_columns)"

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
complete -c cdc -n "__fish_seen_subcommand_from manage-sink-groups" -l remove-server -d "Remove a server from a sink group (requires --sink-group)" -r -f -a "(__cdc_complete_sink_group_servers)"
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

# ============================================================================
# manage-column-templates subcommand options
# ============================================================================
complete -c cdc -n "__fish_seen_subcommand_from manage-column-templates" -s h -l help -d "Show manage-column-templates help"

# --list: List all template definitions
complete -c cdc -n "__fish_seen_subcommand_from manage-column-templates" -l list -d "List all column template definitions"

# --show: Dynamic completion from existing templates
complete -c cdc -n "__fish_seen_subcommand_from manage-column-templates" -l show -d "Show template details" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-column-templates 2>/dev/null
)"

# --add: Free text (new template key)
complete -c cdc -n "__fish_seen_subcommand_from manage-column-templates" -l add -d "Add new template definition" -r

# --edit: Dynamic completion from existing templates
complete -c cdc -n "__fish_seen_subcommand_from manage-column-templates" -l edit -d "Edit existing template" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-column-templates 2>/dev/null
)"

# --remove: Dynamic completion from existing templates
complete -c cdc -n "__fish_seen_subcommand_from manage-column-templates" -l remove -d "Remove template definition" -r -f -a "(
    python3 -m cdc_generator.helpers.autocompletions --list-column-templates 2>/dev/null
)"

# Template field flags
complete -c cdc -n "__fish_seen_subcommand_from manage-column-templates" -l name -d "Column name (default: _<key>)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-column-templates" -l type -d "PostgreSQL column type" -r -f -a "
    text\t'Variable-length string'
    integer\t'32-bit integer'
    bigint\t'64-bit integer'
    boolean\t'True/false'
    timestamptz\t'Timestamp with timezone'
    uuid\t'UUID'
    jsonb\t'Binary JSON'
    numeric\t'Exact numeric'
    smallint\t'16-bit integer'
    real\t'32-bit float'
    date\t'Calendar date'
    time\t'Time of day'
    bytea\t'Binary data'
"
complete -c cdc -n "__fish_seen_subcommand_from manage-column-templates" -l value -d "Bloblang expression or env var" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-column-templates" -l description -d "Human-readable description" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-column-templates" -l not-null -d "Mark column as NOT NULL"
complete -c cdc -n "__fish_seen_subcommand_from manage-column-templates" -l nullable -d "Mark column as nullable"
complete -c cdc -n "__fish_seen_subcommand_from manage-column-templates" -l default -d "SQL default expression for DDL" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-column-templates" -l applies-to -d "Table glob pattern restriction" -r
