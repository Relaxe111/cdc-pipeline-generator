# Fish shell completions for cdc command
# Auto-generated completions for CDC Pipeline CLI

function __cdc_has_manage_server_group_create --description "Check if --create flag is present for manage-server-group"
    for token in (commandline -opc)
        if test "$token" = "--create"
            return 0
        end
    end
    return 1
end

# Main command description
complete -c cdc -f -d "CDC Pipeline Management CLI"

# Generator commands (work from generator or implementation)
complete -c cdc -n "__fish_use_subcommand" -a "init" -d "Initialize a new CDC pipeline project"
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

# init subcommand options
complete -c cdc -n "__fish_seen_subcommand_from init" -l name -d "Project name (e.g., adopus-cdc-pipeline)" -r
complete -c cdc -n "__fish_seen_subcommand_from init" -l type -d "Implementation type" -r -f -a "adopus asma"
complete -c cdc -n "__fish_seen_subcommand_from init" -l target-dir -d "Target directory (default: current)" -r -F
complete -c cdc -n "__fish_seen_subcommand_from init" -l git-init -d "Initialize git repository"


# manage-service subcommand options
# Dynamic service completion - lists available services from 2-services/*.yaml
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l service -d "Service name from 2-services/*.yaml" -r -f -a "(
    # Check if we're in an implementation directory (adopus-cdc-pipeline, asma-cdc-pipeline)
    set -l services_dir (pwd | string match -r '.*/(adopus-cdc-pipeline|asma-cdc-pipeline)' | head -n1)
    if test -n \"\$services_dir\"
        # We're in an implementation directory
        set services_dir (pwd | string replace -r '/(adopus-cdc-pipeline|asma-cdc-pipeline).*' '/\$1/2-services')
    else if test -d 2-services
        # We're at the root of an implementation
        set services_dir 2-services
    else if test -d ../2-services
        # We're in a subdirectory
        set services_dir ../2-services
    else if test -d ../../2-services
        # We're deeper in subdirectories
        set services_dir ../../2-services
    else
        # Fallback - try to find from current working directory
        set services_dir (find (pwd) -maxdepth 3 -type d -name '2-services' 2>/dev/null | head -n1)
    end
    
    if test -d \"\$services_dir\"
        for file in \$services_dir/*.yaml
            if test -f \"\$file\"
                basename \$file .yaml
            end
        end
    end
)"
complete -c cdc -n "__fish_seen_subcommand_from manage-service" -l create-service -d "Create a new service configuration file"

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
        # Find service-schemas directory
        set -l schemas_dir
        if test -d service-schemas
            set schemas_dir service-schemas
        else if test -d ../service-schemas
            set schemas_dir ../service-schemas
        else if test -d ../../service-schemas
            set schemas_dir ../../service-schemas
        else
            set schemas_dir (find (pwd) -maxdepth 3 -type d -name 'service-schemas' 2>/dev/null | head -n1)
        end
        
        if test -d \"\$schemas_dir/\$service_name\"
            # List all tables in format schema.TableName
            for schema_dir in \$schemas_dir/\$service_name/*/
                if test -d \"\$schema_dir\"
                    set -l schema_name (basename \$schema_dir)
                    for table_file in \$schema_dir/*.yaml
                        if test -f \"\$table_file\"
                            set -l table_name (basename \$table_file .yaml)
                            echo \"\$schema_name.\$table_name\"
                        end
                    end
                end
            end
        end
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

# manage-server-group subcommand options
# General actions
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l update -d "Update server group from database inspection"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l list -d "List all server groups"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l info -d "Show detailed server group information"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l create -d "Create new server group" -r

# Exclude patterns
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l add-to-ignore-list -d "Add pattern(s) to database exclude list" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l list-ignore-patterns -d "List current database exclude patterns"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l add-to-schema-excludes -d "Add pattern(s) to schema exclude list" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group" -l list-schema-excludes -d "List current schema exclude patterns"

# Creation flags (only show when --create is present in the command line)
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_has_manage_server_group_create" -l pattern -d "Server group pattern" -r -f -a "db-per-tenant db-shared"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_has_manage_server_group_create" -l source-type -d "Source database type" -r -f -a "postgres mssql"
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_has_manage_server_group_create" -l host -d "Database host (use \${VAR} for env vars)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_has_manage_server_group_create" -l port -d "Database port" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_has_manage_server_group_create" -l user -d "Database user (use \${VAR} for env vars)" -r
complete -c cdc -n "__fish_seen_subcommand_from manage-server-group; and __cdc_has_manage_server_group_create" -l password -d "Database password (use \${VAR} for env vars)" -r

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
