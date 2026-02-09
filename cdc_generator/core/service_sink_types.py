"""Type definitions for service-level sink configurations.

Service sinks define WHERE source tables are sent and HOW they are mapped.
Lives in services/*.yaml under the `sinks:` key.

Sink key format: {sink_group}.{target_service}
    - sink_group: references sink-groups.yaml top-level key (e.g., sink_asma)
    - target_service: target service/database within that sink group

Examples:
    sinks:
      sink_asma.chat:              # inherited sink (envs auto-matched)
        tables:
          public.customer_user: {}                    # clone as-is (target_exists: false)
          public.attachments:                         # map to existing target
            target_exists: true
            target: public.chat_attachments
            columns:
              id: attachment_id
              name: file_name
"""

from typing import TypedDict


class SinkColumnMapping(TypedDict, total=False):
    """Column mapping for a sink table.

    Keys are source column names, values are target column names.

    Example:
        columns:
          id: attachment_id
          name: file_name
    """


class CustomColumnDefinition(TypedDict, total=False):
    """Column definition for a custom sink table.

    Used when custom: true — defines the column structure for auto-creation.

    Attributes:
        type: PostgreSQL column type (e.g., uuid, text, bigint, timestamptz).
        nullable: Whether the column allows NULL (default: true).
        primary_key: Whether this column is part of the primary key.
        default: Default value expression (e.g., now(), gen_random_uuid()).

    Example:
        columns:
          id:
            type: uuid
            primary_key: true
            default: gen_random_uuid()
          event_type:
            type: text
            nullable: false
    """

    type: str
    nullable: bool
    primary_key: bool
    default: str


class ExtraColumnEntry(TypedDict, total=False):
    """A single column template reference in a sink table config.

    Attributes:
        template: Column template key from column-templates.yaml.
        name: Optional column name override (default: template's name).

    Example:
        column_templates:
          - template: source_table
          - template: environment
            name: deploy_env
    """

    template: str
    name: str


class TransformEntry(TypedDict, total=False):
    """A single transform rule reference in a sink table config.

    Attributes:
        rule: Transform rule key from transform-rules.yaml.

    Example:
        transforms:
          - rule: user_class_splitter
          - rule: active_users_only
    """

    rule: str


class SinkTableConfig(TypedDict, total=False):
    """Configuration for a single table in a service sink.

    Attributes:
        target_exists: REQUIRED. Whether the target table already exists in the sink.
            - false: Clone table as-is (CREATE TABLE from source schema).
            - true: Map to existing table (requires target + columns).
        from: Source table reference (schema.table) that this sink table reads from.
            If omitted, defaults to the sink table key name. Required when sink table
            name differs from source table name.
        target: Target table reference (schema.table) when target_exists=true.
        target_schema: Override target schema (when target_exists=false).
        columns: Column mapping {source_col: target_col} when target_exists=true,
            OR column definitions {col_name: CustomColumnDefinition} when custom=true.
        include_columns: Only sync these columns (when target_exists=false).
        custom: True if the table was manually created (not from source schemas).
        managed: True if the table can be modified via CLI (only for custom tables).
        replicate_structure: When true, auto-create the sink table with exact
            structure from source schema, applying type mapping via adapters.
            Reads source schema from service-schemas/{service}/{schema}/{table}.yaml
            and converts column types using adapters (auto-deduced from source/sink groups).

    Examples:
        # Clone as-is (must specify target_exists: false)
        public.customer_user:
            target_exists: false

        # Clone with schema override
        public.customer_user:
            target_exists: false
            target_schema: directory

        # Clone with column subset
        public.customer_user:
            target_exists: false
            include_columns: [brukerBrukerNavn, created_at, pnr]

        # Replicate structure with auto-deduced type mapping
        public.customer_user:
            from: public.customer_user  # explicit source reference
            target_exists: false
            replicate_structure: true

        # Map to different sink table name
        other_schema.manage_audits:
            from: logs.audit_queue      # source table differs from sink table
            target_exists: false

        # Map to existing table
        public.attachments:
            target_exists: true
            target: public.chat_attachments
            columns:
                id: attachment_id
                name: file_name

        # Custom table (auto-created in sink)
        public.audit_log:
            target_exists: false
            custom: true
            managed: true
            columns:
                id:
                    type: uuid
                    primary_key: true
                    default: gen_random_uuid()
                event_type:
                    type: text
                    nullable: false
    """

    target_exists: bool  # REQUIRED - use Required[bool] in Python 3.11+
    target: str
    target_schema: str
    columns: dict[str, str] | dict[str, CustomColumnDefinition]
    include_columns: list[str]
    custom: bool
    managed: bool
    replicate_structure: bool
    column_templates: list[ExtraColumnEntry]
    transforms: list[TransformEntry]


# Add 'from' field using __annotations__ to avoid Python keyword conflict
SinkTableConfig.__annotations__["from"] = str


class SinkDatabaseMapping(TypedDict, total=False):
    """Per-environment database mapping for standalone sinks.

    Only needed for standalone sink groups that don't inherit
    environment-to-database mappings from source groups.

    Example:
        databases:
          nonprod: analytics_nonprod
          prod: analytics_prod
    """


class ServiceSinkConfig(TypedDict, total=False):
    """Configuration for a service sink destination.

    Sink key format: {sink_group}.{target_service}

    Attributes:
        tables: Mapping of source tables → sink table configuration.
        databases: Per-env database mapping (standalone sinks only).

    Example:
        sink_asma.chat:
          tables:
            public.customer_user: {}
            public.attachments:
              target_exists: true
              target: public.chat_attachments
              columns:
                id: attachment_id
    """

    tables: dict[str, SinkTableConfig]
    databases: dict[str, str]
