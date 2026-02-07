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


class SinkTableConfig(TypedDict, total=False):
    """Configuration for a single table in a service sink.

    Attributes:
        target_exists: REQUIRED. Whether the target table already exists in the sink.
            - false: Clone table as-is (CREATE TABLE from source schema).
            - true: Map to existing table (requires target + columns).
        target: Target table reference (schema.table) when target_exists=true.
        target_schema: Override target schema (when target_exists=false).
        columns: Column mapping {source_col: target_col} when target_exists=true.
        include_columns: Only sync these columns (when target_exists=false).

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

        # Map to existing table
        public.attachments:
            target_exists: true
            target: public.chat_attachments
            columns:
                id: attachment_id
                name: file_name
    """

    target_exists: bool  # REQUIRED - use Required[bool] in Python 3.11+
    target: str
    target_schema: str
    columns: dict[str, str]
    include_columns: list[str]


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
