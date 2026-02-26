"""Unit tests for the dispatch routing logic in service.py.

Covers _dispatch, _dispatch_validation, _dispatch_inspect,
_dispatch_extra_columns, _dispatch_sink, _dispatch_sink_conditional,
and _dispatch_source.
"""

import argparse
from pathlib import Path
from unittest.mock import patch

import pytest

from cdc_generator.cli.service import (
    _dispatch,
    _dispatch_extra_columns,
    _dispatch_sink,
    _dispatch_sink_conditional,
    _dispatch_source,
    _dispatch_validation,
)
from tests._namespace_defaults import make_namespace

# project_dir fixture is provided by tests/conftest.py


@pytest.fixture()
def service_yaml(project_dir: Path) -> Path:
    """Service YAML with tables and a sink."""
    sf = project_dir / "services" / "proxy.yaml"
    sf.write_text(
        "proxy:\n"
        "  source:\n"
        "    tables:\n"
        "      public.queries: {}\n"
        "      public.users: {}\n"
        "  sinks:\n"
        "    sink_asma.chat:\n"
        "      tables: {}\n"
    )
    return sf


def _full_ns(**kwargs: object) -> argparse.Namespace:
    """Build a complete argparse.Namespace for dispatch (skip_validation=False)."""
    return make_namespace(skip_validation=False, **kwargs)


# ═══════════════════════════════════════════════════════════════════════════
# _dispatch_validation
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchValidation:
    """Tests for _dispatch_validation routing."""

    def test_routes_list_source_tables(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--list-source-tables dispatches correctly."""
        args = _full_ns(list_source_tables=True)
        result = _dispatch_validation(args)
        assert result is not None
        assert result == 0

    def test_routes_create_service(
        self, project_dir: Path,
    ) -> None:
        """--create-service dispatches to create handler."""
        args = _full_ns(create_service="proxy")
        result = _dispatch_validation(args)
        assert result is not None
        # Will be 0 (creates) or 1 (error), but was dispatched
        assert isinstance(result, int)

    def test_routes_validate_config(
        self, project_dir: Path,
    ) -> None:
        """--validate-config dispatches correctly."""
        sf = project_dir / "services" / "proxy.yaml"
        sf.write_text(
            "proxy:\n"
            "  source:\n"
            "    validation_database: proxy_dev\n"
            "    tables:\n"
            "      public.queries:\n"
            "        primary_key: id\n"
        )
        args = _full_ns(validate_config=True)
        result = _dispatch_validation(args)
        assert result is not None
        assert result == 0

    def test_returns_none_no_flags(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Returns None when no validation flags set."""
        args = _full_ns()
        result = _dispatch_validation(args)
        assert result is None

    def test_returns_none_no_service(self) -> None:
        """Returns None when no service and no validation flags."""
        args = _full_ns(service=None)
        result = _dispatch_validation(args)
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════
# _dispatch_extra_columns
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchExtraColumns:
    """Tests for _dispatch_extra_columns routing."""

    def test_returns_none_for_removed_global_template_rule_flags(self) -> None:
        """Removed global listing flags no longer dispatch in manage-service."""
        args = _full_ns(
            service=None,
            list_template_keys=True,
            list_transform_rule_keys=True,
        )
        result = _dispatch_extra_columns(args)
        assert result is None

    def test_returns_none_no_flags(
        self, project_dir: Path,
    ) -> None:
        """Returns None when no extra column flags."""
        args = _full_ns()
        result = _dispatch_extra_columns(args)
        assert result is None

    def test_returns_none_without_service(self) -> None:
        """Returns None when no service and non-global flags."""
        args = _full_ns(service=None, add_column_template="test")
        result = _dispatch_extra_columns(args)
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════
# _dispatch_sink
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchSink:
    """Tests for _dispatch_sink routing."""

    def test_routes_list_sinks(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--list-sinks dispatches."""
        args = _full_ns(list_sinks=True)
        result = _dispatch_sink(args)
        assert result is not None

    def test_routes_validate_sinks(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--validate-sinks dispatches."""
        args = _full_ns(validate_sinks=True)
        result = _dispatch_sink(args)
        assert result is not None

    def test_routes_add_sink(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--add-sink dispatches."""
        args = _full_ns(add_sink=["sink_asma.calendar"])
        result = _dispatch_sink(args)
        assert result is not None

    def test_routes_remove_sink(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--remove-sink dispatches."""
        args = _full_ns(remove_sink=["sink_asma.chat"])
        result = _dispatch_sink(args)
        assert result is not None

    def test_returns_none_no_sink_flags(
        self, project_dir: Path,
    ) -> None:
        """Returns None when no sink flags set."""
        args = _full_ns()
        result = _dispatch_sink(args)
        assert result is None

    def test_returns_none_without_service(self) -> None:
        """Returns None when no --service."""
        args = _full_ns(service=None)
        result = _dispatch_sink(args)
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════
# _dispatch_sink_conditional
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchSinkConditional:
    """Tests for _dispatch_sink_conditional routing."""

    def test_routes_remove_sink_table_with_sink(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--remove-sink-table + --sink dispatches."""
        args = _full_ns(
            remove_sink_table="public.users",
            sink="sink_asma.chat",
        )
        result = _dispatch_sink_conditional(args)
        assert result is not None

    def test_no_dispatch_remove_without_sink(self) -> None:
        """--remove-sink-table without --sink not dispatched."""
        args = _full_ns(remove_sink_table="public.users")
        result = _dispatch_sink_conditional(args)
        assert result is None

    def test_routes_update_schema_with_sink(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--update-schema + --sink dispatches."""
        args = _full_ns(
            update_schema="new_schema",
            sink="sink_asma.chat",
            sink_table="public.users",
        )
        result = _dispatch_sink_conditional(args)
        assert result is not None

    def test_routes_map_column_error(
        self, project_dir: Path,
    ) -> None:
        """--map-column + --sink without --add-sink-table → error."""
        args = _full_ns(
            map_column=[["id", "user_id"]],
            sink="sink_asma.chat",
        )
        result = _dispatch_sink_conditional(args)
        assert result == 1

    def test_returns_none_no_conditional_flags(self) -> None:
        """Returns None when no conditional sink flags."""
        args = _full_ns()
        result = _dispatch_sink_conditional(args)
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════
# _dispatch_source
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchSource:
    """Tests for _dispatch_source routing."""

    def test_routes_source_table(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--source-table dispatches to update handler."""
        args = _full_ns(
            source_table="public.queries",
            track_columns=["public.queries.status"],
        )
        result = _dispatch_source(args)
        assert result is not None

    def test_routes_add_source_table(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--add-source-table dispatches."""
        args = _full_ns(add_source_table="public.orders")
        result = _dispatch_source(args)
        assert result is not None

    def test_routes_add_source_tables(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--add-source-tables dispatches."""
        args = _full_ns(add_source_tables=["public.a", "public.b"])
        result = _dispatch_source(args)
        assert result is not None

    def test_routes_remove_table(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--remove-table dispatches."""
        args = _full_ns(remove_table="public.queries")
        result = _dispatch_source(args)
        assert result is not None

    def test_returns_none_no_source_flags(
        self, project_dir: Path,
    ) -> None:
        """Returns None when no source flags."""
        args = _full_ns()
        result = _dispatch_source(args)
        assert result is None

    def test_returns_none_without_service(self) -> None:
        """Returns None when no --service."""
        args = _full_ns(service=None)
        result = _dispatch_source(args)
        assert result is None

    def test_source_table_priority_over_add(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--source-table checked before --add-source-table."""
        args = _full_ns(
            source_table="public.queries",
            add_source_table="public.orders",
            track_columns=["public.queries.status"],
        )
        result = _dispatch_source(args)
        # Should dispatch source_table, not add
        assert result is not None

    def test_source_table_redirects_to_sink_when_sink_flag(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--source-table + --sink redirects to sink handler as --from."""
        args = _full_ns(
            source_table="public.users",
            sink="sink_asma.chat",
            add_sink_table=None,
            target_exists="false",
        )
        result = _dispatch_source(args)
        assert result is not None
        # source_table should have been moved to from_table
        assert args.from_table == "public.users"
        assert args.source_table is None

    def test_source_table_redirects_with_replicate_structure(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--source-table + --replicate-structure redirects to sink."""
        args = _full_ns(
            source_table="public.users",
            replicate_structure=True,
            sink="sink_asma.chat",
            sink_schema="chat",
        )
        result = _dispatch_source(args)
        assert result is not None
        assert args.from_table == "public.users"
        assert args.source_table is None

    def test_source_table_stays_source_without_sink_flags(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--source-table without sink flags stays in source handler."""
        args = _full_ns(
            source_table="public.queries",
            track_columns=["public.queries.status"],
        )
        result = _dispatch_source(args)
        assert result is not None
        # source_table should NOT have been moved
        assert args.source_table == "public.queries"
        assert args.from_table is None


# ═══════════════════════════════════════════════════════════════════════════
# _dispatch (top-level)
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatch:
    """Tests for the top-level _dispatch router."""

    def test_no_service_no_flags_returns_1(
        self, project_dir: Path,
    ) -> None:
        """No service → handle_no_service → 1."""
        args = _full_ns(service=None)
        result = _dispatch(args)
        assert result == 1

    def test_service_no_flags_goes_interactive(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Service with no action flags → interactive mode."""
        args = _full_ns()
        with patch(
            "cdc_generator.cli.service_handlers_misc.run_interactive_mode",
            return_value=0,
        ):
            result = _dispatch(args)
            assert isinstance(result, int)

    def test_validation_takes_priority(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Validation flags checked first."""
        args = _full_ns(
            list_source_tables=True,
            add_source_table="public.orders",
        )
        result = _dispatch(args)
        assert result == 0  # list_source_tables returns 0

    def test_source_after_sink(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Source flags dispatched if no sink flags active."""
        args = _full_ns(add_source_table="public.orders")
        result = _dispatch(args)
        assert result == 0
