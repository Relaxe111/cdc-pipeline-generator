"""End-to-end CLI tests for ``cdc manage-services schema custom-tables``."""

from pathlib import Path

import pytest

from tests.cli.conftest import RunCdc, RunCdcCompletion

pytestmark = pytest.mark.cli


def _service_schema_dir(root: Path, service: str) -> Path:
    return root / "service-schemas" / service / "custom-tables"


def _write_custom_table(root: Path, service: str, table_ref: str) -> None:
    schema, table = table_ref.split(".", 1)
    custom_dir = _service_schema_dir(root, service)
    custom_dir.mkdir(parents=True, exist_ok=True)
    (custom_dir / f"{schema}.{table}.yaml").write_text(
        "database: null\n"
        f"schema: {schema}\n"
        f"service: {service}\n"
        f"table: {table}\n"
        "custom: true\n"
        "columns:\n"
        "  - name: id\n"
        "    type: uuid\n"
        "    nullable: false\n"
        "    primary_key: true\n"
        "primary_key: id\n"
    )


def _write_pgsql_definitions(root: Path) -> None:
    definitions_dir = root / "services" / "_schemas" / "_definitions"
    definitions_dir.mkdir(parents=True, exist_ok=True)
    (definitions_dir / "pgsql.yaml").write_text(
        "categories:\n"
        "  numeric:\n"
        "    types:\n"
        "      - bigint\n"
        "      - integer\n"
        "      - numeric\n"
        "    defaults:\n"
        "      - default_0\n"
        "  text:\n"
        "    types:\n"
        "      - text\n"
        "    defaults:\n"
        "      - default_empty\n"
        "  date_time:\n"
        "    types:\n"
        "      - date\n"
        "      - timestamp\n"
        "      - timestamptz\n"
        "  uuid:\n"
        "    types:\n"
        "      - uuid\n"
        "type_defaults:\n"
        "  uuid:\n"
        "    - default_uuid\n"
        "    - default_gen_random_uuid\n"
        "  date:\n"
        "    - default_current_date\n"
        "  timestamp:\n"
        "    - default_now\n"
        "    - default_current_timestamp\n"
        "  timestamptz:\n"
        "    - default_now\n"
        "    - default_current_timestamp\n"
    )


class TestCliListServices:
    """CLI e2e: service listing."""

    def test_list_services_empty(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc(
            "manage-services", "schema", "custom-tables", "--list-services",
        )
        assert result.returncode == 0
        assert "No service schema directories found" in result.stdout + result.stderr

    def test_list_services_with_data(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_custom_table(isolated_project, "chat", "public.audit_log")

        result = run_cdc(
            "manage-services", "schema", "custom-tables", "--list-services",
        )

        assert result.returncode == 0
        assert "chat" in result.stdout


class TestCliListAndDispatchErrors:
    """CLI e2e: list and validation errors."""

    def test_list_requires_service(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc(
            "manage-services", "schema", "custom-tables", "--list",
        )
        assert result.returncode == 1
        assert "--list requires --service" in result.stdout + result.stderr

    def test_no_action_with_service_defaults_to_list(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_custom_table(isolated_project, "chat", "public.audit_log")

        result = run_cdc(
            "manage-services", "schema", "custom-tables", "--service", "chat",
        )

        assert result.returncode == 0
        assert "public.audit_log" in result.stdout

    def test_service_required_for_show(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc(
            "manage-services",
            "schema",
            "custom-tables",
            "--show",
            "public.audit_log",
        )

        assert result.returncode == 1
        assert "--service is required" in result.stdout + result.stderr

    def test_service_required_for_remove_custom_table(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc(
            "manage-services",
            "schema",
            "custom-tables",
            "--remove-custom-table",
            "public.audit_log",
        )

        assert result.returncode == 1
        assert "--service is required" in result.stdout + result.stderr

    def test_list_for_service_with_no_custom_tables(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc(
            "manage-services",
            "schema",
            "custom-tables",
            "--service",
            "chat",
            "--list",
        )

        assert result.returncode == 0
        assert "No custom tables for service 'chat'" in result.stdout + result.stderr


class TestCliCustomTableCrud:
    """CLI e2e: add/show/remove custom tables."""

    def test_add_show_remove_custom_table(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        add_result = run_cdc(
            "manage-services",
            "schema",
            "custom-tables",
            "--service",
            "chat",
            "--add-custom-table",
            "public.audit_log",
            "--column",
            "id:uuid:pk",
            "--column",
            "event_type:text:not_null",
        )
        assert add_result.returncode == 0

        show_result = run_cdc(
            "manage-services",
            "schema",
            "custom-tables",
            "--service",
            "chat",
            "--show",
            "public.audit_log",
        )
        assert show_result.returncode == 0
        assert "Custom table: public.audit_log" in show_result.stdout
        assert "event_type: text" in show_result.stdout

        remove_result = run_cdc(
            "manage-services",
            "schema",
            "custom-tables",
            "--service",
            "chat",
            "--remove-custom-table",
            "public.audit_log",
        )
        assert remove_result.returncode == 0

    def test_add_requires_column(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc(
            "manage-services",
            "schema",
            "custom-tables",
            "--service",
            "chat",
            "--add-custom-table",
            "public.audit_log",
        )
        assert result.returncode == 1
        assert "requires at least one --column" in result.stdout + result.stderr

    def test_invalid_table_ref_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc(
            "manage-services",
            "schema",
            "custom-tables",
            "--service",
            "chat",
            "--add-custom-table",
            "audit_log",
            "--column",
            "id:uuid:pk",
        )
        assert result.returncode == 1
        assert "Invalid table reference" in result.stdout + result.stderr

    def test_show_missing_custom_table_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc(
            "manage-services",
            "schema",
            "custom-tables",
            "--service",
            "chat",
            "--show",
            "public.missing",
        )

        assert result.returncode == 1
        assert "not found" in result.stdout + result.stderr

    def test_remove_missing_custom_table_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc(
            "manage-services",
            "schema",
            "custom-tables",
            "--service",
            "chat",
            "--remove-custom-table",
            "public.missing",
        )

        assert result.returncode == 1
        assert "not found" in result.stdout + result.stderr

    def test_add_with_unknown_modifier_succeeds(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc(
            "manage-services",
            "schema",
            "custom-tables",
            "--service",
            "chat",
            "--add-custom-table",
            "public.audit_log",
            "--column",
            "id:uuid:pk",
            "--column",
            "name:text:weird_modifier",
        )

        assert result.returncode == 0


class TestCliCompletions:
    """CLI e2e: fish completion entries."""

    def test_flag_completion(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        result = run_cdc_completion("cdc manage-services schema --")
        assert result.returncode == 0
        output = result.stdout
        if not output.strip():
            pytest.skip(
                "No fish flag completions registered for manage-services schema",
            )
        assert "custom-tables" in output
        assert "column-templates" in output
        assert "transforms" in output

    def test_custom_tables_service_completion_uses_schema_services(
        self,
        run_cdc_completion: RunCdcCompletion,
        isolated_project: Path,
    ) -> None:
        """--service completes from schema dirs, not services/*.yaml."""
        # Service config exists, but should NOT drive this completion.
        services_dir = isolated_project / "services"
        services_dir.mkdir(parents=True, exist_ok=True)
        (services_dir / "directory.yaml").write_text(
            "directory:\n"
            "  source:\n"
            "    tables: {}\n"
        )

        # Schema services should drive completion values.
        (services_dir / "_schemas" / "notification" / "custom-tables").mkdir(
            parents=True, exist_ok=True,
        )
        (services_dir / "_schemas" / "calendar" / "custom-tables").mkdir(
            parents=True, exist_ok=True,
        )

        result = run_cdc_completion(
            "cdc manage-services schema custom-tables --service n"
        )
        assert result.returncode == 0
        output = result.stdout
        assert "notification" in output
        assert "directory" not in output

    def test_custom_tables_column_completion_suggests_types(
        self,
        run_cdc_completion: RunCdcCompletion,
        isolated_project: Path,
    ) -> None:
        """--column id: suggests PostgreSQL type candidates."""
        definitions_dir = isolated_project / "services" / "_schemas" / "_definitions"
        definitions_dir.mkdir(parents=True, exist_ok=True)
        (definitions_dir / "pgsql.yaml").write_text(
            "categories:\n"
            "  text:\n"
            "    types:\n"
            "      - text\n"
            "      - custom_text_type\n"
            "  uuid:\n"
            "    types:\n"
            "      - uuid\n"
        )

        result = run_cdc_completion(
            "cdc manage-services schema custom-tables "
            + "--service notification "
            + "--add-custom-table some_schema.test "
            + "--column id:"
        )
        assert result.returncode == 0
        output = result.stdout
        assert "id:uuid" in output
        assert "id:text" in output
        assert "id:custom_text_type" in output
        assert "id:bytea" not in output

    def test_custom_tables_column_completion_suggests_modifiers(
        self,
        run_cdc_completion: RunCdcCompletion,
        isolated_project: Path,
    ) -> None:
        """--column id:uuid: suggests modifier candidates."""
        _write_pgsql_definitions(isolated_project)

        result = run_cdc_completion(
            "cdc manage-services schema custom-tables "
            + "--service notification "
            + "--add-custom-table some_schema.test "
            + "--column id:uuid:"
        )
        assert result.returncode == 0
        output = result.stdout
        assert "id:uuid:pk" in output
        assert "id:uuid:not_null" in output
        assert "id:uuid:default_uuid" in output
        assert "id:uuid:default_now" not in output
        assert "id:uuid:default_current_date" not in output

    def test_custom_tables_column_completion_filters_incompatible_defaults(
        self,
        run_cdc_completion: RunCdcCompletion,
        isolated_project: Path,
    ) -> None:
        """BIGINT suggests numeric default alias and filters incompatible ones."""
        _write_pgsql_definitions(isolated_project)

        result = run_cdc_completion(
            "cdc manage-services schema custom-tables "
            + "--service notification "
            + "--add-custom-table some_schema.test "
            + "--column id:bigint:"
        )
        assert result.returncode == 0
        output = result.stdout
        assert "id:bigint:pk" in output
        assert "id:bigint:not_null" in output
        assert "id:bigint:nullable" in output
        assert "id:bigint:default_0" in output
        assert "id:bigint:default_now" not in output
        assert "id:bigint:default_current_timestamp" not in output
        assert "id:bigint:default_current_date" not in output
        assert "id:bigint:default_uuid" not in output

    def test_custom_tables_column_completion_hides_conflicting_modifiers_after_nullable(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """After nullable, don't suggest pk or not_null."""
        result = run_cdc_completion(
            "cdc manage-services schema custom-tables "
            + "--service notification "
            + "--add-custom-table some_schema.test "
            + "--column user_id:bigint:nullable:"
        )
        assert result.returncode == 0
        output = result.stdout
        assert "user_id:bigint:nullable:pk" not in output
        assert "user_id:bigint:nullable:not_null" not in output

    def test_custom_tables_column_completion_hides_nullable_after_pk(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """After pk, don't suggest nullable."""
        result = run_cdc_completion(
            "cdc manage-services schema custom-tables "
            + "--service notification "
            + "--add-custom-table some_schema.test "
            + "--column user_id:bigint:pk:"
        )
        assert result.returncode == 0
        output = result.stdout
        assert "user_id:bigint:pk:nullable" not in output
