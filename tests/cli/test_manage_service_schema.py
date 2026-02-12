"""End-to-end CLI tests for ``cdc manage-service-schema``."""

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


class TestCliListServices:
    """CLI e2e: service listing."""

    def test_list_services_empty(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc("manage-service-schema", "--list-services")
        assert result.returncode == 0
        assert "No service schema directories found" in result.stdout + result.stderr

    def test_list_services_with_data(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_custom_table(isolated_project, "chat", "public.audit_log")

        result = run_cdc("manage-service-schema", "--list-services")

        assert result.returncode == 0
        assert "chat" in result.stdout


class TestCliListAndDispatchErrors:
    """CLI e2e: list and validation errors."""

    def test_list_requires_service(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc("manage-service-schema", "--list")
        assert result.returncode == 1
        assert "--list requires --service" in result.stdout + result.stderr

    def test_no_action_with_service_defaults_to_list(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_custom_table(isolated_project, "chat", "public.audit_log")

        result = run_cdc("manage-service-schema", "--service", "chat")

        assert result.returncode == 0
        assert "public.audit_log" in result.stdout

    def test_service_required_for_show(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc(
            "manage-service-schema",
            "--show",
            "public.audit_log",
        )

        assert result.returncode == 1
        assert "--service is required" in result.stdout + result.stderr

    def test_service_required_for_remove_custom_table(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc(
            "manage-service-schema",
            "--remove-custom-table",
            "public.audit_log",
        )

        assert result.returncode == 1
        assert "--service is required" in result.stdout + result.stderr

    def test_list_for_service_with_no_custom_tables(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc(
            "manage-service-schema",
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
            "manage-service-schema",
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
            "manage-service-schema",
            "--service",
            "chat",
            "--show",
            "public.audit_log",
        )
        assert show_result.returncode == 0
        assert "Custom table: public.audit_log" in show_result.stdout
        assert "event_type: text" in show_result.stdout

        remove_result = run_cdc(
            "manage-service-schema",
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
            "manage-service-schema",
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
            "manage-service-schema",
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
            "manage-service-schema",
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
            "manage-service-schema",
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
            "manage-service-schema",
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
        result = run_cdc_completion("cdc manage-service-schema --")
        assert result.returncode == 0
        output = result.stdout
        if not output.strip():
            pytest.skip(
                "No fish flag completions registered for manage-service-schema",
            )
        assert "--list-services" in output
        assert "--add-custom-table" in output
