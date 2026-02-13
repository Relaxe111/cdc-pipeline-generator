"""Tests for scaffold template source path resolution and copy behavior."""

from __future__ import annotations

from pathlib import Path

import pytest

from cdc_generator.validators.manage_server_group.scaffolding import create, update


def _set_fake_generator_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Point cdc_generator.__file__ to a temporary fake package root."""
    import cdc_generator

    fake_pkg_dir = tmp_path / "fakepkg" / "cdc_generator"
    fake_pkg_dir.mkdir(parents=True, exist_ok=True)
    fake_init = fake_pkg_dir / "__init__.py"
    fake_init.write_text("# fake package root\n")

    monkeypatch.setattr(cdc_generator, "__file__", str(fake_init))
    return fake_pkg_dir


def _prepare_project_root(tmp_path: Path) -> Path:
    """Create minimal implementation folders required by copy helpers."""
    project_root = tmp_path / "impl"
    (project_root / "services" / "_schemas" / "_definitions").mkdir(
        parents=True,
        exist_ok=True,
    )
    return project_root


def _write_templates(base: Path) -> None:
    """Write template files under a given template root path."""
    base.mkdir(parents=True, exist_ok=True)
    (base / "column-templates.yaml").write_text("templates: {}\n")
    (base / "transform-rules.yaml").write_text("rules: {}\n")


class TestScaffoldTemplatePathResolution:
    """Scaffold should work with both new and legacy template locations."""

    def test_update_copies_from_new_services_schemas_location(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """If templates move to templates/init/services/_schemas, copy still works."""
        generator_root = _set_fake_generator_root(monkeypatch, tmp_path)
        _write_templates(generator_root / "templates" / "init" / "services" / "_schemas")

        project_root = _prepare_project_root(tmp_path)
        update._copy_template_library_files(project_root)

        assert (project_root / "services" / "_schemas" / "column-templates.yaml").exists()
        assert (project_root / "services" / "_schemas" / "transform-rules.yaml").exists()

    def test_create_copies_from_legacy_service_schemas_location(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Legacy templates/init/service-schemas location remains supported."""
        generator_root = _set_fake_generator_root(monkeypatch, tmp_path)
        _write_templates(generator_root / "templates" / "init" / "service-schemas")

        project_root = _prepare_project_root(tmp_path)
        create._copy_template_library_files(project_root)

        assert (project_root / "services" / "_schemas" / "column-templates.yaml").exists()
        assert (project_root / "services" / "_schemas" / "transform-rules.yaml").exists()

    def test_update_does_not_warn_when_target_templates_already_exist(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """No missing-template warning when target files already exist in project."""
        _set_fake_generator_root(monkeypatch, tmp_path)
        project_root = _prepare_project_root(tmp_path)

        (project_root / "services" / "_schemas" / "column-templates.yaml").write_text(
            "templates: {}\n"
        )
        (project_root / "services" / "_schemas" / "transform-rules.yaml").write_text(
            "rules: {}\n"
        )

        update._copy_template_library_files(project_root)

        output = capsys.readouterr().out
        assert "Template not found in generator: column-templates.yaml" not in output
        assert "Template not found in generator: transform-rules.yaml" not in output
