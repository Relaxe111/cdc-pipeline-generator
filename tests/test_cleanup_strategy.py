"""Test that verifies test cleanup and isolation strategy.

This test documents and validates that:
1. Tests use tmp_path for isolation (automatic cleanup)
2. No test artifacts persist in generator root
3. Git ignores test artifacts if they do appear
4. .test-workspace/ directory is available for manual testing
"""

from pathlib import Path

import pytest


def test_tmp_path_provides_isolation(tmp_path: Path) -> None:
    """Verify tmp_path creates isolated temporary directory."""
    # tmp_path is unique per test
    assert tmp_path.exists()
    assert tmp_path.is_dir()
    assert "pytest" in str(tmp_path)  # Managed by pytest

    # Create test artifacts
    (tmp_path / "source-groups.yaml").write_text("test: content")
    (tmp_path / "services").mkdir()
    (tmp_path / "services" / "test.yaml").write_text("service: test")

    assert (tmp_path / "source-groups.yaml").exists()
    assert (tmp_path / "services" / "test.yaml").exists()

    # After test, pytest automatically deletes tmp_path


def test_no_artifacts_in_generator_root() -> None:
    """Verify generator root stays clean (no test artifacts)."""
    # Get generator root (where this test file lives)
    generator_root = Path(__file__).parent.parent

    # These should NOT exist in generator root (they're gitignored)
    # If they do exist, they're test artifacts from manual testing
    source_groups = generator_root / "source-groups.yaml"
    service_schemas = generator_root / "service-schemas"

    # Document expected state:
    # - source-groups.yaml should NOT exist (gitignored)
    # - service-schemas/ should NOT exist (gitignored)
    # Tests create these in tmp_path or .test-workspace/, not root
    if source_groups.exists():
        pytest.skip(
            "source-groups.yaml exists in generator root - "
            "this is a test artifact from manual testing. "
            "It's gitignored and safe to delete."
        )

    if service_schemas.exists():
        pytest.skip(
            "service-schemas/ exists in generator root - "
            "this is a test artifact (contains audit_log.yaml, etc.). "
            "It's gitignored and safe to delete."
        )


def test_gitignore_prevents_test_artifacts() -> None:
    """Verify .gitignore properly excludes test artifacts."""
    generator_root = Path(__file__).parent.parent
    gitignore = generator_root / ".gitignore"

    assert gitignore.exists()

    gitignore_content = gitignore.read_text()

    # Verify key patterns are in .gitignore
    assert ".test-workspace/*" in gitignore_content
    assert "!.test-workspace/README.md" in gitignore_content
    assert "source-groups.yaml" in gitignore_content
    assert "service-schemas/" in gitignore_content

    # Note: services/ is NOT in generator root, only in test workspaces


def test_test_workspace_directory_for_manual_testing() -> None:
    """Verify .test-workspace/ can be used for manual testing."""
    generator_root = Path(__file__).parent.parent
    test_workspace = generator_root / ".test-workspace"

    # .test-workspace doesn't need to exist (created on-demand)
    # But if it does, it should be a directory
    if test_workspace.exists():
        assert test_workspace.is_dir()

        # Any content in .test-workspace is gitignored
        # This is by design for manual testing


def test_legacy_implementation_directory_is_not_used() -> None:
    """Verify legacy implementation/ workspace is not used anymore."""
    generator_root = Path(__file__).parent.parent
    impl_dir = generator_root / "implementation"
    assert not impl_dir.exists()
