"""Tests for source_ref_resolver module - source-group reference resolution."""

from typing import Any

import pytest  # type: ignore[import-not-found]

from cdc_generator.core.source_ref_resolver import (
    SourceRef,
    SourceRefError,
    is_source_ref,
    parse_source_ref,
    resolve_source_ref,
    validate_all_sources_have_key,
    validate_source_ref_for_all_envs,
)

# ---------------------------------------------------------------------------
# Fixtures - source-group configs
# ---------------------------------------------------------------------------


def _make_db_shared_config() -> dict[str, Any]:
    """Source-group config for db-shared pattern (like asma)."""
    return {
        "asma": {
            "pattern": "db-shared",
            "database_type": "postgres",
            "sources": {
                "directory": {
                    "customer_id": "3",
                    "schemas": ["public", "revman"],
                    "dev": {"database": "directory_dev", "host": "db-dev"},
                    "prod": {"database": "directory_prod", "host": "db-prod"},
                },
                "activities": {
                    "customer_id": "5",
                    "schemas": ["public"],
                    "dev": {"database": "activities_dev", "host": "db-dev"},
                    "prod": {"database": "activities_prod", "host": "db-prod"},
                },
            },
        },
    }


def _make_db_per_tenant_config() -> dict[str, Any]:
    """Source-group config for db-per-tenant pattern (like adopus)."""
    return {
        "adopus": {
            "pattern": "db-per-tenant",
            "database_type": "mssql",
            "sources": {
                "AVProd": {
                    "customer_id": "1",
                    "dev": {"database": "AVProd_dev"},
                    "prod": {"database": "AVProd_prod"},
                },
                "BVProd": {
                    "customer_id": "2",
                    "dev": {"database": "BVProd_dev"},
                    "prod": {"database": "BVProd_prod"},
                },
            },
        },
    }


def _make_missing_key_config() -> dict[str, Any]:
    """Source-group config where a key is missing in one env."""
    return {
        "broken": {
            "pattern": "db-shared",
            "sources": {
                "svc": {
                    "schemas": ["public"],
                    "dev": {"database": "svc_dev", "host": "db-dev"},
                    "prod": {"database": "svc_prod"},  # missing 'host'
                },
            },
        },
    }


# ---------------------------------------------------------------------------
# is_source_ref tests
# ---------------------------------------------------------------------------


class TestIsSourceRef:
    """Tests for is_source_ref()."""

    def test_valid_reference(self) -> None:
        assert is_source_ref("{asma.sources.*.customer_id}") is True

    def test_valid_reference_with_underscores(self) -> None:
        assert is_source_ref("{my_group.sources.*.my_key}") is True

    def test_not_a_reference_plain_text(self) -> None:
        assert is_source_ref("some_value") is False

    def test_not_a_reference_env_var(self) -> None:
        assert is_source_ref("${TENANT_ID}") is False

    def test_not_a_reference_bloblang(self) -> None:
        assert is_source_ref('meta("table")') is False

    def test_not_a_reference_missing_braces(self) -> None:
        assert is_source_ref("asma.sources.*.customer_id") is False

    def test_not_a_reference_wrong_keyword(self) -> None:
        assert is_source_ref("{asma.targets.*.customer_id}") is False

    def test_not_a_reference_no_wildcard(self) -> None:
        assert is_source_ref("{asma.sources.directory.customer_id}") is False

    def test_not_a_reference_empty(self) -> None:
        assert is_source_ref("") is False

    def test_not_a_reference_partial_braces(self) -> None:
        assert is_source_ref("{asma.sources.*.customer_id") is False


# ---------------------------------------------------------------------------
# parse_source_ref tests
# ---------------------------------------------------------------------------


class TestParseSourceRef:
    """Tests for parse_source_ref()."""

    def test_parse_valid(self) -> None:
        ref = parse_source_ref("{asma.sources.*.customer_id}")
        assert ref is not None
        assert ref.group == "asma"
        assert ref.key == "customer_id"
        assert ref.raw == "{asma.sources.*.customer_id}"

    def test_parse_different_group(self) -> None:
        ref = parse_source_ref("{adopus.sources.*.database}")
        assert ref is not None
        assert ref.group == "adopus"
        assert ref.key == "database"

    def test_parse_invalid_returns_none(self) -> None:
        assert parse_source_ref("not_a_ref") is None

    def test_parse_env_var_returns_none(self) -> None:
        assert parse_source_ref("${TENANT_ID}") is None

    def test_parse_empty_returns_none(self) -> None:
        assert parse_source_ref("") is None


# ---------------------------------------------------------------------------
# resolve_source_ref tests
# ---------------------------------------------------------------------------


class TestResolveSourceRef:
    """Tests for resolve_source_ref()."""

    def test_resolve_source_level_key(self) -> None:
        """Key found directly on the source (e.g., customer_id)."""
        config = _make_db_shared_config()
        ref = SourceRef(group="asma", key="customer_id", raw="{asma.sources.*.customer_id}")
        result = resolve_source_ref(ref, "directory", config=config)
        assert result == "3"

    def test_resolve_source_level_different_source(self) -> None:
        config = _make_db_shared_config()
        ref = SourceRef(group="asma", key="customer_id", raw="{asma.sources.*.customer_id}")
        result = resolve_source_ref(ref, "activities", config=config)
        assert result == "5"

    def test_resolve_env_level_key(self) -> None:
        """Key found at env level (e.g., database under dev)."""
        config = _make_db_shared_config()
        ref = SourceRef(group="asma", key="database", raw="{asma.sources.*.database}")
        result = resolve_source_ref(ref, "directory", env="dev", config=config)
        assert result == "directory_dev"

    def test_resolve_env_level_prod(self) -> None:
        config = _make_db_shared_config()
        ref = SourceRef(group="asma", key="database", raw="{asma.sources.*.database}")
        result = resolve_source_ref(ref, "directory", env="prod", config=config)
        assert result == "directory_prod"

    def test_resolve_db_per_tenant(self) -> None:
        config = _make_db_per_tenant_config()
        ref = SourceRef(group="adopus", key="customer_id", raw="{adopus.sources.*.customer_id}")
        result = resolve_source_ref(ref, "AVProd", config=config)
        assert result == "1"

    def test_source_level_takes_precedence(self) -> None:
        """Source-level key wins over env-level key."""
        config = _make_db_shared_config()
        ref = SourceRef(group="asma", key="customer_id", raw="{asma.sources.*.customer_id}")
        # customer_id is at source level, should still resolve even with env
        result = resolve_source_ref(ref, "directory", env="dev", config=config)
        assert result == "3"

    def test_numeric_value_converted_to_string(self) -> None:
        """Numeric values should be returned as strings."""
        config: dict[str, Any] = {
            "grp": {
                "sources": {
                    "svc": {"port": 5432},
                },
            },
        }
        ref = SourceRef(group="grp", key="port", raw="{grp.sources.*.port}")
        result = resolve_source_ref(ref, "svc", config=config)
        assert result == "5432"

    # Error cases

    def test_group_not_found_raises(self) -> None:
        config = _make_db_shared_config()
        ref = SourceRef(group="unknown", key="x", raw="{unknown.sources.*.x}")
        with pytest.raises(SourceRefError, match="Source group 'unknown' not found"):
            resolve_source_ref(ref, "directory", config=config)

    def test_group_not_found_shows_available(self) -> None:
        config = _make_db_shared_config()
        ref = SourceRef(group="unknown", key="x", raw="{unknown.sources.*.x}")
        with pytest.raises(SourceRefError, match="asma"):
            resolve_source_ref(ref, "directory", config=config)

    def test_source_not_found_raises(self) -> None:
        config = _make_db_shared_config()
        ref = SourceRef(group="asma", key="customer_id", raw="{asma.sources.*.customer_id}")
        with pytest.raises(SourceRefError, match="Source 'nonexistent' not found"):
            resolve_source_ref(ref, "nonexistent", config=config)

    def test_key_not_found_raises(self) -> None:
        config = _make_db_shared_config()
        ref = SourceRef(group="asma", key="missing_key", raw="{asma.sources.*.missing_key}")
        with pytest.raises(SourceRefError, match="Key 'missing_key' not found"):
            resolve_source_ref(ref, "directory", config=config)

    def test_key_not_found_shows_available_keys(self) -> None:
        config = _make_db_shared_config()
        ref = SourceRef(group="asma", key="missing_key", raw="{asma.sources.*.missing_key}")
        with pytest.raises(SourceRefError, match="customer_id"):
            resolve_source_ref(ref, "directory", config=config)

    def test_no_sources_section_raises(self) -> None:
        config: dict[str, Any] = {"grp": {"pattern": "db-shared"}}
        ref = SourceRef(group="grp", key="x", raw="{grp.sources.*.x}")
        with pytest.raises(SourceRefError, match="no 'sources' section"):
            resolve_source_ref(ref, "svc", config=config)


# ---------------------------------------------------------------------------
# validate_source_ref_for_all_envs tests
# ---------------------------------------------------------------------------


class TestValidateSourceRefForAllEnvs:
    """Tests for validate_source_ref_for_all_envs()."""

    def test_source_level_key_valid_for_all_envs(self) -> None:
        """Source-level key is valid for all environments."""
        config = _make_db_shared_config()
        ref = SourceRef(group="asma", key="customer_id", raw="{asma.sources.*.customer_id}")
        errors = validate_source_ref_for_all_envs(ref, "directory", config=config)
        assert errors == []

    def test_env_level_key_all_present(self) -> None:
        """Key present in all env entries."""
        config = _make_db_shared_config()
        ref = SourceRef(group="asma", key="database", raw="{asma.sources.*.database}")
        errors = validate_source_ref_for_all_envs(ref, "directory", config=config)
        assert errors == []

    def test_env_level_key_missing_in_one_env(self) -> None:
        """Key missing in one env should report error."""
        config = _make_missing_key_config()
        ref = SourceRef(group="broken", key="host", raw="{broken.sources.*.host}")
        errors = validate_source_ref_for_all_envs(ref, "svc", config=config)
        assert len(errors) == 1
        assert "prod" in errors[0]
        assert "host" in errors[0]

    def test_group_not_found(self) -> None:
        config = _make_db_shared_config()
        ref = SourceRef(group="nope", key="x", raw="{nope.sources.*.x}")
        errors = validate_source_ref_for_all_envs(ref, "svc", config=config)
        assert len(errors) == 1
        assert "not found" in errors[0]

    def test_source_not_found(self) -> None:
        config = _make_db_shared_config()
        ref = SourceRef(group="asma", key="x", raw="{asma.sources.*.x}")
        errors = validate_source_ref_for_all_envs(ref, "nope", config=config)
        assert len(errors) == 1
        assert "not found" in errors[0]


# ---------------------------------------------------------------------------
# validate_all_sources_have_key tests
# ---------------------------------------------------------------------------


class TestValidateAllSourcesHaveKey:
    """Tests for validate_all_sources_have_key()."""

    def test_all_sources_have_source_level_key(self) -> None:
        config = _make_db_shared_config()
        ref = SourceRef(group="asma", key="customer_id", raw="{asma.sources.*.customer_id}")
        errors = validate_all_sources_have_key(ref, config=config)
        assert errors == []

    def test_all_sources_have_env_level_key(self) -> None:
        config = _make_db_shared_config()
        ref = SourceRef(group="asma", key="database", raw="{asma.sources.*.database}")
        errors = validate_all_sources_have_key(ref, config=config)
        assert errors == []

    def test_missing_key_in_some_sources(self) -> None:
        """Key exists on one source but not another."""
        config: dict[str, Any] = {
            "grp": {
                "sources": {
                    "svc_a": {"special": "yes", "dev": {"db": "a_dev"}},
                    "svc_b": {"dev": {"db": "b_dev"}},  # missing 'special'
                },
            },
        }
        ref = SourceRef(group="grp", key="special", raw="{grp.sources.*.special}")
        errors = validate_all_sources_have_key(ref, config=config)
        assert len(errors) > 0
        # Should mention svc_b or its environments
        error_text = "\n".join(errors)
        assert "svc_b" in error_text

    def test_group_not_found(self) -> None:
        config = _make_db_shared_config()
        ref = SourceRef(group="nope", key="x", raw="{nope.sources.*.x}")
        errors = validate_all_sources_have_key(ref, config=config)
        assert len(errors) == 1
        assert "not found" in errors[0]

    def test_db_per_tenant_all_sources(self) -> None:
        config = _make_db_per_tenant_config()
        ref = SourceRef(
            group="adopus", key="customer_id",
            raw="{adopus.sources.*.customer_id}",
        )
        errors = validate_all_sources_have_key(ref, config=config)
        assert errors == []


# ---------------------------------------------------------------------------
# SourceRefError tests
# ---------------------------------------------------------------------------


class TestSourceRefError:
    """Tests for SourceRefError exception."""

    def test_message_attribute(self) -> None:
        err = SourceRefError("test message")
        assert err.message == "test message"
        assert str(err) == "test message"

    def test_print_error(self, capsys: pytest.CaptureFixture[str]) -> None:
        err = SourceRefError("something went wrong")
        err.print_error()
        captured = capsys.readouterr()
        assert "something went wrong" in captured.out or "something went wrong" in captured.err
