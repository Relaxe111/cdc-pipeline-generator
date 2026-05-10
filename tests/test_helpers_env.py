from pathlib import Path

from cdc_generator.helpers import helpers_env


def _mssql_placeholders() -> dict[str, str]:
    return {
        "host": "${MSSQL_SOURCE_HOST_FRETEX}",
        "port": "${MSSQL_SOURCE_PORT_FRETEX}",
        "user": "${MSSQL_SOURCE_USER_FRETEX}",
        "password": "${MSSQL_SOURCE_PASSWORD_FRETEX}",
    }


def test_append_env_vars_updates_env_and_env_example(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / ".env").write_text("# local values\n", encoding="utf-8")
    (tmp_path / ".env.example").write_text("# template values\n", encoding="utf-8")
    monkeypatch.setattr(helpers_env, "get_project_root", lambda: tmp_path)

    count = helpers_env.append_env_vars_to_dotenv(
        _mssql_placeholders(),
        "Source Server: fretex (mssql)",
    )

    assert count == 4
    for file_name in (".env", ".env.example"):
        content = (tmp_path / file_name).read_text(encoding="utf-8")
        assert "# Source Server: fretex (mssql)" in content
        assert "MSSQL_SOURCE_HOST_FRETEX=" in content
        assert "MSSQL_SOURCE_PORT_FRETEX=" in content
        assert "MSSQL_SOURCE_USER_FRETEX=" in content
        assert "MSSQL_SOURCE_PASSWORD_FRETEX=" in content


def test_append_env_vars_counts_missing_template_entries(
    tmp_path: Path,
    monkeypatch,
) -> None:
    env_content = "\n".join(
        [
            "# Source Server: fretex (mssql)",
            "MSSQL_SOURCE_HOST_FRETEX=10.90.38.9",
            "MSSQL_SOURCE_PORT_FRETEX=40501",
            "MSSQL_SOURCE_USER_FRETEX=runtime_user",
            "MSSQL_SOURCE_PASSWORD_FRETEX=secret",
            "",
        ],
    )
    (tmp_path / ".env").write_text(env_content, encoding="utf-8")
    (tmp_path / ".env.example").write_text("# template values\n", encoding="utf-8")
    monkeypatch.setattr(helpers_env, "get_project_root", lambda: tmp_path)

    count = helpers_env.append_env_vars_to_dotenv(
        _mssql_placeholders(),
        "Source Server: fretex (mssql)",
    )

    assert count == 4
    env_after = (tmp_path / ".env").read_text(encoding="utf-8")
    assert env_after.count("MSSQL_SOURCE_HOST_FRETEX=") == 1
    example_after = (tmp_path / ".env.example").read_text(encoding="utf-8")
    assert "MSSQL_SOURCE_HOST_FRETEX=" in example_after
    assert "MSSQL_SOURCE_PORT_FRETEX=" in example_after


def test_remove_env_vars_updates_env_and_env_example(
    tmp_path: Path,
    monkeypatch,
) -> None:
    section = "\n".join(
        [
            "# Source Server: fretex (mssql)",
            "MSSQL_SOURCE_HOST_FRETEX=",
            "MSSQL_SOURCE_PORT_FRETEX=",
            "MSSQL_SOURCE_USER_FRETEX=",
            "MSSQL_SOURCE_PASSWORD_FRETEX=",
            "",
        ],
    )
    for file_name in (".env", ".env.example"):
        (tmp_path / file_name).write_text(section, encoding="utf-8")
    monkeypatch.setattr(helpers_env, "get_project_root", lambda: tmp_path)

    count = helpers_env.remove_env_vars_from_dotenv(_mssql_placeholders())

    assert count == 4
    for file_name in (".env", ".env.example"):
        content = (tmp_path / file_name).read_text(encoding="utf-8")
        assert "MSSQL_SOURCE_HOST_FRETEX=" not in content
        assert "MSSQL_SOURCE_PORT_FRETEX=" not in content
        assert "MSSQL_SOURCE_USER_FRETEX=" not in content
        assert "MSSQL_SOURCE_PASSWORD_FRETEX=" not in content
        assert "Source Server: fretex (mssql)" not in content
