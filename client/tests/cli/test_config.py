"""Tests for the ``igem config`` CLI group + first-run discovery hint."""
from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from igem.api.cli.main import main
from igem.config import (
    cwd_config_path,
    find_any_config_path,
    find_local_config_path,
    get_api_key_from_config,
    get_resolved_config_value,
    get_server_url_from_config,
    home_config_path,
    is_sensitive_key,
    resolve_config_key,
    unset_local_config,
    write_local_config,
)


# ----- shared fixture: isolate cwd + HOME so the user's real config
# never leaks into a test ----------------------------------------------
@pytest.fixture
def isolated_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    work = tmp_path / "project"
    home = tmp_path / "home"
    work.mkdir()
    home.mkdir()
    monkeypatch.chdir(work)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.delenv("IGEM_URL", raising=False)
    monkeypatch.delenv("IGEM_API_KEY", raising=False)
    return work


# ----------------------------------------------------------------------
# resolve_config_key
# ----------------------------------------------------------------------
class TestResolveConfigKey:
    @pytest.mark.parametrize(
        "cli_key,expected",
        [
            ("server-url", ("client", "server_url")),
            ("server_url", ("client", "server_url")),
            ("api-key", ("client", "api_key")),
            ("api_key", ("client", "api_key")),
        ],
    )
    def test_known_keys(self, cli_key, expected):
        assert resolve_config_key(cli_key) == expected

    def test_unknown_key_raises(self):
        with pytest.raises(ValueError, match="Unknown config key"):
            resolve_config_key("bogus")


# ----------------------------------------------------------------------
# write_local_config (helper used by the CLI)
# ----------------------------------------------------------------------
class TestWriteLocalConfig:
    def test_creates_file_when_missing(self, isolated_workspace):
        path = write_local_config("client", "server_url", "https://x.org")
        assert path == isolated_workspace / ".igem.toml"
        assert path.exists()
        assert 'server_url = "https://x.org"' in path.read_text()

    def test_preserves_other_sections(self, isolated_workspace):
        (isolated_workspace / ".igem.toml").write_text(
            '[database]\nuri = "sqlite:///mydb.db"\n'
        )
        write_local_config("client", "server_url", "https://api.example.org")
        text = (isolated_workspace / ".igem.toml").read_text()
        assert 'uri = "sqlite:///mydb.db"' in text
        assert 'server_url = "https://api.example.org"' in text

    def test_updates_existing_value(self, isolated_workspace):
        write_local_config("client", "server_url", "https://old.example.org")
        write_local_config("client", "server_url", "https://new.example.org")
        text = (isolated_workspace / ".igem.toml").read_text()
        assert "old.example.org" not in text
        assert "new.example.org" in text

    def test_escapes_special_chars(self, isolated_workspace):
        write_local_config("client", "api_key", 'has"quote\\back')
        text = (isolated_workspace / ".igem.toml").read_text()
        assert 'has\\"quote\\\\back' in text


# ----------------------------------------------------------------------
# CLI: igem config set
# ----------------------------------------------------------------------
class TestConfigSetCli:
    def test_sets_server_url(self, isolated_workspace):
        runner = CliRunner()
        result = runner.invoke(
            main,
            ["config", "set", "server-url", "https://geneexposure.org/api"],
        )
        assert result.exit_code == 0, result.output
        assert "set server-url = https://geneexposure.org/api" in result.output
        path = isolated_workspace / ".igem.toml"
        assert path.exists()
        assert 'server_url = "https://geneexposure.org/api"' in path.read_text()

    def test_resolves_back_via_get_server_url(self, isolated_workspace):
        runner = CliRunner()
        runner.invoke(
            main,
            ["config", "set", "server-url", "https://geneexposure.org/api"],
        )
        assert (
            get_server_url_from_config() == "https://geneexposure.org/api"
        )

    def test_sets_api_key_separately(self, isolated_workspace):
        runner = CliRunner()
        runner.invoke(main, ["config", "set", "api-key", "secret-token"])
        assert get_api_key_from_config() == "secret-token"

    def test_set_then_set_preserves_first_key(self, isolated_workspace):
        runner = CliRunner()
        runner.invoke(
            main,
            ["config", "set", "server-url", "https://geneexposure.org/api"],
        )
        runner.invoke(main, ["config", "set", "api-key", "abc"])
        assert get_server_url_from_config() == "https://geneexposure.org/api"
        assert get_api_key_from_config() == "abc"

    def test_unknown_key_fails(self, isolated_workspace):
        runner = CliRunner()
        result = runner.invoke(
            main, ["config", "set", "bogus", "value"]
        )
        assert result.exit_code != 0
        assert "Unknown config key" in result.output

    def test_missing_value_fails(self, isolated_workspace):
        runner = CliRunner()
        result = runner.invoke(main, ["config", "set", "server-url"])
        assert result.exit_code != 0
        assert "Missing argument" in result.output


# ----------------------------------------------------------------------
# Discovery hint on `igem` (no args) and `igem --help`
# ----------------------------------------------------------------------
class TestNoArgHint:
    def test_hint_shown_when_no_config(self, isolated_workspace):
        assert find_any_config_path() is None
        runner = CliRunner()
        result = runner.invoke(main, [])
        assert result.exit_code == 0
        assert "no .igem.toml found" in result.output.lower()
        assert "config set server-url" in result.output

    def test_help_flag_also_shows_hint(self, isolated_workspace):
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert "no .igem.toml found" in result.output.lower()

    def test_hint_hidden_when_local_config_exists(self, isolated_workspace):
        (isolated_workspace / ".igem.toml").write_text(
            '[client]\nserver_url = "https://example.org"\n'
        )
        runner = CliRunner()
        result = runner.invoke(main, [])
        assert "no .igem.toml found" not in result.output.lower()
        assert find_local_config_path() == (
            isolated_workspace / ".igem.toml"
        )

    def test_hint_hidden_when_only_home_config_exists(self, isolated_workspace):
        # HOME was already redirected to a tmp dir by isolated_workspace.
        home_config = Path.home() / ".igem.toml"
        home_config.write_text(
            '[client]\nserver_url = "https://example.org"\n'
        )
        runner = CliRunner()
        result = runner.invoke(main, [])
        assert "no .igem.toml found" not in result.output.lower()


# ---------------------------------------------------------------------------
# unset (helper + CLI)
# ---------------------------------------------------------------------------
class TestUnsetLocalConfigHelper:
    def test_removes_key(self, isolated_workspace):
        write_local_config("client", "server_url", "https://x.org")
        write_local_config("client", "api_key", "abc")
        path, removed = unset_local_config("client", "api_key")
        assert removed is True
        text = path.read_text()
        assert "api_key" not in text
        assert "server_url" in text

    def test_removes_empty_section(self, isolated_workspace):
        write_local_config("client", "server_url", "https://x.org")
        path, removed = unset_local_config("client", "server_url")
        assert removed is True
        # Empty section dropped → file should be empty too → deleted.
        assert not path.exists()

    def test_removes_empty_file(self, isolated_workspace):
        write_local_config("client", "server_url", "https://x.org")
        path, _ = unset_local_config("client", "server_url")
        assert not path.exists()

    def test_noop_when_key_absent(self, isolated_workspace):
        write_local_config("client", "server_url", "https://x.org")
        _, removed = unset_local_config("client", "api_key")
        assert removed is False

    def test_noop_when_file_absent(self, isolated_workspace):
        path, removed = unset_local_config("client", "server_url")
        assert removed is False
        assert not path.exists()


class TestUnsetCli:
    def test_unset_clears_value(self, isolated_workspace):
        runner = CliRunner()
        runner.invoke(main, ["config", "set", "server-url", "https://x.org"])
        result = runner.invoke(main, ["config", "unset", "server-url"])
        assert result.exit_code == 0, result.output
        assert "unset server-url" in result.output
        assert get_server_url_from_config() is None

    def test_unset_absent_key_is_noop(self, isolated_workspace):
        runner = CliRunner()
        result = runner.invoke(main, ["config", "unset", "server-url"])
        assert result.exit_code == 0
        assert "nothing to do" in result.output

    def test_unset_unknown_key_fails(self, isolated_workspace):
        runner = CliRunner()
        result = runner.invoke(main, ["config", "unset", "bogus"])
        assert result.exit_code != 0
        assert "Unknown config key" in result.output

    def test_unset_global_targets_home_file(self, isolated_workspace):
        runner = CliRunner()
        runner.invoke(
            main,
            ["config", "set", "--global", "server-url", "https://home.org"],
        )
        assert home_config_path().exists()
        runner.invoke(main, ["config", "unset", "--global", "server-url"])
        assert not home_config_path().exists()


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------
class TestGetCli:
    def test_returns_value_from_local_toml(self, isolated_workspace):
        runner = CliRunner()
        runner.invoke(
            main, ["config", "set", "server-url", "https://x.org"]
        )
        result = runner.invoke(main, ["config", "get", "server-url"])
        assert result.exit_code == 0, result.output
        assert result.output.strip() == "https://x.org"

    def test_returns_value_from_home_toml(self, isolated_workspace):
        # No local config; only ~/.igem.toml has the value.
        home_config_path().write_text(
            '[client]\nserver_url = "https://home.org"\n'
        )
        runner = CliRunner()
        result = runner.invoke(main, ["config", "get", "server-url"])
        assert result.exit_code == 0
        assert result.output.strip() == "https://home.org"

    def test_local_overrides_home(self, isolated_workspace):
        home_config_path().write_text(
            '[client]\nserver_url = "https://home.org"\n'
        )
        (isolated_workspace / ".igem.toml").write_text(
            '[client]\nserver_url = "https://local.org"\n'
        )
        runner = CliRunner()
        result = runner.invoke(main, ["config", "get", "server-url"])
        assert result.output.strip() == "https://local.org"

    def test_env_var_overrides_files(
        self, isolated_workspace, monkeypatch
    ):
        (isolated_workspace / ".igem.toml").write_text(
            '[client]\nserver_url = "https://local.org"\n'
        )
        monkeypatch.setenv("IGEM_URL", "https://env.org")
        runner = CliRunner()
        result = runner.invoke(main, ["config", "get", "server-url"])
        assert result.output.strip() == "https://env.org"

    def test_get_unset_key_exits_nonzero(self, isolated_workspace):
        runner = CliRunner()
        result = runner.invoke(main, ["config", "get", "server-url"])
        assert result.exit_code == 1
        assert "is not set" in result.output

    def test_get_unknown_key_fails(self, isolated_workspace):
        runner = CliRunner()
        result = runner.invoke(main, ["config", "get", "bogus"])
        assert result.exit_code != 0
        assert "Unknown config key" in result.output

    def test_resolved_helper_returns_none_when_unset(
        self, isolated_workspace
    ):
        assert get_resolved_config_value("server-url") is None
        assert get_resolved_config_value("api-key") is None


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------
class TestShowCli:
    def test_empty_config(self, isolated_workspace):
        runner = CliRunner()
        result = runner.invoke(main, ["config", "show"])
        assert result.exit_code == 0
        assert "(empty)" in result.output
        assert "(none)" in result.output  # both file paths absent

    def test_shows_server_url_unmasked(self, isolated_workspace):
        runner = CliRunner()
        runner.invoke(
            main, ["config", "set", "server-url", "https://x.org"]
        )
        result = runner.invoke(main, ["config", "show"])
        assert "[client]" in result.output
        assert 'server_url = "https://x.org"' in result.output

    def test_masks_api_key(self, isolated_workspace):
        runner = CliRunner()
        runner.invoke(main, ["config", "set", "api-key", "super-secret-token"])
        result = runner.invoke(main, ["config", "show"])
        assert 'api_key = "***"' in result.output
        assert "super-secret-token" not in result.output

    def test_shows_file_paths(self, isolated_workspace):
        runner = CliRunner()
        runner.invoke(
            main, ["config", "set", "server-url", "https://x.org"]
        )
        result = runner.invoke(main, ["config", "show"])
        local = cwd_config_path()
        assert str(local) in result.output

    def test_shows_merged_local_and_home(self, isolated_workspace):
        home_config_path().write_text(
            '[client]\napi_key = "home-key"\n'
        )
        (isolated_workspace / ".igem.toml").write_text(
            '[client]\nserver_url = "https://local.org"\n'
        )
        runner = CliRunner()
        result = runner.invoke(main, ["config", "show"])
        # Local server_url + home api_key both visible (api_key masked).
        assert "https://local.org" in result.output
        assert 'api_key = "***"' in result.output


# ---------------------------------------------------------------------------
# --global flag on set
# ---------------------------------------------------------------------------
class TestGlobalFlag:
    def test_set_global_writes_home_not_local(self, isolated_workspace):
        runner = CliRunner()
        result = runner.invoke(
            main,
            ["config", "set", "--global", "server-url", "https://home.org"],
        )
        assert result.exit_code == 0, result.output
        assert home_config_path().exists()
        assert not (isolated_workspace / ".igem.toml").exists()

    def test_set_local_then_global_keeps_both(self, isolated_workspace):
        runner = CliRunner()
        runner.invoke(
            main, ["config", "set", "server-url", "https://local.org"]
        )
        runner.invoke(
            main,
            [
                "config", "set", "--global", "server-url",
                "https://home.org",
            ],
        )
        assert (isolated_workspace / ".igem.toml").exists()
        assert home_config_path().exists()
        # Local takes precedence in resolution.
        assert get_server_url_from_config() == "https://local.org"


# ---------------------------------------------------------------------------
# is_sensitive_key
# ---------------------------------------------------------------------------
class TestIsSensitiveKey:
    def test_api_key_is_sensitive(self):
        assert is_sensitive_key("client", "api_key") is True

    def test_server_url_is_not(self):
        assert is_sensitive_key("client", "server_url") is False
