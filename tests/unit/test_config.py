"""Config — env-driven, secret-masked, localhost-by-default (constraint #1).

`_env_file=None` bypasses the repo's real .env so default/override assertions
are deterministic regardless of the developer's local secrets.
"""

from __future__ import annotations

from jarvis.config import Config, DatabaseConfig, GatewayConfig, MemoryConfig


def _clean(monkeypatch, *names: str) -> None:
    for n in names:
        monkeypatch.delenv(n, raising=False)


def test_defaults_are_localhost(monkeypatch) -> None:
    # The whole Phase 2 migration story rests on this: services default to
    # localhost and move by changing *_HOST only.
    _clean(monkeypatch, "GATEWAY_HOST", "MEMORY_HOST", "DB_HOST")
    assert GatewayConfig(_env_file=None).host == "localhost"
    assert MemoryConfig(_env_file=None).host == "localhost"
    assert DatabaseConfig(_env_file=None).host == "localhost"


def test_base_url_is_computed_from_host_port(monkeypatch) -> None:
    _clean(monkeypatch, "MEMORY_HOST", "MEMORY_PORT")
    c = MemoryConfig(_env_file=None, host="frankfurt", port=8123)
    assert c.base_url == "http://frankfurt:8123"


def test_env_var_overrides_with_prefix(monkeypatch) -> None:
    monkeypatch.setenv("MEMORY_HOST", "hive.tailnet")
    monkeypatch.setenv("MEMORY_PORT", "9000")
    c = MemoryConfig(_env_file=None)
    assert c.host == "hive.tailnet"
    assert c.base_url == "http://hive.tailnet:9000"


def test_config_uses_jarvis_env_file(monkeypatch, tmp_path) -> None:
    env_file = tmp_path / "service.env"
    env_file.write_text(
        "CAPS_DEVICE_ID=laptop-worker\n"
        "INTERCOM_BRAIN_HOST=imac.private\n"
        "INTERCOM_TOKEN=paired-token\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("JARVIS_ENV_FILE", str(env_file))
    _clean(monkeypatch, "CAPS_DEVICE_ID", "INTERCOM_BRAIN_HOST", "INTERCOM_TOKEN")

    c = Config()

    assert c.capabilities.device_id == "laptop-worker"
    assert c.intercom.brain_host == "imac.private"
    assert c.intercom.token.get_secret_value() == "paired-token"


def test_database_url_masks_password(monkeypatch) -> None:
    _clean(monkeypatch, "DB_PASSWORD", "DB_HOST")
    c = DatabaseConfig(_env_file=None, password="s3cret", host="localhost")
    assert "s3cret" in c.url
    assert "s3cret" not in c.url_masked
    assert "****" in c.url_masked


def test_resolved_never_leaks_secrets() -> None:
    # resolved() is the dry-run printout (`jarvis config`) — must mask.
    r = Config().resolved()
    assert r["gateway.api_key"] in {"<set>", "<unset>"}
    assert r["memory.api_key"] in {"<set>", "<unset>"}
    assert r["tts.api_key"] in {"<set>", "<unset>"}
    assert "****" in r["database.url"]
