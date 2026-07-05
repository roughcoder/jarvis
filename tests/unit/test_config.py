"""Config — env-driven, secret-masked, localhost-by-default (constraint #1).

`_env_file=None` bypasses the repo's real .env so default/override assertions
are deterministic regardless of the developer's local secrets.
"""

from __future__ import annotations

from jarvis.config import (
    AccountConfig,
    BrainConfig,
    Config,
    DatabaseConfig,
    GatewayConfig,
    IntercomConfig,
    IntercomDeviceConfig,
    LinearConfig,
    MemoryConfig,
    WorkerConfig,
)


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


def test_gateway_voice_model_is_optional_and_env_driven(monkeypatch) -> None:
    _clean(monkeypatch, "GATEWAY_FAST_MODEL", "GATEWAY_VOICE_MODEL")

    assert GatewayConfig(_env_file=None).voice_model == ""

    monkeypatch.setenv("GATEWAY_VOICE_MODEL", "strong")

    assert GatewayConfig(_env_file=None).voice_model == "strong"


def test_base_url_is_computed_from_host_port(monkeypatch) -> None:
    _clean(monkeypatch, "MEMORY_HOST", "MEMORY_PORT")
    c = MemoryConfig(_env_file=None, host="frankfurt", port=8123)
    assert c.base_url == "http://frankfurt:8123"


def test_memory_backend_sidecar_and_curation_outbox_are_env_driven(monkeypatch, tmp_path) -> None:
    _clean(
        monkeypatch,
        "MEMORY_BACKEND",
        "MEMORY_CONCLUSION_SIDECAR_PATH",
        "MEMORY_DERIVER_IDLE_TIMEOUT_S",
        "MEMORY_CURATION_OUTBOX_PATH",
        "MEMORY_CURATION_OUTBOX_MAX_RETRIES",
        "MEMORY_TOOL_TIMEOUT_S",
    )
    monkeypatch.setenv("MEMORY_BACKEND", "v3")
    monkeypatch.setenv("MEMORY_CONCLUSION_SIDECAR_PATH", str(tmp_path / "sidecar.json"))
    monkeypatch.setenv("MEMORY_DERIVER_IDLE_TIMEOUT_S", "4.5")
    monkeypatch.setenv("MEMORY_CURATION_OUTBOX_PATH", str(tmp_path / "outbox.jsonl"))
    monkeypatch.setenv("MEMORY_CURATION_OUTBOX_MAX_RETRIES", "5")
    monkeypatch.setenv("MEMORY_TOOL_TIMEOUT_S", "1.5")

    c = MemoryConfig(_env_file=None)

    assert c.backend == "v3"
    assert c.conclusion_sidecar_path == str(tmp_path / "sidecar.json")
    assert c.deriver_idle_timeout_s == 4.5
    assert c.curation_outbox_path == str(tmp_path / "outbox.jsonl")
    assert c.curation_outbox_max_retries == 5
    assert c.tool_timeout_s == 1.5


def test_worker_workspace_defaults_outside_repo() -> None:
    assert WorkerConfig(_env_file=None).workspace == "~/.jarvis/worker"


def test_brain_websocket_limit_allows_long_utterances() -> None:
    assert BrainConfig(_env_file=None).websocket_max_size == 8 * 1024 * 1024


def test_websocket_keepalive_tolerates_slow_pi_event_loop() -> None:
    brain = BrainConfig(_env_file=None)
    intercom = IntercomConfig(_env_file=None)

    assert brain.websocket_ping_interval_s == 20.0
    assert brain.websocket_ping_timeout_s == 60.0
    assert intercom.websocket_max_size == brain.websocket_max_size
    assert intercom.websocket_ping_interval_s == brain.websocket_ping_interval_s
    assert intercom.websocket_ping_timeout_s == brain.websocket_ping_timeout_s


def test_intercom_network_probe_defaults_are_configurable() -> None:
    intercom = IntercomConfig(_env_file=None)

    assert intercom.network_probe_host == "1.1.1.1"
    assert intercom.network_probe_port == 53
    assert intercom.network_probe_timeout_s == 0.75


def test_pi_panel_sleep_defaults_to_ninety_seconds(monkeypatch) -> None:
    _clean(monkeypatch, "INTERCOM_DEVICE_PI_PANEL_SLEEP_AFTER_S", "INTERCOM_DEVICE_EYES_SLEEP_AFTER_S")

    c = IntercomDeviceConfig(_env_file=None)

    assert c.pi_panel_sleep_s == 90.0


def test_pi_panel_env_names_configure_panel(monkeypatch) -> None:
    _clean(
        monkeypatch,
        "INTERCOM_DEVICE_PI_PANEL",
        "INTERCOM_DEVICE_PI_PANEL_SLEEP_AFTER_S",
        "INTERCOM_DEVICE_PI_PANEL_GEOMETRY",
        "INTERCOM_DEVICE_PI_PANEL_URL",
        "INTERCOM_DEVICE_EYES",
        "INTERCOM_DEVICE_EYES_SLEEP_AFTER_S",
    )
    monkeypatch.setenv("INTERCOM_DEVICE_PI_PANEL", "true")
    monkeypatch.setenv("INTERCOM_DEVICE_PI_PANEL_SLEEP_AFTER_S", "12")
    monkeypatch.setenv("INTERCOM_DEVICE_PI_PANEL_GEOMETRY", "800x480+0+0")
    monkeypatch.setenv("INTERCOM_DEVICE_PI_PANEL_URL", "http://127.0.0.1:8787")

    c = IntercomDeviceConfig(_env_file=None)

    assert c.pi_panel_setting == "true"
    assert c.pi_panel_sleep_s == 12.0
    assert c.pi_panel_geometry == "800x480+0+0"
    assert c.pi_panel_url == "http://127.0.0.1:8787"


def test_account_binding_env_defaults() -> None:
    c = AccountConfig(_env_file=None)

    assert c.bindings_dir == "jarvis-workspace/.accounts"
    assert c.audit_path == "jarvis-workspace/.accounts/audit.jsonl"
    assert c.house_email_binding == "house-email"
    assert c.house_calendar_binding == "house-calendar"


def test_legacy_eyes_env_still_configures_pi_panel(monkeypatch) -> None:
    _clean(
        monkeypatch,
        "INTERCOM_DEVICE_PI_PANEL",
        "INTERCOM_DEVICE_PI_PANEL_SLEEP_AFTER_S",
        "INTERCOM_DEVICE_PI_PANEL_GEOMETRY",
        "INTERCOM_DEVICE_EYES",
        "INTERCOM_DEVICE_EYES_SLEEP_AFTER_S",
    )
    monkeypatch.setenv("INTERCOM_DEVICE_EYES", "false")
    monkeypatch.setenv("INTERCOM_DEVICE_EYES_SLEEP_AFTER_S", "9")

    c = IntercomDeviceConfig(_env_file=None)

    assert c.pi_panel_setting == "false"
    assert c.pi_panel_sleep_s == 9.0


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


def test_linear_config_uses_jarvis_env_file(monkeypatch, tmp_path) -> None:
    env_file = tmp_path / "service.env"
    env_file.write_text("LINEAR_API_KEY=lin-secret\n", encoding="utf-8")
    monkeypatch.setenv("JARVIS_ENV_FILE", str(env_file))
    _clean(monkeypatch, "LINEAR_API_KEY")

    c = Config()

    assert c.linear.api_key.get_secret_value() == "lin-secret"
    assert LinearConfig(_env_file=str(env_file)).api_key.get_secret_value() == "lin-secret"


def test_private_state_paths_resolve_relative_to_jarvis_env_file(monkeypatch, tmp_path) -> None:
    env_dir = tmp_path / "runtime-home"
    env_dir.mkdir()
    env_file = env_dir / ".env"
    env_file.write_text("", encoding="utf-8")
    other_cwd = tmp_path / "elsewhere"
    other_cwd.mkdir()
    monkeypatch.chdir(other_cwd)
    monkeypatch.setenv("JARVIS_ENV_FILE", str(env_file))

    c = Config()

    assert c.trace.path == str(env_dir / ".cache/traces.jsonl")
    assert c.brain.streaming_stt_enabled is True  # on by default (latency)
    assert c.capabilities.profiles_dir == str(env_dir / "jarvis-workspace/profiles")
    assert c.capabilities.users_dir == str(env_dir / "jarvis-workspace/users")
    assert c.registry.path == str(env_dir / "jarvis-workspace/registry/registry.json")
    assert c.orchestration.workspace == str(env_dir / "jarvis-workspace/orchestration")
    assert c.orchestration.workers_path == str(
        env_dir / "jarvis-workspace/orchestration/workers.json"
    )
    assert c.orchestration.schedules_path == str(
        env_dir / "jarvis-workspace/orchestration/schedules.json"
    )


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
    assert r["accounts.house_email_binding"] == "house-email"
    assert r["accounts.house_calendar_binding"] == "house-calendar"
    assert r["registry.path"]
    assert "****" in r["database.url"]
