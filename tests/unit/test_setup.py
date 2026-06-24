from __future__ import annotations

import json

from jarvis.setup import apply_setup, read_setup, validate_setup


def _payload() -> dict:
    return {
        "admin": {
            "name": "Neil Barton",
            "email": "neil@example.com",
            "phone": "+44 7921 815819",
            "whatsapp_admin": "+44 7921 815819",
        },
        "machine": {"device_id": "Neil iMac", "room": "Office", "personal": True},
        "roles": ["brain", "intercom", "worker", "whatsapp"],
        "providers": {
            "openai_api_key": "sk-openai",
            "tts_api_key": "tts-secret",
            "tools_websearch_api_key": "tvly-secret",
        },
        "brain": {"host": "0.0.0.0", "port": "8700"},
        "worker": {"repo_root": "/Users/neilbarton/Development", "agent": "codex"},
        "whatsapp": {"dm_policy": "pairing"},
    }


def test_setup_apply_writes_env_user_and_autopairs_local_roles(tmp_path) -> None:
    env_file = tmp_path / ".jarvis" / ".env"

    result = apply_setup(env_file, _payload())

    text = env_file.read_text(encoding="utf-8")
    assert oct(env_file.stat().st_mode & 0o777) == "0o600"
    assert 'CAPS_DEVICE_ID="neil-imac"' in text
    assert 'CAPS_IDENTITY="neil-barton"' in text
    assert 'CAPS_SCOPE="personal"' in text
    assert 'INTERCOM_BRAIN_HOST="localhost"' in text
    assert 'WHATSAPP_ENABLED="true"' in text
    assert 'WHATSAPP_DM_POLICY="pairing"' in text
    assert 'WHATSAPP_ADMIN="447921815819"' in text
    assert 'OPENAI_API_KEY="sk-openai"' in text
    assert 'TTS_API_KEY="tts-secret"' in text

    devices = json.loads(next(line for line in text.splitlines() if line.startswith("BRAIN_DEVICES=")).split("=", 1)[1].strip('"').replace('\\"', '"'))
    assert {d["device_id"] for d in devices} == {"neil-imac", "whatsapp"}
    local = next(d for d in devices if d["device_id"] == "neil-imac")
    assert local["identity"] == "neil-barton"
    assert local["token"]

    user_file = tmp_path / ".jarvis" / "jarvis-workspace" / "users" / "neil-barton.md"
    user_text = user_file.read_text(encoding="utf-8")
    assert 'devices: ["neil-imac"]' in user_text
    assert 'whatsapp: ["447921815819"]' in user_text
    assert "- email: neil@example.com" in user_text
    assert "- phone: +44 7921 815819" in user_text
    assert result["user_file"] == str(user_file)


def test_setup_apply_preserves_tokens_and_unrelated_env(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text('UNRELATED="keep"\nGATEWAY_API_KEY="sk-existing"\n', encoding="utf-8")

    first = apply_setup(env_file, _payload())
    text1 = env_file.read_text(encoding="utf-8")
    second = apply_setup(env_file, _payload())
    text2 = env_file.read_text(encoding="utf-8")

    assert first["changed_keys"] == second["changed_keys"]
    assert 'UNRELATED="keep"' in text2
    assert 'GATEWAY_API_KEY="sk-existing"' in text2
    assert json.loads(_dotenv_value(text1, "BRAIN_DEVICES")) == json.loads(_dotenv_value(text2, "BRAIN_DEVICES"))


def test_setup_read_prefills_non_secret_values(tmp_path) -> None:
    env_file = tmp_path / ".env"
    apply_setup(env_file, _payload())

    state = read_setup(env_file)

    assert state["admin"]["name"] == "Neil Barton"
    assert state["admin"]["email"] == "neil@example.com"
    assert state["machine"] == {"device_id": "neil-imac", "room": "Office", "personal": True}
    assert set(state["roles"]) == {"brain", "intercom", "worker", "whatsapp"}
    assert state["providers"]["has_openai_api_key"] is True
    assert state["providers"]["has_tts_api_key"] is True


def test_setup_validate_reports_missing_role_requirements(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")

    result = validate_setup(env_file, ["brain", "intercom", "whatsapp"])

    assert result["ok"] is False
    assert "one LLM provider key" in result["missing"]
    assert "INTERCOM_TOKEN" in result["missing"]
    assert "WHATSAPP_TOKEN" in result["missing"]


def _dotenv_value(text: str, key: str) -> str:
    raw = next(line for line in text.splitlines() if line.startswith(f"{key}=")).split("=", 1)[1]
    return raw.strip().strip('"').replace('\\"', '"').replace("\\\\", "\\")
