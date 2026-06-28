"""Gateway client attribution helpers.

LiteLLM stores OpenAI's `user` as End User and `metadata.tags` as request tags,
so these helpers are the contract that makes proxy logs filterable by Jarvis
surface, person, and call kind.
"""

from __future__ import annotations

from jarvis.brain.gateway_client import GatewayClient, LLMAttribution
from jarvis.config import GatewayConfig


def _client() -> GatewayClient:
    cfg = GatewayConfig(
        _env_file=None,
        client_key="sk-test",
        speaker="family",
        room="kitchen",
    )
    return GatewayClient(cfg)


def test_attribution_uses_resolved_person_and_filter_tags() -> None:
    c = _client()
    attr = LLMAttribution(
        kind="turn",
        channel="whatsapp",
        speaker="neil",
        device_id="whatsapp",
    )

    assert c._end_user(attr) == "neil"
    body = c._extra_body(attr)
    meta = body["metadata"]

    assert meta["jarvis_kind"] == "turn"
    assert meta["jarvis_channel"] == "whatsapp"
    assert meta["jarvis_speaker"] == "neil"
    assert meta["jarvis_device"] == "whatsapp"
    assert meta["user_id"] == "neil"
    assert meta["tags"] == [
        "room:kitchen",
        "kind:turn",
        "channel:whatsapp",
        "speaker:neil",
        "device:whatsapp",
    ]
    assert c._extra_headers(attr) == {
        "x-litellm-end-user-id": "neil",
        "x-litellm-tags": "room:kitchen,kind:turn,channel:whatsapp,speaker:neil,device:whatsapp",
    }


def test_house_falls_back_to_family_and_heartbeat_is_explicit() -> None:
    c = _client()

    assert c._end_user(LLMAttribution(speaker="house")) == "family"
    assert c._end_user(LLMAttribution(kind="heartbeat", channel="system", speaker="heartbeat")) == "heartbeat"
