import functools
import http.client
import http.server
import threading
import time

from jarvis.config import IntercomDeviceConfig
from jarvis.intercom.panel_dev import (
    PANEL_STATES,
    PanelStateStore,
    PreviewConfig,
    _PreviewHandler,
    render_panel_preview_html,
)
from jarvis.intercom.pi_panel import WebPiPanel


def test_panel_preview_renders_every_voice_state() -> None:
    html = render_panel_preview_html()

    for state in PANEL_STATES:
        assert f'"{state}"' in html
        assert f'[data-state="{state}"]' in html
    assert html.count('<span class="brow"></span>') == 2
    assert ".brow {" in html
    assert "--brow-left-rot" in html
    assert "--brow-right-rot" in html


def test_panel_preview_sanitizes_title_and_falls_back_to_idle_state() -> None:
    html = render_panel_preview_html(PreviewConfig(initial_state="unknown", title='<Jarvis "panel">'))

    assert "<title>&lt;Jarvis &quot;panel&quot;&gt;</title>" in html
    assert '<main class="screen" data-state="idle">' in html


def test_panel_preview_head_does_not_expose_host_filesystem() -> None:
    html = render_panel_preview_html()
    handler = functools.partial(_PreviewHandler, html=html)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
        conn.request("HEAD", "/")
        root = conn.getresponse()
        root.read()
        conn.close()

        conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
        conn.request("HEAD", "/etc/passwd")
        host_file = conn.getresponse()
        host_file.read()
        conn.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert root.status == 200
    assert root.getheader("Content-Type") == "text/html; charset=utf-8"
    assert root.getheader("Content-Length") == str(len(html.encode("utf-8")))
    assert host_file.status == 404


def test_panel_state_endpoint_accepts_valid_states() -> None:
    html = render_panel_preview_html()
    store = PanelStateStore("idle")
    handler = functools.partial(_PreviewHandler, html=html, state_store=store)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
        conn.request("POST", "/state", '{"state":"listening"}', {"Content-Type": "application/json"})
        posted = conn.getresponse()
        posted_body = posted.read().decode()
        conn.close()

        conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
        conn.request("GET", "/state")
        fetched = conn.getresponse()
        fetched_body = fetched.read().decode()
        conn.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert posted.status == 200
    assert posted_body == '{"state": "listening"}'
    assert fetched.status == 200
    assert fetched.getheader("Content-Type") == "application/json"
    assert fetched_body == '{"state": "listening"}'


def test_panel_state_endpoint_rejects_invalid_states() -> None:
    html = render_panel_preview_html()
    handler = functools.partial(_PreviewHandler, html=html, state_store=PanelStateStore("idle"))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
        conn.request("POST", "/state", '{"state":"unknown"}', {"Content-Type": "application/json"})
        response = conn.getresponse()
        response.read()
        conn.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert response.status == 400


def test_web_pi_panel_publishes_state_to_local_panel_endpoint() -> None:
    html = render_panel_preview_html()
    store = PanelStateStore("idle")
    handler = functools.partial(_PreviewHandler, html=html, state_store=store)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    panel = WebPiPanel(
        IntercomDeviceConfig(
            pi_panel_url=f"http://127.0.0.1:{server.server_port}",
            _env_file=None,
        )
    )
    try:
        panel.start()
        panel.set("speaking")
        deadline = time.monotonic() + 2
        while store.get() != "speaking" and time.monotonic() < deadline:
            time.sleep(0.02)
    finally:
        panel.stop()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert store.get() == "speaking"


def test_panel_preview_uses_spinner_pupils_for_thinking_state() -> None:
    html = render_panel_preview_html(PreviewConfig(initial_state="thinking"))

    assert '<main class="screen" data-state="thinking">' in html
    assert "@keyframes pupilSpin" in html
    assert '[data-state="thinking"] .pupil' in html
    assert 'border-top-color: #080d0a;' in html
    assert "border-right-color: #080d0a;" in html
    assert "animation: pupilSpin 760ms linear infinite;" in html
    assert '[data-state="thinking"]' in html
    assert "--accent: #8ddcff;" in html
    assert '<div class="eyes">' in html
    assert '[data-state="thinking"] .eyes' not in html
    assert "apertureThink" not in html
    assert "pupilLoader" not in html
    assert '<span class="pupil"></span>' in html


def test_panel_preview_connecting_is_spinner_only() -> None:
    html = render_panel_preview_html(PreviewConfig(initial_state="connecting"))

    assert '<main class="screen" data-state="connecting">' in html
    assert "--accent: #f4d46a;" in html
    assert '[data-state="connecting"] .eyes' in html
    assert "@keyframes connectSpin" in html
    assert 'screen.classList.toggle("info", next === "disconnected")' in html
    assert 'fetch("/state"' in html


def test_panel_preview_disconnected_looks_angry_and_offline() -> None:
    html = render_panel_preview_html(PreviewConfig(initial_state="disconnected"))

    assert '<main class="screen" data-state="disconnected">' in html
    assert "--accent: #ff4b42;" in html
    assert '[data-state="disconnected"] .eye:first-child' in html
    assert '[data-state="disconnected"] .eye:last-child' in html
    assert "rotate(7deg)" in html
    assert "rotate(-7deg)" in html
    assert "--brow-left-rot: 18deg;" in html
    assert "--brow-right-rot: -18deg;" in html
    assert '[data-state="disconnected"] .pupil::before' in html
    assert '[data-state="disconnected"] .pupil::after' in html
    assert "rotate(45deg)" in html
    assert "rotate(-45deg)" in html


def test_panel_preview_ready_state_has_subtle_idle_motion_only() -> None:
    html = render_panel_preview_html()

    assert "@keyframes idleLookLeft" in html
    assert "@keyframes idleLookRight" in html
    assert "pupilCartwheel" not in html
    assert "trick-cross" not in html
    assert "trick-spin" not in html


def test_panel_preview_speaking_uses_soft_bright_pink() -> None:
    html = render_panel_preview_html()

    assert '[data-state="speaking"]' in html
    assert "--accent: #ff7abb;" in html


def test_panel_preview_speaking_randomizes_eye_motion() -> None:
    html = render_panel_preview_html(PreviewConfig(initial_state="speaking"))

    assert '<main class="screen" data-state="speaking">' in html
    assert "scheduleSpeakingMotion" in html
    assert "applySpeakingPose" in html
    assert "--speak-brow-left" in html
    assert "--speak-brow-right" in html
    assert "--speak-brow-lift" in html
    assert "--eye-scale-y" in html
    assert "--pupil-scale" in html
    assert "randomBetween(-2, 2)" in html
    assert "randomBetween(-1.8, 1.8)" in html
    assert "randomBetween(.98, 1.03)" in html
    assert "720 + Math.random() * 640" in html


def test_panel_preview_sleep_feels_resting_but_ready() -> None:
    html = render_panel_preview_html(PreviewConfig(initial_state="sleep"))

    assert '<main class="screen" data-state="sleep">' in html
    assert "--accent: #c8d8ca;" in html
    assert "height: min(20vw, 118px);" in html
    assert "peek-left" in html
    assert "peek-right" in html
    assert "scheduleSleepPeek" in html
    assert "sleepPeekPupil" in html
    assert "--sleep-look-1-x" in html
    assert "2450 + Math.random() * 700" in html
    assert '<div class="sleep-zz" aria-hidden="true"><span>z</span><span>z</span><span>z</span></div>' in html
    assert "seedSleepZs" in html
    assert "animationiteration" in html
    assert "randomBetween(52, 88)" in html
    assert "randomBetween(8, 88)" in html
    assert "@keyframes sleepFloat" in html
    assert "animation: sleepFloat 7.4s linear infinite;" in html
    assert "8% { opacity: .28;" in html
    assert "64% { opacity: 0;" in html
    assert "font-size: var(--zz-size, clamp(24px, 8vw, 78px));" in html
    assert "randomBetween(24, 82)" in html


def test_panel_preview_listening_uses_bright_blue() -> None:
    html = render_panel_preview_html(PreviewConfig(initial_state="listening"))

    assert '<main class="screen" data-state="listening">' in html
    assert '[data-state="listening"]' in html
    assert "--accent: #8ddcff;" in html
