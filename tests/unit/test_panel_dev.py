from jarvis.intercom.panel_dev import PANEL_STATES, PreviewConfig, render_panel_preview_html


def test_panel_preview_renders_every_voice_state() -> None:
    html = render_panel_preview_html()

    for state in PANEL_STATES:
        assert f'"{state}"' in html
        assert f'[data-state="{state}"]' in html


def test_panel_preview_sanitizes_title_and_falls_back_to_idle_state() -> None:
    html = render_panel_preview_html(PreviewConfig(initial_state="unknown", title='<Jarvis "panel">'))

    assert "<title>&lt;Jarvis &quot;panel&quot;&gt;</title>" in html
    assert '<main class="screen" data-state="idle">' in html


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


def test_panel_preview_disconnected_looks_angry_and_offline() -> None:
    html = render_panel_preview_html(PreviewConfig(initial_state="disconnected"))

    assert '<main class="screen" data-state="disconnected">' in html
    assert "--accent: #ff4b42;" in html
    assert '[data-state="disconnected"] .eye:first-child' in html
    assert '[data-state="disconnected"] .eye:last-child' in html
    assert "rotate(7deg)" in html
    assert "rotate(-7deg)" in html
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
    assert "--eye-scale-y" in html
    assert "--pupil-scale" in html
    assert "randomBetween(-3, 3)" in html
    assert "randomBetween(.96, 1.04)" in html
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
