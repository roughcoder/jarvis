"""Standalone PiPanel preview server.

This is a design/development harness, not the production intercom display path.
It lets the panel UI be built and tested on a laptop or the Pi without pairing,
audio hardware, wake word models, or a running brain.
"""

from __future__ import annotations

import argparse
import contextlib
import functools
import http.server
import json
import shutil
import subprocess
import sys
import threading
import webbrowser
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse


PANEL_STATES = (
    "idle",
    "connecting",
    "awake",
    "listening",
    "thinking",
    "speaking",
    "disconnected",
    "sleep",
)


@dataclass(frozen=True)
class PreviewConfig:
    initial_state: str = "idle"
    title: str = "Jarvis PiPanel"


def render_panel_preview_html(cfg: PreviewConfig | None = None) -> str:
    cfg = cfg or PreviewConfig()
    state = cfg.initial_state if cfg.initial_state in PANEL_STATES else "idle"
    states = ",".join(f'"{item}"' for item in PANEL_STATES)
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>{_escape_html(cfg.title)}</title>
<style>
:root {{
  color-scheme: dark;
  --bg: #070a08;
  --panel: #101611;
  --panel-2: #172019;
  --line: #2b332d;
  --text: #f4efe4;
  --muted: #aeb7ad;
  --accent: #dce9d3;
  --accent-soft: #435246;
  --warn: #e6bd68;
  --bad: #ea7764;
  --ok: #b9e4bd;
  --radius: 8px;
}}

* {{ box-sizing: border-box; }}

html, body {{
  width: 100%;
  height: 100%;
  margin: 0;
  overflow: hidden;
  background: var(--bg);
  color: var(--text);
  font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}}

body {{
  touch-action: manipulation;
  user-select: none;
}}

.screen {{
  position: relative;
  width: 100vw;
  height: 100vh;
  min-width: 320px;
  min-height: 240px;
  overflow: hidden;
  background: var(--bg);
}}

.screen::after {{
  content: none;
}}

.topline {{
  position: absolute;
  left: clamp(18px, 4vw, 34px);
  right: clamp(18px, 4vw, 34px);
  top: clamp(16px, 4vh, 26px);
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  z-index: 3;
}}

.brand {{
  display: flex;
  align-items: baseline;
  gap: 10px;
  min-width: 0;
}}

.brand strong {{
  font-size: clamp(16px, 4.4vw, 26px);
  letter-spacing: 0;
  font-weight: 720;
}}

.brand span {{
  color: var(--muted);
  font-size: clamp(11px, 2.8vw, 15px);
  white-space: nowrap;
}}

.state-pill {{
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 104px;
  justify-content: flex-end;
  color: var(--accent);
  font-size: clamp(12px, 3vw, 16px);
  font-weight: 700;
}}

.state-pill i {{
  width: 10px;
  height: 10px;
  border-radius: 999px;
  background: var(--accent);
}}

.stage {{
  position: absolute;
  inset: 0;
  display: grid;
  place-items: center;
  padding: clamp(56px, 14vh, 86px) clamp(16px, 5vw, 44px) clamp(70px, 15vh, 96px);
}}

.eyes {{
  position: relative;
  width: min(82vw, 660px);
  height: min(45vh, 260px);
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: min(10vw, 86px);
  align-items: center;
  transform: translateY(var(--eye-shift, 0));
  transition: transform 420ms cubic-bezier(.16, 1, .3, 1);
}}

.eye {{
  position: relative;
  height: var(--eye-height, min(24vw, 150px));
  border-radius: 999px;
  background: var(--accent);
  overflow: visible;
  transform:
    translate(var(--eye-x, 0px), var(--eye-y, 0px))
    scaleX(var(--eye-width, 1))
    scaleY(var(--eye-scale-y, 1));
  transition:
    height 360ms cubic-bezier(.16, 1, .3, 1),
    transform 520ms cubic-bezier(.16, 1, .3, 1),
    background 240ms ease;
}}

.eye::before {{
  content: none;
}}

.brow {{
  position: absolute;
  left: 50%;
  top: var(--brow-y, -34%);
  z-index: 3;
  width: var(--brow-width, 62%);
  height: var(--brow-height, min(2.7vw, 16px));
  border-radius: 999px;
  background: var(--accent);
  transform:
    translate(calc(-50% + var(--brow-x, 0px)), var(--brow-lift, 0px))
    rotate(var(--brow-rot, 0deg))
    scaleX(var(--brow-scale-x, 1))
    scaleY(var(--brow-scale-y, 1));
  transform-origin: center;
  transition:
    transform 520ms cubic-bezier(.16, 1, .3, 1),
    top 360ms cubic-bezier(.16, 1, .3, 1),
    width 260ms ease,
    height 260ms ease,
    background 240ms ease,
    opacity 240ms ease;
}}

.eye:first-child .brow {{
  --brow-rot: var(--brow-left-rot, -4deg);
}}

.eye:last-child .brow {{
  --brow-rot: var(--brow-right-rot, 4deg);
}}

.pupil {{
  position: absolute;
  left: 50%;
  top: 50%;
  z-index: 2;
  width: var(--pupil, min(9vw, 64px));
  height: var(--pupil, min(9vw, 64px));
  border-radius: 999px;
  background: #080d0a;
  transform:
    translate(calc(-50% + var(--look-x, 0px)), calc(-50% + var(--look-y, 0px)))
    scale(var(--pupil-scale, 1));
  transition:
    width 260ms ease,
    height 260ms ease,
    transform 460ms cubic-bezier(.16, 1, .3, 1),
    background 220ms ease;
}}

.stage::before,
.stage::after {{
  content: "";
  display: none;
  position: absolute;
  left: 50%;
  top: 50%;
  transform: translate(-50%, -50%);
}}

.sleep-zz {{
  position: absolute;
  inset: 0;
  color: color-mix(in srgb, var(--accent), transparent 22%);
  font-weight: 820;
  opacity: 0;
  pointer-events: none;
}}

.sleep-zz span {{
  position: absolute;
  left: var(--zz-left, 50vw);
  top: var(--zz-top, 72vh);
  display: block;
  font-size: var(--zz-size, clamp(24px, 8vw, 78px));
  line-height: .9;
  opacity: 0;
  transform: translate(0, 18px) scale(.86);
}}

[data-state="sleep"] .sleep-zz {{
  opacity: .88;
}}

[data-state="sleep"] .sleep-zz span {{
  animation: sleepFloat 7.4s linear infinite;
}}

[data-state="sleep"] .sleep-zz span:nth-child(1) {{
  animation-delay: .1s;
}}

[data-state="sleep"] .sleep-zz span:nth-child(2) {{
  animation-delay: 2.05s;
  opacity: .78;
}}

[data-state="sleep"] .sleep-zz span:nth-child(3) {{
  animation-delay: 4.05s;
  opacity: .56;
}}

.expression {{
  position: absolute;
  left: 50%;
  bottom: clamp(42px, 10vh, 64px);
  transform: translateX(-50%);
  color: var(--muted);
  font-size: clamp(12px, 3vw, 16px);
  text-align: center;
  white-space: nowrap;
}}

.meter {{
  position: absolute;
  left: clamp(18px, 4vw, 34px);
  right: clamp(18px, 4vw, 34px);
  bottom: clamp(16px, 4vh, 26px);
  height: 30px;
  display: grid;
  grid-template-columns: repeat(24, 1fr);
  gap: 4px;
  opacity: .78;
}}

.meter span {{
  align-self: end;
  min-height: 4px;
  border-radius: 2px 2px 0 0;
  background: color-mix(in srgb, var(--accent), transparent 26%);
  transform-origin: bottom;
  transform: scaleY(var(--bar, .2));
  transition: transform 180ms ease, background 240ms ease;
}}

.controls {{
  position: absolute;
  left: clamp(12px, 3vw, 22px);
  right: clamp(12px, 3vw, 22px);
  bottom: clamp(56px, 12vh, 72px);
  display: flex;
  justify-content: center;
  flex-wrap: wrap;
  gap: 8px;
  z-index: 5;
  opacity: var(--controls-opacity, 0);
  transform: translateY(var(--controls-y, 8px));
  transition: opacity 180ms ease, transform 180ms ease;
  pointer-events: none;
}}

.screen.controls-open {{ --controls-opacity: 1; --controls-y: 0; }}
.screen.controls-open .controls {{ pointer-events: auto; }}

button {{
  appearance: none;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: rgba(16, 22, 17, .9);
  color: var(--text);
  min-height: 34px;
  padding: 0 12px;
  font: inherit;
  font-weight: 650;
}}

button[aria-pressed="true"] {{
  border-color: var(--accent);
  color: #07100b;
  background: var(--accent);
}}

.debug {{
  position: absolute;
  left: clamp(18px, 4vw, 34px);
  right: clamp(18px, 4vw, 34px);
  top: clamp(72px, 17vh, 104px);
  display: none;
  grid-template-columns: repeat(4, 1fr);
  gap: 10px;
  z-index: 4;
}}

.screen.info .debug {{ display: grid; }}

.tile {{
  min-height: 72px;
  border: 1px solid var(--line);
  border-radius: var(--radius);
  background: color-mix(in srgb, var(--panel), transparent 7%);
  padding: 12px;
}}

.tile label {{
  display: block;
  color: var(--muted);
  font-size: 11px;
  font-weight: 750;
  margin-bottom: 10px;
}}

.tile strong {{
  display: block;
  color: var(--text);
  font-size: clamp(15px, 3.4vw, 22px);
  font-weight: 760;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}}

[data-state="sleep"] {{
  --accent: #c8d8ca;
  --accent-soft: #3b4b40;
  --eye-height: min(5vw, 28px);
  --pupil: 0px;
  --brow-y: -58%;
  --brow-height: min(2vw, 12px);
  --brow-width: 70%;
  --brow-left-rot: -2deg;
  --brow-right-rot: 2deg;
  --brow-lift: min(1vw, 6px);
}}

[data-state="sleep"] .brow {{
  opacity: 0;
}}

.peek-left[data-state="sleep"] .eye:first-child,
.peek-right[data-state="sleep"] .eye:last-child {{
  height: min(20vw, 118px);
  transform: translateY(min(-1.2vw, -6px)) scaleX(.98) scaleY(.72);
}}

.peek-left[data-state="sleep"] .eye:first-child .brow,
.peek-right[data-state="sleep"] .eye:last-child .brow {{
  --brow-y: -42%;
  --brow-lift: min(-.8vw, -5px);
  --brow-scale-y: .86;
  opacity: 1;
}}

.peek-left[data-state="sleep"] .eye:first-child .pupil,
.peek-right[data-state="sleep"] .eye:last-child .pupil {{
  width: min(5.4vw, 34px);
  height: min(5.4vw, 34px);
  opacity: .82;
  animation: sleepPeekPupil 2.45s cubic-bezier(.16, 1, .3, 1) both;
}}

[data-state="idle"] {{
  --accent: #dce9d3;
  --eye-height: min(22vw, 138px);
  --pupil: min(8.6vw, 58px);
  --brow-y: -36%;
  --brow-left-rot: -3deg;
  --brow-right-rot: 3deg;
}}

[data-state="idle"] .eye:first-child .pupil {{
  animation: idleLookLeft 9.5s cubic-bezier(.16, 1, .3, 1) infinite;
}}

[data-state="idle"] .eye:last-child .pupil {{
  animation: idleLookRight 9.5s cubic-bezier(.16, 1, .3, 1) infinite;
}}

[data-state="connecting"] {{
  --accent: #f4d46a;
}}

[data-state="connecting"] .eyes {{
  opacity: 0;
  transform: scale(.9);
}}

[data-state="connecting"] .stage::before {{
  display: block;
  width: min(22vw, 110px);
  aspect-ratio: 1;
  border-radius: 999px;
  border: 3px solid #5a4d23;
  border-top-color: var(--accent);
  border-right-color: #fff0a3;
  animation: connectSpin 980ms linear infinite;
}}

[data-state="connecting"] .stage::after {{
  display: block;
  width: min(7vw, 34px);
  aspect-ratio: 1;
  border-radius: 999px;
  background: var(--accent);
}}

[data-state="connecting"] .meter {{
  opacity: .25;
}}

[data-state="disconnected"] {{
  --accent: #ff4b42;
  --eye-height: min(11vw, 68px);
  --eye-width: .96;
  --eye-scale-y: .74;
  --eye-y: min(.9vw, 6px);
  --pupil: min(6.4vw, 44px);
  --brow-y: -88%;
  --brow-height: min(3vw, 18px);
  --brow-width: 76%;
  --brow-left-rot: 14deg;
  --brow-right-rot: -14deg;
  --brow-lift: min(-.3vw, -2px);
}}

[data-state="disconnected"] .pupil {{
  background: transparent;
  box-shadow: none;
}}

[data-state="disconnected"] .pupil::before,
[data-state="disconnected"] .pupil::after {{
  content: "";
  position: absolute;
  left: 50%;
  top: 50%;
  width: 118%;
  height: 18%;
  border-radius: 999px;
  background: #170504;
}}

[data-state="disconnected"] .pupil::before {{
  transform: translate(-50%, -50%) rotate(45deg);
}}

[data-state="disconnected"] .pupil::after {{
  transform: translate(-50%, -50%) rotate(-45deg);
}}

[data-state="awake"] {{
  --accent: var(--ok);
  --eye-height: min(27vw, 164px);
  --pupil: min(8.8vw, 62px);
  --brow-y: -42%;
  --brow-left-rot: -7deg;
  --brow-right-rot: 7deg;
  --brow-lift: min(-.5vw, -4px);
}}

[data-state="listening"] {{
  --accent: #8ddcff;
  --eye-height: min(25vw, 154px);
  --pupil: min(7.6vw, 54px);
  --look-y: -10px;
  --brow-y: -45%;
  --brow-left-rot: -9deg;
  --brow-right-rot: 9deg;
  --brow-lift: min(-.8vw, -6px);
}}

[data-state="thinking"] {{
  --accent: #8ddcff;
  --eye-height: min(18vw, 114px);
  --pupil: min(6.2vw, 42px);
  --brow-y: -48%;
  --brow-left-rot: -8deg;
  --brow-right-rot: 6deg;
  --brow-width: 68%;
}}

[data-state="thinking"] .eye:first-child .brow {{
  --brow-lift: min(-1.4vw, -10px);
  --brow-scale-x: .95;
}}

[data-state="thinking"] .eye:last-child .brow {{
  --brow-lift: min(.8vw, 6px);
  --brow-scale-x: 1.05;
}}

[data-state="thinking"] .pupil {{
  background: transparent;
  border-radius: 999px;
  border: 3px solid #080d0a;
  border-top-color: #080d0a;
  border-right-color: #080d0a;
  border-bottom-color: var(--accent);
  animation: pupilSpin 760ms linear infinite;
  will-change: transform;
}}

[data-state="thinking"] .pupil::after {{
  content: "";
  position: absolute;
  inset: 32%;
  border-radius: 999px;
  background: #080d0a;
}}

[data-state="speaking"] {{
  --accent: #ff7abb;
  --eye-height: min(22vw, 136px);
  --pupil: min(8.2vw, 58px);
  --brow-y: -40%;
  --brow-left-rot: var(--speak-brow-left, -6deg);
  --brow-right-rot: var(--speak-brow-right, 6deg);
  --brow-lift: var(--speak-brow-lift, 0px);
}}

@keyframes connectSpin {{
  to {{ transform: translate(-50%, -50%) rotate(360deg); }}
}}

@keyframes idleLookLeft {{
  0%, 13%, 100% {{ transform: translate(calc(-50% + 0px), calc(-50% + 0px)); }}
  24%, 35% {{ transform: translate(calc(-50% - min(3.8vw, 22px)), calc(-50% - min(1.5vw, 9px))); }}
  46%, 55% {{ transform: translate(calc(-50% + min(2.8vw, 18px)), calc(-50% + min(1vw, 7px))); }}
  67%, 76% {{ transform: translate(calc(-50% + min(.5vw, 4px)), calc(-50% - min(2.1vw, 13px))); }}
}}

@keyframes idleLookRight {{
  0%, 13%, 100% {{ transform: translate(calc(-50% + 0px), calc(-50% + 0px)); }}
  24%, 35% {{ transform: translate(calc(-50% - min(2.8vw, 18px)), calc(-50% - min(1.4vw, 8px))); }}
  46%, 55% {{ transform: translate(calc(-50% + min(3.8vw, 22px)), calc(-50% + min(1vw, 7px))); }}
  67%, 76% {{ transform: translate(calc(-50% - min(.5vw, 4px)), calc(-50% - min(2.1vw, 13px))); }}
}}

@keyframes workSpin {{
  to {{ transform: translate(calc(-50% + var(--look-x, 0px)), calc(-50% + var(--look-y, 0px))) rotate(360deg); }}
}}

@keyframes pupilSpin {{
  0% {{
    transform: translate(calc(-50% + var(--look-x, 0px)), calc(-50% + var(--look-y, 0px))) rotate(0deg);
  }}
  100% {{
    transform: translate(calc(-50% + var(--look-x, 0px)), calc(-50% + var(--look-y, 0px))) rotate(360deg);
  }}
}}

@keyframes sleepFloat {{
  0%, 100% {{ opacity: 0; transform: translate(0, 18px) rotate(0deg) scale(.82); }}
  8% {{ opacity: .28; transform: translate(calc(var(--zz-sway-a, -4vw) * .24), -5vh) rotate(calc(var(--zz-rot-a, -12deg) * .22)) scale(.9); }}
  16% {{ opacity: .72; transform: translate(calc(var(--zz-sway-a, -4vw) * .58), -10vh) rotate(calc(var(--zz-rot-a, -12deg) * .55)) scale(.98); }}
  24% {{ opacity: .82; transform: translate(var(--zz-sway-a, -4vw), -15vh) rotate(var(--zz-rot-a, -12deg)) scale(1.03); }}
  32% {{ opacity: .7; transform: translate(calc((var(--zz-sway-a, -4vw) + var(--zz-sway-b, 7vw)) * .5), -21vh) rotate(calc((var(--zz-rot-a, -12deg) + var(--zz-rot-b, 18deg)) * .5)) scale(1.07); }}
  40% {{ opacity: .52; transform: translate(var(--zz-sway-b, 7vw), -27vh) rotate(var(--zz-rot-b, 18deg)) scale(1.1); }}
  48% {{ opacity: .3; transform: translate(calc((var(--zz-sway-b, 7vw) + var(--zz-sway-c, -9vw)) * .5), -35vh) rotate(calc((var(--zz-rot-b, 18deg) + var(--zz-rot-c, -24deg)) * .5)) scale(1.14); }}
  56% {{ opacity: .08; transform: translate(var(--zz-sway-c, -9vw), -43vh) rotate(var(--zz-rot-c, -24deg)) scale(1.18); }}
  64% {{ opacity: 0; transform: translate(var(--zz-sway-d, 11vw), -52vh) rotate(var(--zz-rot-d, 34deg)) scale(1.22); }}
}}

@keyframes sleepPeekPupil {{
  0%, 100% {{
    transform: translate(-50%, -50%) scale(.92);
  }}
  22% {{
    transform: translate(calc(-50% + var(--sleep-look-1-x, 8px)), calc(-50% + var(--sleep-look-1-y, -3px))) scale(.96);
  }}
  52% {{
    transform: translate(calc(-50% + var(--sleep-look-2-x, -7px)), calc(-50% + var(--sleep-look-2-y, 2px))) scale(1);
  }}
  78% {{
    transform: translate(calc(-50% + var(--sleep-look-3-x, 3px)), calc(-50% + var(--sleep-look-3-y, -4px))) scale(.96);
  }}
}}

@media (max-width: 520px), (max-height: 420px) {{
  .brand span {{ display: none; }}
  .debug {{ grid-template-columns: repeat(2, 1fr); }}
  .meter {{ height: 22px; gap: 3px; }}
}}
</style>
</head>
<body>
<main class="screen" data-state="{state}">
  <div class="topline">
    <div class="brand"><strong>Jarvis</strong><span>room intercom</span></div>
    <div class="state-pill"><i></i><span id="stateLabel">{state}</span></div>
  </div>
  <section class="debug" aria-label="status">
    <div class="tile"><label>voice</label><strong id="tileVoice">{state}</strong></div>
    <div class="tile"><label>brain</label><strong>paired</strong></div>
    <div class="tile"><label>display</label><strong>800 x 480</strong></div>
    <div class="tile"><label>camera</label><strong>ready</strong></div>
  </section>
  <section class="stage" aria-label="eyes">
    <div class="eyes">
      <div class="eye"><span class="brow"></span><span class="pupil"></span></div>
      <div class="eye"><span class="brow"></span><span class="pupil"></span></div>
    </div>
    <div class="sleep-zz" aria-hidden="true"><span>z</span><span>z</span><span>z</span></div>
  </section>
  <div class="expression" id="expression">tap for controls</div>
  <div class="controls" id="controls" aria-label="preview states"></div>
  <div class="meter" id="meter" aria-hidden="true"></div>
</main>
<script>
const states = [{states}];
const labels = {{
  idle: "ready",
  connecting: "connecting",
  awake: "awake",
  listening: "listening",
  thinking: "thinking",
  speaking: "speaking",
  disconnected: "offline",
  sleep: "resting"
}};
const expressions = {{
  idle: "waiting for hey jarvis",
  connecting: "finding the brain",
  awake: "wake word heard",
  listening: "listening",
  thinking: "working",
  speaking: "speaking",
  disconnected: "brain link offline",
  sleep: "screen resting"
}};
const screen = document.querySelector(".screen");
const controls = document.getElementById("controls");
const meter = document.getElementById("meter");
const stateLabel = document.getElementById("stateLabel");
const tileVoice = document.getElementById("tileVoice");
const expression = document.getElementById("expression");
const eyes = [...document.querySelectorAll(".eye")];
const pupils = [...document.querySelectorAll(".pupil")];
const sleepZs = [...document.querySelectorAll(".sleep-zz span")];
let stateIndex = Math.max(0, states.indexOf(screen.dataset.state));
let controlsTimer = 0;
let demoTimer = 0;
let speakingTimer = 0;
let sleepPeekTimer = 0;

for (const state of states) {{
  const button = document.createElement("button");
  button.type = "button";
  button.textContent = labels[state];
  button.dataset.state = state;
  button.addEventListener("click", () => setState(state, {{ publish: true }}));
  controls.append(button);
}}

for (let i = 0; i < 24; i += 1) {{
  const bar = document.createElement("span");
  bar.style.setProperty("--bar", String(.16 + ((i % 7) / 12)));
  meter.append(bar);
}}

async function publishState(next) {{
  try {{
    await fetch("/state", {{
      method: "POST",
      headers: {{ "Content-Type": "application/json" }},
      body: JSON.stringify({{ state: next }}),
      cache: "no-store"
    }});
  }} catch (_error) {{
    // Local preview still works if the state endpoint is unavailable.
  }}
}}

function setState(next, options = {{}}) {{
  if (!states.includes(next)) return;
  stateIndex = states.indexOf(next);
  screen.dataset.state = next;
  clearSpeakingMotion();
  clearSleepPeek();
  screen.classList.toggle("info", next === "disconnected");
  stateLabel.textContent = labels[next];
  tileVoice.textContent = labels[next];
  expression.textContent = expressions[next];
  for (const button of controls.children) {{
    button.setAttribute("aria-pressed", String(button.dataset.state === next));
  }}
  const params = new URLSearchParams(location.search);
  params.set("state", next);
  if (!options.remote) history.replaceState(null, "", `${{location.pathname}}?${{params}}`);
  if (options.publish) publishState(next);
  if (next === "speaking") scheduleSpeakingMotion();
  if (next === "sleep") {{
    seedSleepZs();
    scheduleSleepPeek();
  }}
}}

function showControls() {{
  screen.classList.add("controls-open");
  clearTimeout(controlsTimer);
  controlsTimer = setTimeout(() => screen.classList.remove("controls-open"), 4200);
}}

function cycle(delta) {{
  setState(states[(stateIndex + delta + states.length) % states.length], {{ publish: true }});
  showControls();
}}

function tickMeter() {{
  const active = screen.dataset.state;
  const level = active === "speaking" ? .9 : active === "listening" ? .62 : active === "thinking" ? .38 : .18;
  for (const [i, bar] of [...meter.children].entries()) {{
    const wave = (Math.sin(Date.now() / 180 + i * .7) + 1) / 2;
    bar.style.setProperty("--bar", String(.12 + wave * level));
  }}
  requestAnimationFrame(tickMeter);
}}

function startDemo() {{
  clearInterval(demoTimer);
  const demo = ["idle", "awake", "listening", "thinking", "speaking", "idle", "sleep"];
  let i = 0;
  demoTimer = setInterval(() => {{
    setState(demo[i % demo.length], {{ publish: true }});
    i += 1;
  }}, 1800);
}}

function randomBetween(min, max) {{
  return min + Math.random() * (max - min);
}}

function clearSpeakingMotion() {{
  clearTimeout(speakingTimer);
  screen.style.removeProperty("--speak-brow-left");
  screen.style.removeProperty("--speak-brow-right");
  screen.style.removeProperty("--speak-brow-lift");
  for (const eye of eyes) {{
    eye.style.removeProperty("--eye-x");
    eye.style.removeProperty("--eye-y");
    eye.style.removeProperty("--eye-scale-y");
  }}
  for (const pupil of pupils) {{
    pupil.style.removeProperty("--look-x");
    pupil.style.removeProperty("--look-y");
    pupil.style.removeProperty("--pupil-scale");
  }}
}}

function clearSleepPeek() {{
  clearTimeout(sleepPeekTimer);
  screen.classList.remove("peek-left", "peek-right");
  for (const pupil of pupils) {{
    pupil.style.removeProperty("--sleep-look-1-x");
    pupil.style.removeProperty("--sleep-look-1-y");
    pupil.style.removeProperty("--sleep-look-2-x");
    pupil.style.removeProperty("--sleep-look-2-y");
    pupil.style.removeProperty("--sleep-look-3-x");
    pupil.style.removeProperty("--sleep-look-3-y");
  }}
}}

function seedSleepZ(z) {{
  z.style.setProperty("--zz-left", `${{randomBetween(8, 88).toFixed(1)}}vw`);
  z.style.setProperty("--zz-top", `${{randomBetween(52, 88).toFixed(1)}}vh`);
  z.style.setProperty("--zz-size", `${{randomBetween(24, 82).toFixed(0)}}px`);
  z.style.setProperty("--zz-sway-a", `${{randomBetween(-7, 7).toFixed(1)}}vw`);
  z.style.setProperty("--zz-sway-b", `${{randomBetween(-12, 12).toFixed(1)}}vw`);
  z.style.setProperty("--zz-sway-c", `${{randomBetween(-16, 16).toFixed(1)}}vw`);
  z.style.setProperty("--zz-sway-d", `${{randomBetween(-20, 20).toFixed(1)}}vw`);
  z.style.setProperty("--zz-rot-a", `${{randomBetween(-20, 20).toFixed(0)}}deg`);
  z.style.setProperty("--zz-rot-b", `${{randomBetween(-32, 32).toFixed(0)}}deg`);
  z.style.setProperty("--zz-rot-c", `${{randomBetween(-44, 44).toFixed(0)}}deg`);
  z.style.setProperty("--zz-rot-d", `${{randomBetween(-58, 58).toFixed(0)}}deg`);
}}

function seedSleepZs() {{
  for (const z of sleepZs) {{
    seedSleepZ(z);
  }}
}}

function applySpeakingPose() {{
  const browLift = randomBetween(-5, 3).toFixed(1);
  screen.style.setProperty("--speak-brow-left", `${{randomBetween(-10, -3).toFixed(1)}}deg`);
  screen.style.setProperty("--speak-brow-right", `${{randomBetween(3, 10).toFixed(1)}}deg`);
  screen.style.setProperty("--speak-brow-lift", `${{browLift}}px`);
  for (const [index, eye] of eyes.entries()) {{
    const side = index === 0 ? -1 : 1;
    eye.style.setProperty("--eye-x", `${{randomBetween(-2, 2) + side * randomBetween(0, 1)}}px`);
    eye.style.setProperty("--eye-y", `${{randomBetween(-1.3, 1.3)}}px`);
    eye.style.setProperty("--eye-scale-y", randomBetween(.98, 1.03).toFixed(2));
  }}
  for (const [index, pupil] of pupils.entries()) {{
    const side = index === 0 ? -1 : 1;
    pupil.style.setProperty("--look-x", `${{randomBetween(-1.8, 1.8) + side * randomBetween(0, .8)}}px`);
    pupil.style.setProperty("--look-y", `${{randomBetween(-1.2, 1.2)}}px`);
    pupil.style.setProperty("--pupil-scale", randomBetween(.98, 1.03).toFixed(2));
  }}
}}

function scheduleSpeakingMotion() {{
  if (screen.dataset.state !== "speaking") return;
  applySpeakingPose();
  speakingTimer = setTimeout(scheduleSpeakingMotion, 720 + Math.random() * 640);
}}

function scheduleSleepPeek() {{
  clearTimeout(sleepPeekTimer);
  sleepPeekTimer = setTimeout(() => {{
    if (screen.dataset.state !== "sleep") return;
    const side = Math.random() > .5 ? "peek-left" : "peek-right";
    const pupil = side === "peek-left" ? pupils[0] : pupils[1];
    for (let i = 1; i <= 3; i += 1) {{
      pupil.style.setProperty(`--sleep-look-${{i}}-x`, `${{randomBetween(-11, 11)}}px`);
      pupil.style.setProperty(`--sleep-look-${{i}}-y`, `${{randomBetween(-5, 4)}}px`);
    }}
    screen.classList.add(side);
    sleepPeekTimer = setTimeout(() => {{
      screen.classList.remove(side);
      pupil.style.removeProperty("--sleep-look-1-x");
      pupil.style.removeProperty("--sleep-look-1-y");
      pupil.style.removeProperty("--sleep-look-2-x");
      pupil.style.removeProperty("--sleep-look-2-y");
      pupil.style.removeProperty("--sleep-look-3-x");
      pupil.style.removeProperty("--sleep-look-3-y");
      if (screen.dataset.state === "sleep") scheduleSleepPeek();
    }}, 2450 + Math.random() * 700);
  }}, 3600 + Math.random() * 5200);
}}

screen.addEventListener("click", showControls);
window.addEventListener("keydown", (event) => {{
  if (event.key === "ArrowRight" || event.key === " ") cycle(1);
  if (event.key === "ArrowLeft") cycle(-1);
  if (event.key === "i") screen.classList.toggle("info");
  if (event.key === "d") startDemo();
}});

for (const z of sleepZs) {{
  z.addEventListener("animationiteration", () => seedSleepZ(z));
}}

setState(new URLSearchParams(location.search).get("state") || screen.dataset.state);
async function pollState() {{
  try {{
    const response = await fetch("/state", {{ cache: "no-store" }});
    if (response.ok) {{
      const payload = await response.json();
      if (payload.state && payload.state !== screen.dataset.state) {{
        setState(payload.state, {{ remote: true }});
      }}
    }}
  }} catch (_error) {{
    // Keep the panel responsive; the service manager will restart hard failures.
  }} finally {{
    setTimeout(pollState, 250);
  }}
}}
pollState();
tickMeter();
</script>
</body>
</html>
"""


class PanelStateStore:
    def __init__(self, initial_state: str = "idle") -> None:
        self._state = initial_state if initial_state in PANEL_STATES else "idle"
        self._lock = threading.Lock()

    def get(self) -> str:
        with self._lock:
            return self._state

    def set(self, state: str) -> bool:
        if state not in PANEL_STATES:
            return False
        with self._lock:
            self._state = state
        return True


class _PreviewHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, html: str, state_store: PanelStateStore | None = None, **kwargs) -> None:  # noqa: ANN002, ANN003
        self._html = html
        self._state_store = state_store or PanelStateStore()
        super().__init__(*args, directory="/", **kwargs)

    def do_GET(self) -> None:  # noqa: N802
        self._send_panel_response(include_body=True)

    def do_HEAD(self) -> None:  # noqa: N802
        self._send_panel_response(include_body=False)

    def do_POST(self) -> None:  # noqa: N802
        if urlparse(self.path).path != "/state":
            self.send_error(404)
            return
        length = min(int(self.headers.get("Content-Length") or "0"), 4096)
        raw = self.rfile.read(length).decode("utf-8", errors="replace")
        state = ""
        if self.headers.get("Content-Type", "").split(";")[0] == "application/json":
            with contextlib.suppress(json.JSONDecodeError):
                payload = json.loads(raw or "{}")
                if isinstance(payload, dict):
                    state = str(payload.get("state") or "")
        else:
            state = parse_qs(raw).get("state", [""])[0]
        if not self._state_store.set(state):
            self.send_error(400, "invalid panel state")
            return
        self._send_state_response(include_body=True)

    def _send_panel_response(self, *, include_body: bool) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/state":
            self._send_state_response(include_body=include_body)
            return
        if parsed.path == "/":
            data = self._html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            if include_body:
                self.wfile.write(data)
            return
        self.send_error(404)

    def _send_state_response(self, *, include_body: bool) -> None:
        data = json.dumps({"state": self._state_store.get()}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if include_body:
            self.wfile.write(data)

    def log_message(self, fmt: str, *args) -> None:  # noqa: ANN002
        print(f"  [panel-preview] {fmt % args}", file=sys.stderr)


def _escape_html(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _launch_browser(url: str, *, kiosk: bool) -> subprocess.Popen | None:
    if not kiosk:
        webbrowser.open(url)
        return None
    for name in ("chromium-browser", "chromium", "google-chrome", "google-chrome-stable"):
        path = shutil.which(name)
        if not path:
            continue
        return subprocess.Popen(  # noqa: S603
            [path, "--kiosk", "--noerrdialogs", "--disable-infobars", url],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    print("No Chromium/Chrome binary found for --kiosk; opening the default browser.", file=sys.stderr)
    webbrowser.open(url)
    return None


def serve_preview(
    *,
    host: str = "127.0.0.1",
    port: int = 8787,
    initial_state: str = "idle",
    open_browser: bool = False,
    kiosk: bool = False,
) -> None:
    html = render_panel_preview_html(PreviewConfig(initial_state=initial_state))
    state_store = PanelStateStore(initial_state)
    handler = functools.partial(_PreviewHandler, html=html, state_store=state_store)
    server = http.server.ThreadingHTTPServer((host, port), handler)
    url_host = "127.0.0.1" if host in {"", "0.0.0.0"} else host
    url = f"http://{url_host}:{server.server_port}/"
    proc = _launch_browser(url, kiosk=kiosk) if open_browser or kiosk else None
    print(f"Jarvis PiPanel preview: {url}")
    if host == "0.0.0.0":
        print(f"From another device: http://<this-machine-ip>:{server.server_port}/")
    print("Keys: left/right or space cycle states, i toggles status tiles, d starts demo.")
    thread = threading.Thread(target=server.serve_forever, name="jarvis-panel-preview", daemon=True)
    thread.start()
    try:
        while thread.is_alive():
            thread.join(0.5)
    except KeyboardInterrupt:
        print("\nStopping PiPanel preview.")
    finally:
        server.shutdown()
        server.server_close()
        if proc is not None:
            with contextlib.suppress(Exception):
                proc.terminate()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the standalone Jarvis PiPanel preview.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host. Use 0.0.0.0 for LAN/device testing.")
    parser.add_argument("--port", type=int, default=8787, help="HTTP port.")
    parser.add_argument("--state", choices=PANEL_STATES, default="idle", help="Initial panel state.")
    parser.add_argument("--open", action="store_true", help="Open the preview in the default browser.")
    parser.add_argument("--kiosk", action="store_true", help="Launch Chromium/Chrome fullscreen kiosk.")
    args = parser.parse_args(argv)
    serve_preview(
        host=args.host,
        port=args.port,
        initial_state=args.state,
        open_browser=args.open,
        kiosk=args.kiosk,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
