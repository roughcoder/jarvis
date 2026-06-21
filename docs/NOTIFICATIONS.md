# Alarms, timers & proactive notifications

How Jarvis reaches out to you — alarms/timers you set, and notifications he initiates
(a background job finished, a heartbeat thought). The model: a **proactive event** is a
signal, then it says its piece, then it waits for your reply (the mic opens, no wake word
needed) — Jarvis tapping you on the shoulder, not a one-way announcement.

## The UX rule (who opened the mic?)
- **You start it** → say **"Hey Jarvis"** first (setting an alarm, asking anything).
- **Jarvis started it** (an alarm ringing, a notification just spoke) → the mic is
  already open; **just talk** ("stop", "yes, book it", a follow-up).

## Alarms & timers
Set by voice or text: *"in 30 minutes"*, *"at 10:20"*, *"a timer for five seconds"* →
`set_alarm` (gated `alarms.set`); also `cancel_alarm`, `list_alarms`.

- **Fires on the device that set it** (device-local — it isn't your problem elsewhere).
- **Repeats until acknowledged**: rings for `ALARM_RING_S`, pauses `ALARM_QUIET_S`,
  repeats until you say **"stop"/"dismiss"/"enough"** (or `ALARM_MAX_S` elapses — a safety
  auto-stop). The mic opens after each tone, so the ack needs no wake word (say it in the
  quiet gap after a tone, not over it).
- **Sound + cadence are config** (`ALARM_SOUND`, `ALARM_TONE_FREQ`, `ALARM_RING_S`,
  `ALARM_QUIET_S`) — a generated tone by default; point `ALARM_SOUND` at a file to swap it.
- Alarms **ignore quiet hours and the idle-hold** — they're meant to interrupt.

Implementation: `brain/scheduler.py` (pure cadence logic), `tools/alarm.py`,
delivery via `server._deliver_ring` → the proactive audio path.

## Proactive voice delivery
A proactive event plays as **tone + spoken text** on the device, then (for notifications)
**opens the mic for a reply**. TTS is brain-side, so the brain synthesises the audio and
streams it (`Proactive` header + ReplyAudio frames under a `pa-` turn id, then ReplyEnd);
text clients (`jarvis text`, WhatsApp) use the header text. See `brain/proactive.py` +
`brain/tones.py`; the intercom plays it via its socket router (`intercom/client.py`).

## Multi-channel routing
A notification (background result, heartbeat) goes to the user's **device**, and — with
`NOTIFY_ALSO_WHATSAPP=true` — also to them on **WhatsApp** (the number in
`users/<name>.md`), via the connector, so it reaches them when out. A background job's
result routes to the **asker**, not broadcast. Alarms stay device-local.

## Idle-aware timing
A notification that arrives **mid-conversation is held** and delivered at the next gap
(never spoken over you, never lost) — tracked per device (`busy`), flushed by the
proactive tick once idle. **Quiet hours** (`NOTIFY_QUIET_START`/`NOTIFY_QUIET_END`,
HH:MM, wraps midnight) suppress *spoken* notifications; WhatsApp delivery isn't held.
Alarms bypass both.

## Config (`.env.example`)
`ALARM_*` (enabled, ring/quiet/max, sound, tone_freq), `NOTIFY_ALSO_WHATSAPP`,
`NOTIFY_QUIET_START`/`NOTIFY_QUIET_END`, `BACKGROUND_*`, `HEARTBEAT_*`.

## Tested vs human-verified
- **Unit-tested:** the cadence (repeat-until-ack, auto-stop, cancel), time resolution,
  quiet-hours window, the proactive frame builder + tone, WhatsApp forward routing, the
  intercom queue routing.
- **Live end-to-end (proven):** set timer → rings → repeat → "stop" → silence; the brain
  emitting real tone+TTS audio frames; alarm rings carry `open_mic`.
- **Human-verified:** how the tone/voice actually *sound* from the speaker
  (config-swappable). **Self-skips until provisioned:** live WhatsApp send (needs `wacli`).
