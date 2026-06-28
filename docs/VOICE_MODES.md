# Voice Modes

Voice modes control how long Jarvis stays active after a spoken reply. They are
voice-only: text and WhatsApp remain discrete message turns.

## Modes

### Default

Default mode is for normal household use. Jarvis gives short, complete spoken
answers and closes the mic after completed tasks, alarms, timers, time/weather
answers, simple factual answers, and polite endings.

Jarvis only keeps listening when the user is clearly exploring, planning,
troubleshooting, or asking a multi-step question.

Examples:

```text
Jarvis, set an alarm for seven.
Jarvis, what's the weather?
Jarvis, what time is it?
```

These should be answered and closed.

### Stay

Stay mode is for longer back-and-forth conversation. Jarvis keeps listening
through silence windows until the user explicitly exits.

Examples:

```text
Jarvis, stay with me.
Jarvis, go into stay mode.
Jarvis, keep listening.
Jarvis, let's chat for a bit.
```

Exit examples:

```text
Jarvis, exit stay mode.
Jarvis, stop listening.
Jarvis, go to sleep.
Jarvis, that's enough.
Jarvis, bye.
```

## Identity

On shared voice devices, a spoken identity claim such as "it's Neil" lasts for
the current conversation. If the user enters stay mode, the claim lasts until
stay mode exits.

Dedicated devices keep their configured default identity. If another user
temporarily identifies themselves for a conversation or stay-mode session, the
device falls back to its default identity when that conversation or mode ends.

## Developer Contract

Add new voice modes as profiles, not one-off booleans. A mode should define:

```text
name
listening_policy
exit_policy
identity_scope
prompt_style
```

Current modes:

```text
default: short-task assistant; default closed unless a follow-up is expected
stay: persistent voice session; explicit exit only
```

The brain returns voice-mode metadata on `ReplyEnd`:

```text
ended
continue_listening
voice_mode
close_reason
```

The intercom uses `continue_listening` and `voice_mode` to decide whether to
open another capture window. When a default follow-up window times out, the
intercom sends `conversation_idle` so the brain can reset temporary identity.

Hard exits such as `stop listening`, `go to sleep`, and `bye` are handled before
the LLM. Softer conversation decisions come from the voice prompt's structured
control markers, with deterministic backstops for completed local tools such as
alarms.
