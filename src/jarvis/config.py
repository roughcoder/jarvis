"""Central configuration for Jarvis.

HARD CONSTRAINT (spec §3.1): every service the turn loop talks to — the LLM
gateway, the memory service, and its database — is reached over HTTP/TCP at a
*configurable* host:port. Defaults are localhost for Phase 1. In Phase 2 the
migration is purely changing these env vars (e.g. MEMORY_HOST) to a Tailscale
hostname. Nothing in code may hardcode a host, port, or assume co-location.

All values load from environment / a local .env file. Nothing is hardcoded.
"""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class _Base(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


class GatewayConfig(_Base):
    """LiteLLM proxy — the single OpenAI-compatible endpoint (spec §4, Step 1)."""

    model_config = SettingsConfigDict(env_prefix="GATEWAY_", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = 4000
    api_key: SecretStr = SecretStr("sk-jarvis-local")  # LiteLLM master/admin key
    # The voice turn loop authenticates with its OWN virtual key (alias
    # "jarvis-voice") so its calls are attributable separately from memory in
    # the gateway logs. Falls back to the master key if unset.
    client_key: SecretStr = SecretStr("")
    # End User = the SPEAKER identity. "family" when Jarvis can't tell who's
    # talking; a person's name once voice recognition can. A future speaker-ID
    # step sets this per turn; it's also the natural Honcho peer id. Filter
    # gateway logs by End User to see one person.
    speaker: str = "family"
    # The physical instance / room, attached as a log tag so multiple Jarvis
    # instances running at once are distinguishable.
    room: str = "default"
    # Per-turn model routing (spec §6 Step 1, §8): names are LiteLLM route names,
    # never provider SDK identifiers. Swapping model is a parameter, not code.
    fast_model: str = "fast"
    strong_model: str = "strong"
    # Embeddings route (LiteLLM) for the optional embedding-based tool relevance
    # scorer (§9 / WS8). Only used when TOOLS_RELEVANCE_MODE=embedding.
    embed_model: str = "embed"
    # Multimodal route used once an image enters the turn (native vision —
    # look_at_screen). Must map to a vision-capable model (e.g. gpt-4o).
    vision_model: str = "strong"
    request_timeout_s: float = 60.0
    # Stream the reply and synthesise sentence-by-sentence so speech starts on
    # the first sentence (lower felt latency). False = wait for the full reply.
    stream: bool = True

    @computed_field  # type: ignore[prop-decorator]
    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


class MemoryConfig(_Base):
    """Honcho memory service (spec §4, Step 8). Reached over HTTP only (§3.1)."""

    model_config = SettingsConfigDict(env_prefix="MEMORY_", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = 8000
    api_key: SecretStr = SecretStr("")  # shared secret on the Honcho server
    workspace_id: str = "jarvis"
    user_peer_id: str = "user"
    assistant_peer_id: str = "jarvis"
    # Hot path reads a LOCAL cache (spec §3.2), never the live reasoning endpoint.
    cache_path: str = ".cache/representation.json"
    write_timeout_s: float = 30.0
    # The turn is always WRITTEN to Honcho (facts captured immediately), but the
    # expensive dialectic cache refresh is debounced to at most once per this
    # interval — avoids a ~9s reasoning call every single turn.
    refresh_interval_s: float = 30.0

    @computed_field  # type: ignore[prop-decorator]
    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


class DatabaseConfig(_Base):
    """Postgres + pgvector backing Honcho (spec §4). Config-driven host (§3.1)."""

    model_config = SettingsConfigDict(env_prefix="DB_", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = 5432
    user: str = "honcho"
    password: SecretStr = SecretStr("honcho")
    name: str = "honcho"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def url(self) -> str:
        pw = self.password.get_secret_value()
        return f"postgresql://{self.user}:{pw}@{self.host}:{self.port}/{self.name}"

    @property
    def url_masked(self) -> str:
        return f"postgresql://{self.user}:****@{self.host}:{self.port}/{self.name}"


class TTSConfig(_Base):
    """Cloud streaming TTS (spec §4, Step 2). Behind a config-driven URL."""

    model_config = SettingsConfigDict(env_prefix="TTS_", env_file=".env", extra="ignore")

    # provider: "inworld" | "smallest" — locked candidates (spec §4).
    provider: str = "inworld"
    base_url: str = "https://api.inworld.ai"
    api_key: SecretStr = SecretStr("")  # Inworld: already-base64 Basic-auth token
    voice: str = "Ashley"
    model_id: str = "inworld-tts-2"  # Realtime model: lowest TTFA (~450ms)
    language: str = "en-US"
    sample_rate: int = 24000  # LINEAR16 PCM playback rate
    # Expressiveness of delivery: STABLE | BALANCED | CREATIVE (most emotional).
    delivery_mode: str = "BALANCED"
    # Network timeouts (tunables live in config). connect: time to establish the
    # stream; request: overall read budget for the streamed response.
    connect_timeout_s: float = 10.0
    request_timeout_s: float = 60.0


class STTConfig(_Base):
    """Local Faster-Whisper (spec §4, Step 3). English-only model."""

    model_config = SettingsConfigDict(env_prefix="STT_", env_file=".env", extra="ignore")

    # small.en ≈ 0.8s/turn vs distil-large-v3 ≈ 2.9s (Whisper STT is encoder-
    # bound: time is ~fixed by model size, not clip length). distil-large-v3 for
    # max accuracy, base.en for max speed.
    model: str = "small.en"
    device: str = "auto"            # "auto" -> cpu/metal as available
    compute_type: str = "int8"
    language: str = "en"
    beam_size: int = 1              # greedy = fastest; raise for accuracy


class VADConfig(_Base):
    """Voice activity detection (spec §4/§5). One instance drives endpointing AND barge-in."""

    model_config = SettingsConfigDict(env_prefix="VAD_", env_file=".env", extra="ignore")

    # engine: "silero" for Mac accuracy, "webrtc" for lightweight Raspberry Pi installs.
    engine: str = "silero"
    webrtc_aggressiveness: int = 2  # 0-3, higher is stricter
    # Tunables exposed from the start (spec §8).
    endpoint_silence_ms: int = 900        # trailing silence -> end of speech
    speech_threshold: float = 0.5         # endpointing sensitivity
    # Barge-in needs an AEC input path (AEC mic or headphones); without one,
    # the speakers leak into the mic and Jarvis interrupts itself. Toggle off
    # on bare speakers. (Spec §2: no software AEC.)
    bargein_enabled: bool = True
    # mode: "wakeword" = interrupt only when you say the wake word ("Hey
    # Jarvis") — robust against self-interruption even without AEC, since
    # Jarvis's own voice never says it. "vad" = interrupt on any sustained
    # speech (spec §5 default, but needs an AEC mic / headphones).
    bargein_mode: str = "wakeword"
    bargein_threshold: float = 0.6        # vad-mode barge-in sensitivity
    bargein_min_ms: int = 200             # vad-mode sustained speech to barge in
    bargein_grace_ms: int = 250           # vad-mode: ignore playback onset window
    min_speech_ms: int = 200
    # Conversation mode (spec §5 follow-up nice-to-have): after a reply, keep
    # listening for this long so the user can continue WITHOUT re-saying the wake
    # word. On silence past the window, drop back to PASSIVE (wake required).
    conversation_mode: bool = True
    conversation_timeout_ms: int = 8000


class WakeConfig(_Base):
    """Picovoice Porcupine custom 'Jarvis' keyword (spec §4, Step 6)."""

    model_config = SettingsConfigDict(env_prefix="WAKE_", env_file=".env", extra="ignore")

    # engine: "openwakeword" (FOSS, no account) | "porcupine" (needs AccessKey)
    engine: str = "openwakeword"
    # openWakeWord: pretrained model name, e.g. "hey_jarvis". Porcupine: a
    # built-in keyword name like "jarvis" (or set keyword_path to a custom file).
    keyword: str = "hey_jarvis"
    threshold: float = 0.5  # openWakeWord detection score threshold
    # --- Porcupine-only ---
    access_key: SecretStr = SecretStr("")
    keyword_path: str = ""  # optional custom .ppn; empty -> built-in `keyword`
    sensitivity: float = 0.5


class AudioConfig(_Base):
    """Microphone / playback. AEC is assumed in hardware (spec §2)."""

    model_config = SettingsConfigDict(env_prefix="AUDIO_", env_file=".env", extra="ignore")

    sample_rate: int = 16000   # mic capture rate (STT/VAD/Porcupine want 16k)
    frame_ms: int = 32         # frame size for streaming capture
    input_device: int | None = None
    output_device: int | None = None
    # Wake acknowledgement (spec §5 follow-up): how Jarvis confirms it heard the
    # wake word before listening. "beep" (earcon), "speak" (say ack_phrase via
    # TTS), or "none".
    ack_mode: str = "beep"
    ack_phrase: str = "Yes?"


class PersonaConfig(_Base):
    """Soul (static personality) + short-term conversation context."""

    model_config = SettingsConfigDict(env_prefix="PERSONA_", env_file=".env", extra="ignore")

    soul_path: str = "SOUL.md"      # personality layer injected into the prompt
    # Skills (§7): self-authored markdown recipes composing tools. Index = SKILLS.md.
    skills_dir: str = "jarvis-workspace/skills"
    history_messages: int = 16      # rolling shared context window (user+assistant)
    expressive: bool = True         # let replies use Inworld TTS-2 emotion cues
    # IANA tz name (e.g. "Europe/London") injected so Jarvis knows "now" without
    # a tool/search. Empty = the host's local timezone.
    timezone: str = ""


class TraceConfig(_Base):
    """Per-turn pipeline tracing (STT/LLM/TTS/memory timings)."""

    model_config = SettingsConfigDict(env_prefix="TRACE_", env_file=".env", extra="ignore")

    enabled: bool = True
    path: str = ".cache/traces.jsonl"   # one JSON object per turn
    console: bool = True                # also print a compact per-turn summary


class CapabilityConfig(_Base):
    """Identity + capability resolution (Phase 3 §4/§5). Deny-by-default: with no
    profile file and no default, the device is granted nothing."""

    model_config = SettingsConfigDict(env_prefix="CAPS_", env_file=".env", extra="ignore")

    # Which device this process is (the intercom/host identity). Picks the
    # profile file and, in W4, what the brain pairs it as.
    device_id: str = "local-mac"
    # Single-principal defaults for Phase 3a; per-utterance in 3b+.
    identity: str = "house"
    scope: str = "house"  # house | personal
    # profiles/<device_id>.md front-matter lists granted capabilities.
    profiles_dir: str = "jarvis-workspace/profiles"
    # users/<name>.md: per-user identity bindings + grants (Phase 3d, §5/§10).
    users_dir: str = "jarvis-workspace/users"
    # CSV fallback used only when no profile file exists for this device.
    default_capabilities: str = ""


class AccountConfig(_Base):
    """Household email/calendar account binding metadata.

    User profile front-matter may reference bindings by name. The binding files
    themselves live in a gitignored store and contain provider/account metadata,
    never model-visible OAuth tokens.
    """

    model_config = SettingsConfigDict(env_prefix="ACCOUNTS_", env_file=".env", extra="ignore")

    bindings_dir: str = "jarvis-workspace/.accounts"
    audit_path: str = "jarvis-workspace/.accounts/audit.jsonl"
    house_email_binding: str = "house-email"
    house_calendar_binding: str = "house-calendar"


class ToolsConfig(_Base):
    """Tool layer (Phase 3 §6). Provider keys live brain-side only."""

    model_config = SettingsConfigDict(env_prefix="TOOLS_", env_file=".env", extra="ignore")

    # Hot-path guards: a single tool call can't hang a turn, and a turn can't loop
    # on tools forever.
    timeout_s: float = 8.0
    # Tool-loop rounds per turn. GUI control chains several (see -> act -> see), so
    # allow more than a simple lookup needs; raise further for heavy automation.
    max_rounds: int = 6
    # Console-log each tool call (name, gating capability, args, short result) so a
    # turn's tool/MCP activity is visible when debugging. False => silent.
    log_calls: bool = True
    # Per-turn relevance prefilter (Phase 3 §9): keep ALL tools registered + gated,
    # but only OFFER the MCP servers whose tools look relevant to the utterance, so
    # the voice prompt isn't 100+ schemas every turn. Built-in tools (web/files/
    # worker) are always offered. False => offer everything (no narrowing).
    relevance_filter: bool = True
    # How relevance is scored: "keyword" (instant, default — no hot-path network) or
    # "embedding" (semantic similarity via the gateway embeddings route; better
    # matching at the cost of one small embed call per turn). Embedding mode falls
    # back to keyword on any error.
    relevance_mode: str = "keyword"
    relevance_threshold: float = 0.30  # min cosine similarity to include a server
    # Soft heartbeat pulse cadence while a slow/remote tool (web search) runs.
    heartbeat_interval_s: float = 1.2
    # files tool sandbox root (everything resolves within this; escapes rejected).
    files_root: str = "jarvis-workspace/files"
    # web_search provider + key (tavily). No key => the tool reports unconfigured.
    websearch_provider: str = "tavily"
    websearch_api_key: SecretStr = SecretStr("")
    websearch_max_results: int = 5


class DeviceAuth(BaseModel):
    """One paired device (Phase 3d): its own pairing token, canonical id, and a
    default identity (a personal device pins its owner; a shared room device leaves
    it empty → house until a speaker is confirmed). The profile file is
    `profiles/<device_id>.md`."""

    token: str
    device_id: str
    identity: str = ""  # default principal for this device ("" => house/unknown)


class BrainConfig(_Base):
    """The brain WebSocket server (Phase 3 W4). Intercoms connect here."""

    model_config = SettingsConfigDict(env_prefix="BRAIN_", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = 8700
    # Shared pairing secret. Empty => accept any device (dev/local only).
    pairing_token: SecretStr = SecretStr("")
    # Per-device pairing (Phase 3d): a JSON array of DeviceAuth so each device has
    # its own token (a token is bound to its device_id, so a leaked Pi token can't
    # impersonate your Mac). The shared pairing_token above stays a fallback.
    # BRAIN_DEVICES='[{"token":"…","device_id":"room-pi"},{"token":"…","device_id":"neil-mac","identity":"neil"}]'
    devices: list[DeviceAuth] = []
    # No-token auth is open (dev/local). On a NON-loopback bind that's unauthenticated
    # network access, so the brain refuses to start unless a token is set OR this is on.
    allow_insecure: bool = False
    # Intercoms send captured utterances as JSON/base64 PCM. Long utterances can exceed
    # websockets' 1 MiB default frame cap, so keep the server receive limit explicit.
    websocket_max_size: int = 8 * 1024 * 1024
    websocket_ping_interval_s: float = 20.0
    websocket_ping_timeout_s: float = 60.0


class IntercomConfig(_Base):
    """The intercom client (Phase 3 W4): where the brain is + the pairing token.
    The device's own id/profile come from CapabilityConfig (CAPS_DEVICE_ID), one
    source of truth. The intercom holds NO provider credentials (review HIGH #2)."""

    model_config = SettingsConfigDict(env_prefix="INTERCOM_", env_file=".env", extra="ignore")

    brain_host: str = "localhost"
    brain_port: int = 8700
    token: SecretStr = SecretStr("")
    websocket_max_size: int = 8 * 1024 * 1024
    websocket_ping_interval_s: float = 20.0
    websocket_ping_timeout_s: float = 60.0
    websocket_open_timeout_s: float = 10.0
    websocket_close_timeout_s: float = 5.0
    network_recover_cmd: str = "/usr/local/bin/jarvis-network-recover"
    network_recover_timeout_s: float = 20.0
    network_probe_host: str = "1.1.1.1"
    network_probe_port: int = 53
    network_probe_timeout_s: float = 0.75

    @computed_field  # type: ignore[prop-decorator]
    @property
    def brain_url(self) -> str:
        return f"ws://{self.brain_host}:{self.brain_port}"


class IntercomDeviceConfig(_Base):
    """Optional local hardware on a thin intercom.

    These are resources on the edge device, not authority grants. The brain only
    exposes matching tools when the device profile grants the capability AND the
    intercom advertises live hardware at pairing.
    """

    model_config = SettingsConfigDict(env_prefix="INTERCOM_DEVICE_", env_file=".env", extra="ignore")

    # "auto" probes the Pi camera stack; "true"/"false" force advertisement.
    camera: str = "auto"
    camera_bin: str = ""  # explicit rpicam-still/libcamera-still path; empty => probe PATH
    camera_width: int = 1280
    camera_height: int = 720
    camera_timeout_s: float = 8.0
    camera_warmup_ms: int = 300
    # Preferred Pi touchscreen shell. "auto" starts only when a display session is
    # available. `eyes` is the legacy env name kept for existing Pi installs.
    pi_panel: str = ""
    pi_panel_sleep_after_s: float = 0.0
    pi_panel_geometry: str = ""
    pi_panel_url: str = ""
    eyes: str = "auto"
    eyes_sleep_after_s: float = 90.0

    @property
    def pi_panel_setting(self) -> str:
        return self.pi_panel or self.eyes

    @property
    def pi_panel_sleep_s(self) -> float:
        return self.pi_panel_sleep_after_s or self.eyes_sleep_after_s


class WorkerConfig(_Base):
    """The worker Mac daemon (Phase 3c): a boundary peer the brain dispatches deep
    work + machine control to. Token-authed; provider/agent binaries are local to
    the worker. The brain reaches it at host:port; the daemon binds bind_host
    (default = host)."""

    model_config = SettingsConfigDict(env_prefix="WORKER_", env_file=".env", extra="ignore")

    host: str = "localhost"          # where the brain reaches the worker
    port: int = 8780
    bind_host: str = ""              # daemon bind addr; empty => host
    token: SecretStr = SecretStr("")  # shared pairing token
    allow_insecure: bool = False     # permit a no-token, non-loopback bind (else refuse to start)
    workspace: str = "~/.jarvis/worker"  # default cwd for actions/jobs
    # Where the user's git repos live, so a job can name a repo ("polymarket")
    # instead of an absolute path. Empty = names must be absolute paths.
    repo_root: str = ""
    # If a named repo isn't found under repo_root, clone it there with `gh repo
    # clone <name>` (auth handled by gh; bare names resolve to your namespace,
    # cross-org repos need "org/name").
    clone_missing: bool = True
    clone_timeout_s: float = 240.0
    agent: str = "codex"             # default coding agent: codex | claude
    codex_bin: str = "codex"
    claude_bin: str = "claude"
    peekaboo_bin: str = "peekaboo"   # GUI automation (worker.gui; install + perms)
    # peekaboo's OWN agent (`control_mac`) needs an AI provider. The worker injects
    # these into the peekaboo subprocess. Leave the base URL empty for direct OpenAI;
    # set it to the LiteLLM gateway (http://<gateway>/v1) to route the agent through
    # the same proxy as the voice loop. `providers` is peekaboo's "openai/<model>".
    peekaboo_ai_providers: str = ""              # e.g. "openai/gpt-4o" or "openai/strong"
    peekaboo_openai_base_url: str = ""           # e.g. http://localhost:4000/v1 (LiteLLM)
    peekaboo_openai_api_key: SecretStr = SecretStr("")
    peekaboo_openrouter_api_key: SecretStr = SecretStr("")  # for --model openrouter/<p>/<m>
    # The model `control_mac` passes to `peekaboo agent --model` (peekaboo's own default
    # is gpt-5.5). Must be a peekaboo-supported name — the OpenAI family is gpt-5.x (NOT
    # gpt-4o); or "openrouter/<provider>/<model>" to dodge OpenAI project restrictions.
    peekaboo_agent_model: str = "gpt-5.5"
    # control_mac (the agent) is multi-step and slow — its own budget, separate from
    # the 30s shell timeout, so it isn't strangled mid-task.
    peekaboo_agent_timeout_s: float = 120.0
    # control_mac is one-shot (no way to answer a mid-task prompt), and the user asked
    # for the task by voice — so by default tell the agent it's authorised to finish
    # without pausing for confirmation (it still avoids clearly catastrophic actions).
    # False => the agent may stop and ask; Jarvis relays the question to the user.
    peekaboo_agent_autonomous: bool = True
    # Repo jobs run on an isolated worktree branch "<prefix>/<name>-<id>", never
    # the user's checkout.
    worktree_branch_prefix: str = "jarvis"
    verbose: bool = True             # log each dispatched action + full peekaboo output
    # Secrets Jarvis may USE (not see) in shell commands: a comma-separated allowlist
    # of env var NAMES the worker injects into the shell environment (read from the
    # worker's own .env / environment). The model references them by name — e.g.
    # `curl -H "Authorization: Bearer $OPENAI_API_KEY"` — and never sees the value.
    # Empty => no secrets exposed (deny-by-default). Opt in deliberately: anything
    # with worker.shell could also print these, so only list what you're comfortable with.
    shell_secrets: str = ""
    shell_timeout_s: float = 30.0    # sync shell/applescript max runtime
    job_timeout_s: float = 1800.0    # background code job max runtime (30 min)
    request_timeout_s: float = 40.0  # brain->worker HTTP timeout (> shell_timeout)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


class RemoteConfig(_Base):
    """Claude Managed Agents — the cloud coding lane (Phase 3, PHASE3.md §8). A
    remote job runs in an Anthropic-managed sandbox. Agent + environment ids are
    created once by `jarvis remote-setup`."""

    model_config = SettingsConfigDict(env_prefix="ANTHROPIC_", env_file=".env", extra="ignore")

    api_key: SecretStr = SecretStr("")  # ANTHROPIC_API_KEY (also used by the gateway stack)
    agent_id: str = ""                  # ANTHROPIC_AGENT_ID (from remote-setup)
    environment_id: str = ""            # ANTHROPIC_ENVIRONMENT_ID (from remote-setup)
    base_url: str = "https://api.anthropic.com"
    model: str = "claude-opus-4-8"
    beta: str = "managed-agents-2026-04-01"
    request_timeout_s: float = 60.0

    @computed_field  # type: ignore[prop-decorator]
    @property
    def configured(self) -> bool:
        return bool(self.api_key.get_secret_value() and self.agent_id and self.environment_id)


class MCPServerSpec(BaseModel):
    """One MCP server the bridge connects to (Phase 3 §6). Mirrors the shape of a
    Claude-Code `mcpServers` entry so existing servers drop in unchanged: a
    `stdio` server is a `command` + `args` (a long-lived subprocess); an `http`
    server is a `url`. Every server declares the capability a device profile must
    grant before its tools are offered — the firewall against tool sprawl."""

    name: str  # short id: capability default + tool-name namespace
    transport: str = "stdio"  # stdio | http
    # stdio transport
    command: str = ""
    args: list[str] = []
    env: dict[str, str] = {}  # extra env for the subprocess (e.g. a vault path)
    # http transport (streamable HTTP)
    url: str = ""
    headers: dict[str, str] = {}  # static auth headers; set => skip OAuth for this server
    scope: str = ""  # optional OAuth scope to request at `jarvis mcp login`
    # OAuth client auth method to request at registration. Default "none" (a public
    # client + PKCE) — the right model for MCP clients, and what avoids the
    # "token exchange 401" some servers (WorkOS/AuthKit, e.g. Granola) return when
    # registered confidential. Override per server if one requires a secret.
    token_endpoint_auth_method: str = "none"
    # Capability a profile must grant for these tools (empty => "mcp.<name>").
    capability: str = ""
    # Optional allow-list of tool names to expose from this server; empty = all
    # (still capped by MCPConfig.max_tools_per_server). The per-server firewall.
    include: list[str] = []
    # Optional extra trigger words for the per-turn relevance prefilter (§9), on top
    # of the server name + words auto-derived from its tool names. Use for synonyms
    # the tool names miss, e.g. linear -> ["ticket","bug","sprint"].
    keywords: list[str] = []

    @property
    def required_capability(self) -> str:
        return self.capability or f"mcp.{self.name}"


class MCPConfig(_Base):
    """MCP bridge (Phase 3 §6): the native MCP client + the profile-gated work
    bundle. The brain connects to each server once (cold/startup), discovers its
    tools, and registers them gated + timeout-bounded. No bridged call ever lands
    on the hot path uncapped (constraint #2)."""

    model_config = SettingsConfigDict(env_prefix="MCP_", env_file=".env", extra="ignore")

    enabled: bool = False
    # JSON array of MCPServerSpec, e.g. MCP_SERVERS='[{"name":"context7",...}]'.
    servers: list[MCPServerSpec] = []
    # Hot-path guard: a bridged call is hard-bounded here (in addition to the
    # registry's tools.timeout_s). Connect/discovery happens off the hot path.
    call_timeout_s: float = 20.0
    connect_timeout_s: float = 20.0
    # Tool-sprawl firewall: cap how many tools any one server can contribute, so
    # a chatty server can't flood the model's tool list.
    max_tools_per_server: int = 40
    # Namespace tool names as "<server>_<tool>" to avoid clashes across servers
    # and with built-in tools.
    namespace: bool = True
    # OAuth (http servers): where per-server tokens persist (gitignore it), and the
    # localhost port the `jarvis mcp login` browser flow redirects back to. Auth is
    # interactive ONLY in that command; the brain refreshes cached tokens silently
    # and never pops a browser on the voice path.
    auth_dir: str = "jarvis-workspace/.mcp-auth"
    oauth_redirect_port: int = 41760


class GoogleConfig(_Base):
    """Current email/calendar adapter (Phase 3 §6): Jarvis's OWN Gmail + Calendar
    house account, via the `gogcli` CLI. The registered tool capabilities are
    provider-neutral (`email.*` / `calendar.*`); this config is only the adapter
    wiring. A thin client like the worker: it shells out to a local,
    separately-authenticated binary; provider credentials never live in the tool.
    `jarvis google-setup` does the one-time OAuth."""

    model_config = SettingsConfigDict(env_prefix="GOOGLE_", env_file=".env", extra="ignore")

    gogcli_bin: str = "gog"  # the openclaw/tap/gogcli formula installs a `gog` binary
    timeout_s: float = 20.0
    calendar_days: int = 7  # default look-ahead for "upcoming events"


class HeartbeatConfig(_Base):
    """Proactive heartbeat (Phase 3b, §9). A COLD-path scheduler: it periodically
    works the HEARTBEAT.md checklist and pushes a Proactive message ONLY when it has
    something worth saying (the silent-completion sentinel) — never on the voice hot
    path, never into the conversational transcript."""

    model_config = SettingsConfigDict(env_prefix="HEARTBEAT_", env_file=".env", extra="ignore")

    enabled: bool = False
    interval_s: float = 900.0  # how often to run the checklist (15 min)
    path: str = "jarvis-workspace/HEARTBEAT.md"  # the proactive checklist
    sentinel: str = "NO_REPLY"  # the model emits this when nothing's worth saying


class BackgroundConfig(_Base):
    """Background-task lane (fire-and-forget). Jarvis kicks off a slow, multi-step
    task — book a table, deep research, a long Mac job — says 'on it' immediately on
    the hot path, runs the work DETACHED (its own headless agentic tool loop, with the
    SAME capabilities as the asker — never more), and pushes the outcome as a Proactive
    message when it finishes. Gated by the `background.run` capability (deny-by-default).
    Off the hot path by construction; a concurrency cap + per-job timeout bound it."""

    model_config = SettingsConfigDict(env_prefix="BACKGROUND_", env_file=".env", extra="ignore")

    enabled: bool = True
    max_concurrent: int = 3  # reject new jobs past this many running at once
    timeout_s: float = 600.0  # hard ceiling per job (10 min)
    max_rounds: int = 12  # tool-loop rounds for a job (more than a voice turn — it's unattended)


class NotifyConfig(_Base):
    """Notification routing. A notification (background-job result, heartbeat) always
    goes to the user's device; with `also_whatsapp` it ALSO goes to them on WhatsApp
    (via the connector, to the number in their `users/<name>.md`) so it reaches them
    when they're out. Alarms stay device-local regardless."""

    model_config = SettingsConfigDict(env_prefix="NOTIFY_", env_file=".env", extra="ignore")

    also_whatsapp: bool = False
    # Idle-aware timing: a notification that arrives mid-conversation is held and
    # delivered at the next gap (never spoken over the user). Quiet hours suppress
    # spoken notifications (HH:MM..HH:MM, wraps midnight; empty = off). Alarms ignore
    # both — they're meant to interrupt.
    quiet_start: str = ""  # e.g. "22:00"
    quiet_end: str = ""  # e.g. "07:00"


class AlarmConfig(_Base):
    """Alarms & timers — scheduled proactive events that fire on the device they were
    set on and REPEAT until acknowledged ('stop'). The sound and the repeat cadence are
    config so they're trivial to change without touching code. The cadence is a simple
    ring/quiet cycle: ring for `ring_s`, pause for `quiet_s`, repeat until acknowledged
    or `max_s` elapses (a safety auto-stop)."""

    model_config = SettingsConfigDict(env_prefix="ALARM_", env_file=".env", extra="ignore")

    enabled: bool = True
    ring_s: float = 10.0  # how long it rings each cycle
    quiet_s: float = 10.0  # pause between rings
    max_s: float = 300.0  # auto-stop after this long unacknowledged (safety net)
    tick_s: float = 1.0  # scheduler resolution
    sound: str = "chime"  # tone name (generated) or a path to a sound file — easily swapped
    tone_freq: float = 880.0  # generated-tone pitch in Hz (tweak the sound without a file)


class BrowserConfig(_Base):
    """Browser lane — a real Chrome the worker drives over CDP (nodriver, no
    Playwright); the brain acts on it over HTTP via the worker. Two device-scoped
    contexts share one host: 'jarvis' (his own headed, persistent profile — his
    accounts, zero setup) and 'device' (the machine's default Chrome profile — its
    real logins). Headed by default (less detectable + you can take the wheel for a
    login/captcha). Gated by the `worker.browser` capability; per-device default via
    the profile's `browser_default`."""

    model_config = SettingsConfigDict(env_prefix="BROWSER_", env_file=".env", extra="ignore")

    enabled: bool = True
    chrome_path: str = ""  # explicit Chrome binary; "" => nodriver autodetect
    jarvis_profile_dir: str = "jarvis-workspace/browser/jarvis-profile"  # persistent own profile
    device_profile_dir: str = ""  # the OS default Chrome profile dir (the 'device' context); "" => unset
    default_context: str = "jarvis"  # context when unspecified (profile browser_default overrides)
    headless: bool = False  # headed by default; tests/headless servers override
    nav_timeout_s: float = 30.0  # per navigation/action budget on the host
    request_timeout_s: float = 45.0  # brain -> worker HTTP budget for a browser action


class WhatsAppConfig(_Base):
    """WhatsApp connector (Phase 3b): a boundary peer wrapping `wacli` (a WhatsApp
    CLI). Inbound messages become brain turns (channel=whatsapp, identity=number);
    replies go back out via wacli. The connector authenticates to the brain with a
    pairing token only — it holds no provider credentials."""

    model_config = SettingsConfigDict(env_prefix="WHATSAPP_", env_file=".env", extra="ignore")

    enabled: bool = False
    wacli_bin: str = "wacli"
    account: str = ""  # wacli named account (--account); empty => wacli's default
    device_id: str = "whatsapp"
    token: SecretStr = SecretStr("")  # brain pairing token for this connector
    poll_interval_s: float = 2.0
    # Access control (like OpenClaw's dmPolicy/allowFrom) — who may message the bot.
    # "allowlist" (default, deny-by-default): only numbers in allow_from. "pairing": an
    # unknown number triggers an admin-approved onboarding (a new user.md is written).
    # "open": anyone (drives the LLM — unsafe). "disabled": ignore all inbound.
    dm_policy: str = "allowlist"
    allow_from: str = ""  # CSV of allowed E.164 numbers, e.g. "447921815819,447999246830"
    admin: str = ""  # the number that approves pairings (pairing policy); empty => nobody can
    text_chunk_limit: int = 4000  # split long replies (WhatsApp message length limit)
    # Group behaviour: "ignore" (default — never reply in groups), "mention" (reply only
    # when called out by the trigger name), or "open" (reply to every group message — noisy).
    group_policy: str = "ignore"
    group_allow: str = ""  # CSV of allowed group JIDs (empty = any group it's added to)
    trigger: str = "jarvis"  # the name that "calls out" the bot in a group (case-insensitive)


class OrchestrationConfig(_Base):
    """Agentic work orchestration state.

    This is private local operational state: run graphs, schedule definitions,
    and optional worker profile metadata. Public trackers reflect state, but do
    not own it.
    """

    model_config = SettingsConfigDict(env_prefix="ORCHESTRATION_", env_file=".env", extra="ignore")

    workspace: str = "jarvis-workspace/orchestration"
    workers_path: str = "jarvis-workspace/orchestration/workers.json"
    schedules_path: str = "jarvis-workspace/orchestration/schedules.json"
    default_repo: str = ""
    default_timezone: str = "Europe/London"
    landing_mode: str = "draft_pr"


class LinearConfig(_Base):
    """Linear work-source credentials."""

    model_config = SettingsConfigDict(env_prefix="LINEAR_", env_file=".env", extra="ignore")

    api_key: SecretStr = SecretStr("")


class Config:
    """Aggregate config. Construct once and pass modules their slice."""

    def __init__(self) -> None:
        env_file = os.environ.get("JARVIS_ENV_FILE") or ".env"
        source = {"_env_file": env_file}
        state_base = _env_file_base(env_file)

        self.gateway = GatewayConfig(**source)
        self.memory = MemoryConfig(**source)
        self.database = DatabaseConfig(**source)
        self.tts = TTSConfig(**source)
        self.stt = STTConfig(**source)
        self.vad = VADConfig(**source)
        self.wake = WakeConfig(**source)
        self.audio = AudioConfig(**source)
        self.persona = PersonaConfig(**source)
        self.trace = TraceConfig(**source)
        self.capabilities = CapabilityConfig(**source)
        self.accounts = AccountConfig(**source)
        self.tools = ToolsConfig(**source)
        self.brain = BrainConfig(**source)
        self.intercom = IntercomConfig(**source)
        self.intercom_device = IntercomDeviceConfig(**source)
        self.worker = WorkerConfig(**source)
        self.remote = RemoteConfig(**source)
        self.mcp = MCPConfig(**source)
        self.heartbeat = HeartbeatConfig(**source)
        self.background = BackgroundConfig(**source)
        self.notify = NotifyConfig(**source)
        self.alarm = AlarmConfig(**source)
        self.browser = BrowserConfig(**source)
        self.whatsapp = WhatsAppConfig(**source)
        self.google = GoogleConfig(**source)
        self.orchestration = OrchestrationConfig(**source)
        self.linear = LinearConfig(**source)
        self._resolve_private_state_paths(state_base)

    def _resolve_private_state_paths(self, base_dir: Path) -> None:
        self.capabilities.profiles_dir = _resolve_state_path(
            self.capabilities.profiles_dir, base_dir
        )
        self.capabilities.users_dir = _resolve_state_path(self.capabilities.users_dir, base_dir)
        self.orchestration.workspace = _resolve_state_path(
            self.orchestration.workspace, base_dir
        )
        self.orchestration.workers_path = _resolve_state_path(
            self.orchestration.workers_path, base_dir
        )
        self.orchestration.schedules_path = _resolve_state_path(
            self.orchestration.schedules_path, base_dir
        )

    def resolved(self) -> dict:
        """Flat, secret-masked view for the dry-run printout (Step 0 gate)."""

        def mask(v: SecretStr) -> str:
            s = v.get_secret_value()
            return "<set>" if s else "<unset>"

        return {
            "gateway.base_url": self.gateway.base_url,
            "gateway.api_key": mask(self.gateway.api_key),
            "gateway.fast_model": self.gateway.fast_model,
            "gateway.strong_model": self.gateway.strong_model,
            "memory.base_url": self.memory.base_url,
            "memory.api_key": mask(self.memory.api_key),
            "memory.workspace_id": self.memory.workspace_id,
            "memory.user_peer_id": self.memory.user_peer_id,
            "memory.assistant_peer_id": self.memory.assistant_peer_id,
            "memory.cache_path": self.memory.cache_path,
            "database.host": self.database.host,
            "database.port": self.database.port,
            "database.url": self.database.url_masked,
            "tts.provider": self.tts.provider,
            "tts.base_url": self.tts.base_url,
            "tts.api_key": mask(self.tts.api_key),
            "tts.voice": self.tts.voice,
            "stt.model": self.stt.model,
            "stt.device": self.stt.device,
            "vad.endpoint_silence_ms": self.vad.endpoint_silence_ms,
            "vad.speech_threshold": self.vad.speech_threshold,
            "vad.bargein_enabled": self.vad.bargein_enabled,
            "vad.bargein_mode": self.vad.bargein_mode,
            "vad.bargein_threshold": self.vad.bargein_threshold,
            "wake.engine": self.wake.engine,
            "wake.keyword": self.wake.keyword,
            "wake.threshold": self.wake.threshold,
            "wake.access_key": mask(self.wake.access_key),
            "wake.keyword_path": self.wake.keyword_path or "<built-in>",
            "audio.sample_rate": self.audio.sample_rate,
            "audio.frame_ms": self.audio.frame_ms,
            "audio.ack_mode": self.audio.ack_mode,
            "persona.soul_path": self.persona.soul_path,
            "persona.skills_dir": self.persona.skills_dir,
            "persona.history_messages": self.persona.history_messages,
            "trace.enabled": self.trace.enabled,
            "trace.path": self.trace.path,
            "capabilities.device_id": self.capabilities.device_id,
            "capabilities.identity": self.capabilities.identity,
            "capabilities.scope": self.capabilities.scope,
            "capabilities.profiles_dir": self.capabilities.profiles_dir,
            "capabilities.users_dir": self.capabilities.users_dir,
            "capabilities.default_capabilities": (
                self.capabilities.default_capabilities or "<none — deny-by-default>"
            ),
            "accounts.bindings_dir": self.accounts.bindings_dir,
            "accounts.audit_path": self.accounts.audit_path,
            "accounts.house_email_binding": self.accounts.house_email_binding,
            "accounts.house_calendar_binding": self.accounts.house_calendar_binding,
            "tools.files_root": self.tools.files_root,
            "tools.websearch_provider": self.tools.websearch_provider,
            "tools.websearch_api_key": mask(self.tools.websearch_api_key),
            "tools.timeout_s": self.tools.timeout_s,
            "tools.log_calls": self.tools.log_calls,
            "tools.relevance_mode": self.tools.relevance_mode,
            "brain.host": self.brain.host,
            "brain.port": self.brain.port,
            "brain.pairing_token": mask(self.brain.pairing_token),
            "brain.websocket_max_size": self.brain.websocket_max_size,
            "brain.websocket_ping_interval_s": self.brain.websocket_ping_interval_s,
            "brain.websocket_ping_timeout_s": self.brain.websocket_ping_timeout_s,
            "brain.devices": (
                ", ".join(f"{d.device_id}->{d.identity or 'house'}" for d in self.brain.devices)
                or "<none — shared token>"
            ),
            "intercom.brain_url": self.intercom.brain_url,
            "intercom.token": mask(self.intercom.token),
            "intercom.websocket_max_size": self.intercom.websocket_max_size,
            "intercom.websocket_ping_interval_s": self.intercom.websocket_ping_interval_s,
            "intercom.websocket_ping_timeout_s": self.intercom.websocket_ping_timeout_s,
            "intercom.websocket_open_timeout_s": self.intercom.websocket_open_timeout_s,
            "intercom.websocket_close_timeout_s": self.intercom.websocket_close_timeout_s,
            "intercom.network_recover_cmd": self.intercom.network_recover_cmd,
            "intercom.network_recover_timeout_s": self.intercom.network_recover_timeout_s,
            "intercom.network_probe_host": self.intercom.network_probe_host,
            "intercom.network_probe_port": self.intercom.network_probe_port,
            "intercom.network_probe_timeout_s": self.intercom.network_probe_timeout_s,
            "intercom_device.camera": self.intercom_device.camera,
            "intercom_device.camera_bin": self.intercom_device.camera_bin or "<auto>",
            "intercom_device.pi_panel": self.intercom_device.pi_panel_setting,
            "intercom_device.pi_panel_sleep_after_s": self.intercom_device.pi_panel_sleep_s,
            "intercom_device.pi_panel_geometry": self.intercom_device.pi_panel_geometry or "<auto>",
            "intercom_device.pi_panel_url": self.intercom_device.pi_panel_url or "<disabled>",
            "worker.base_url": self.worker.base_url,
            "worker.token": mask(self.worker.token),
            "worker.agent": self.worker.agent,
            "worker.workspace": self.worker.workspace,
            "worker.repo_root": self.worker.repo_root or "<unset>",
            "remote.api_key": mask(self.remote.api_key),
            "remote.configured": self.remote.configured,
            "remote.model": self.remote.model,
            "mcp.enabled": self.mcp.enabled,
            "mcp.servers": (
                ", ".join(f"{s.name}[{s.transport}]" for s in self.mcp.servers)
                or "<none>"
            ),
            "mcp.call_timeout_s": self.mcp.call_timeout_s,
            "heartbeat.enabled": self.heartbeat.enabled,
            "heartbeat.interval_s": self.heartbeat.interval_s,
            "background.enabled": self.background.enabled,
            "background.max_concurrent": self.background.max_concurrent,
            "background.timeout_s": self.background.timeout_s,
            "notify.also_whatsapp": self.notify.also_whatsapp,
            "alarm.enabled": self.alarm.enabled,
            "alarm.ring_s": self.alarm.ring_s,
            "alarm.quiet_s": self.alarm.quiet_s,
            "alarm.sound": self.alarm.sound,
            "browser.enabled": self.browser.enabled,
            "browser.default_context": self.browser.default_context,
            "browser.headless": self.browser.headless,
            "browser.jarvis_profile_dir": self.browser.jarvis_profile_dir,
            "browser.device_profile_dir": self.browser.device_profile_dir or "<unset>",
            "whatsapp.enabled": self.whatsapp.enabled,
            "whatsapp.wacli_bin": self.whatsapp.wacli_bin,
            "google.gogcli_bin": self.google.gogcli_bin,
            "orchestration.workspace": self.orchestration.workspace,
            "orchestration.workers_path": self.orchestration.workers_path,
            "orchestration.schedules_path": self.orchestration.schedules_path,
            "orchestration.default_repo": self.orchestration.default_repo or "<unset>",
            "orchestration.landing_mode": self.orchestration.landing_mode,
            "linear.api_key": mask(self.linear.api_key),
        }


def is_loopback(host: str) -> bool:
    """True if `host` only accepts local connections — no token needed to be safe."""
    return host in ("localhost", "127.0.0.1", "::1")


def insecure_bind(host: str, has_token: bool, allow_insecure: bool) -> bool:
    """True if binding here would be unauthenticated network access (non-loopback +
    no token) and not explicitly allowed — i.e. the server should refuse to start."""
    return not is_loopback(host) and not has_token and not allow_insecure


def load_config() -> Config:
    return Config()


def _env_file_base(env_file: str) -> Path:
    path = Path(env_file).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.resolve(strict=False).parent


def _resolve_state_path(value: str, base_dir: Path) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return str(path.resolve(strict=False))
