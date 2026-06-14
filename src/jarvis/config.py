"""Central configuration for Jarvis.

HARD CONSTRAINT (spec §3.1): every service the turn loop talks to — the LLM
gateway, the memory service, and its database — is reached over HTTP/TCP at a
*configurable* host:port. Defaults are localhost for Phase 1. In Phase 2 the
migration is purely changing these env vars (e.g. MEMORY_HOST) to a Tailscale
hostname. Nothing in code may hardcode a host, port, or assume co-location.

All values load from environment / a local .env file. Nothing is hardcoded.
"""

from __future__ import annotations

from pydantic import SecretStr, computed_field
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
    request_timeout_s: float = 60.0

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


class STTConfig(_Base):
    """Local Faster-Whisper (spec §4, Step 3). English-only model."""

    model_config = SettingsConfigDict(env_prefix="STT_", env_file=".env", extra="ignore")

    model: str = "distil-large-v3"  # or "turbo"
    device: str = "auto"            # "auto" -> cpu/metal as available
    compute_type: str = "int8"
    language: str = "en"


class VADConfig(_Base):
    """Silero VAD (spec §4/§5). One instance drives endpointing AND barge-in."""

    model_config = SettingsConfigDict(env_prefix="VAD_", env_file=".env", extra="ignore")

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


class Config:
    """Aggregate config. Construct once and pass modules their slice."""

    def __init__(self) -> None:
        self.gateway = GatewayConfig()
        self.memory = MemoryConfig()
        self.database = DatabaseConfig()
        self.tts = TTSConfig()
        self.stt = STTConfig()
        self.vad = VADConfig()
        self.wake = WakeConfig()
        self.audio = AudioConfig()

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
        }


def load_config() -> Config:
    return Config()
