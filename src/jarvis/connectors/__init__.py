"""Connectors — non-voice channels that bridge an external surface to the brain
over the same WebSocket protocol (Phase 3b). Each is a boundary peer: it imports
nothing from the brain, holds no provider credentials, and authenticates to the
brain with a pairing token only. WhatsApp is the first (wraps `wacli`).
"""
