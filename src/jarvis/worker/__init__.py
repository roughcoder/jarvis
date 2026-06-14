"""Worker tier (Phase 3c).

A standalone daemon the brain dispatches deep work + machine control to over
HTTP. It is a boundary peer: it imports NOTHING from the brain, and the brain's
worker tool is a thin HTTP client that imports nothing from here. Built and run
in isolation (`jarvis worker`), then wrapped into the system as a gated tool.
"""
