"""The brain's public surface for the in-process tiers that host it.

`orchestration/` and `connectors/` run brain machinery in-process (the Cockpit
turn tier); everything they are allowed to reach lives here, re-exported from
its true home. This is the reviewable cross-tier contract: widening it is a
deliberate act, not a side effect of one more deep import. Enforced by
`tests/unit/test_architecture_boundaries.py` — those tiers may import
`jarvis.brain.facade` and nothing else under `jarvis.brain`.

Grouped by concern; keep additions in the right group and ask whether a new
symbol really needs to cross the tier boundary before adding it.
"""

from __future__ import annotations

# Identity & the capability gate (RequestContext's true home is jarvis.runtime,
# the neutral boundary module).
from jarvis.runtime import RequestContext
from jarvis.brain.capabilities import (
    can_admin_project,
    can_edit_project,
    can_query_memory_peer,
    context_for_resolution,
    resolve_capabilities,
)
from jarvis.brain.identity import Resolution, load_users

# Boundary clients (HTTP across the network boundary; see AGENTS.md §constraints).
from jarvis.brain.gateway_client import GatewayClient
from jarvis.brain.memory_client import (
    ConclusionRecord,
    MemoryBackend,
    MemoryClient,
    MemoryMessage,
    SessionPeer,
    UnsupportedMemoryOperation,
)
from jarvis.brain.memory_outbox import CurationOutbox

# The shared think/speak turn core.
from jarvis.brain.background import BackgroundRunner
from jarvis.brain.contexts import ActiveProject, ContextStore
from jarvis.brain.session import BrainSession, TurnResult
from jarvis.brain.tracing import Tracer

# Project & registry layer.
from jarvis.brain.memory_tools import make_memory_tools
from jarvis.brain.project_management import BrainProjectClient, ProjectOperationError
from jarvis.brain.project_tools import make_project_tools
from jarvis.brain.registry import ProjectEntry, RegistryStore

# Cross-tier prompt/tool contracts.
from jarvis.brain.dialog import PROJECT_THREAD_TOOL_SURFACE_CONTRACT

__all__ = [
    "PROJECT_THREAD_TOOL_SURFACE_CONTRACT",
    "ActiveProject",
    "BackgroundRunner",
    "BrainProjectClient",
    "BrainSession",
    "ConclusionRecord",
    "ContextStore",
    "CurationOutbox",
    "GatewayClient",
    "MemoryBackend",
    "MemoryClient",
    "MemoryMessage",
    "ProjectEntry",
    "ProjectOperationError",
    "RegistryStore",
    "RequestContext",
    "Resolution",
    "SessionPeer",
    "Tracer",
    "TurnResult",
    "UnsupportedMemoryOperation",
    "can_admin_project",
    "can_edit_project",
    "can_query_memory_peer",
    "context_for_resolution",
    "load_users",
    "make_memory_tools",
    "make_project_tools",
    "resolve_capabilities",
]
