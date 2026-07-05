# Dreamer Tombstone Decision

Status: ACCEPTED 2026-07-05 — revised Option 1

## Problem

Honcho's dreamer can re-derive a deleted deductive or inductive conclusion from
the still-existing source messages, minting a new conclusion id on the next pass.
An id-keyed tombstone is therefore not durable for derived facts. Explicit
conclusions have no source messages and remain deleted after a plain delete.

## Decision

Use Option 1: when Jarvis forgets or corrects a derived conclusion, delete the
current conclusion and write a direct Lane 2 contradiction conclusion on the
same peer. The contradiction is authored explicitly by Jarvis, carries the full
metadata envelope, and states that the user retracted the fact and does not want
it retained as current.

Live verification revised the enforcement mechanism: Honcho is no longer trusted
to honour the contradiction at answer time. Jarvis also records an active local
retraction for the peer, reads that local index on the ambient hot path, and
injects a distinct withdrawn-memory block into the model prompt. The
`memory_search` tool also prepends active peer retractions to live dialectic
answers so semantically equivalent re-derived facts are treated as withdrawn.

## Live verification (2026-07-05)

A live probe against Honcho v3.0.11 refuted the Honcho-reliant part of the
original decision:

- The conclusion-create schema has no `level` field. A Jarvis
  `level=contradiction` create is stored by Honcho as `explicit`; the
  contradiction level survives only in Jarvis's local metadata sidecar.
- The dreamer re-mints the forgotten derived fact from still-present source
  messages, because source messages are not deletable in Honcho.
- A dialectic query about the forgotten fact asserted it as current and did not
  retrieve the local retraction row.

Therefore the retraction is primarily a Jarvis-side suppression signal. Jarvis
may still write the contradiction-shaped conclusion to Honcho for audit, but
correctness depends on the local active-retraction index and answer-time prompt
rails, not on Honcho reasoning or conclusion levels.

## Consequences

- Declared facts keep the existing plain-delete behavior.
- Derived facts get a local semantic guard that survives Honcho re-deriving a
  new id.
- Ambient memory assembly reads active retractions from a local file only; it
  must not add representation, dialectic, or other network calls to the
  wake-to-TTS hot path.
- Memory rendering must treat local active retractions as authoritative over
  derived restatements of the same fact, including different wording.
- Corrections that re-assert a previously withdrawn fact clear the matching
  local retraction so the corrected value is not wrongly suppressed.
