# Dreamer Tombstone Decision

Status: ACCEPTED 2026-07-05 — Option 1

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

## Consequences

- Declared facts keep the existing plain-delete behavior.
- Derived facts get a semantic guard that survives Honcho re-deriving a new id.
- Memory rendering must treat contradiction and retraction conclusions as
  authoritative over derived restatements of the same fact.
