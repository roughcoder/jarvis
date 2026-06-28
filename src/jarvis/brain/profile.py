"""Backward-compatible imports for user profile fact storage."""

from jarvis.users import forget_fact, format_facts, parse_facts, read_facts, remember_fact

__all__ = ["forget_fact", "format_facts", "parse_facts", "read_facts", "remember_fact"]
