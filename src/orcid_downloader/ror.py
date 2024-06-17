"""Utilities for ROR."""

from __future__ import annotations

from functools import lru_cache

import gilda
import pyobo.gilda_utils

__all__ = [
    "get_ror_grounder",
    "RORGrounder",
]


class RORGrounder(gilda.Grounder):
    """A grounder for organizations based on ROR."""

    def ground(
        self,
        raw_str,
        context: str | None = None,
        organisms: list[str] | None = None,
        namespaces: list[str] | None = None,
    ):
        """Ground an organization, and fallback with optional preprocessing."""
        if scored_matches := super().ground(
            raw_str,
            context=context,
            organisms=organisms,
            namespaces=namespaces,
        ):
            return scored_matches

        norm_str = raw_str.removeprefix("The ").replace(",", "")
        return super().ground(
            norm_str,
            context=context,
            organisms=organisms,
            namespaces=namespaces,
        )


@lru_cache(1)
def get_ror_grounder() -> gilda.Grounder:
    """Get a grounder for ROR."""
    return pyobo.gilda_utils.get_grounder("ror", grounder_cls=RORGrounder)
