"""Utilities for ROR."""

from __future__ import annotations

from functools import lru_cache

import gilda
import pyobo
import ssslm

__all__ = [
    "RORGrounder",
    "get_ror_grounder",
]


class RORGrounder(gilda.Grounder):
    """A grounder for organizations based on ROR."""

    def ground(
        self,
        text: str,
        context: str | None = None,
        organisms: list[str] | None = None,
        namespaces: list[str] | None = None,
    ):
        """Ground an organization, and fallback with optional preprocessing."""
        if scored_matches := super().ground(
            text,
            context=context,
            organisms=organisms,
            namespaces=namespaces,
        ):
            return scored_matches

        norm_str = text.removeprefix("The ").replace(",", "")
        return super().ground(
            norm_str,
            context=context,
            organisms=organisms,
            namespaces=namespaces,
        )


@lru_cache(1)
def get_ror_grounder() -> ssslm.Grounder:
    """Get a grounder for ROR."""
    return pyobo.get_grounder("ror", grounder_cls=RORGrounder, progress=False)
