"""Utilities for ROR."""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

import gilda
import pyobo
import ssslm

if TYPE_CHECKING:
    import gilda.grounder

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
    ) -> list[gilda.grounder.ScoredMatch]:
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
def get_ror_grounder(version: str | None = None) -> ssslm.Grounder:
    """Get a grounder for ROR."""
    return pyobo.get_grounder("ror", grounder_cls=RORGrounder, force_process=True, version=version)
