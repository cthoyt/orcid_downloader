"""Utilities for processing name strings."""

from __future__ import annotations

from collections.abc import Iterable
from string import ascii_lowercase

__all__ = [
    "clean_name",
    "name_to_synonyms",
]


def clean_name(name: str) -> str:
    """Clean a name string.

    :param name: A full name
    :return: A cleaned full name, e.g., stripped of titles and suffixes
    """
    # strip titles like Dr. and DR. from beginning of all names/aliases
    # strip post-titles Francess Dufie Azumah (DR.)
    lower = name.lower()
    if lower.startswith("professor "):
        name = name[len("professor ") :].strip()
    if lower.startswith("prof."):
        name = name[len("prof.") :].strip()
    if lower.startswith("dr.-ing."):
        name = name[len("dr.-ing.") :]
    if lower.startswith("dr "):
        name = name[len("dr ") :]
    if lower.startswith("dr."):
        name = name[len("dr.") :].strip()

    for suffix in [
        "(dr)",
        "(dr.)",
        ", m.d.",
        ", phd",
        ", md",
        ", mph",
        ", ph.d.",
        ", ms",
    ]:
        if lower.endswith(suffix):
            name = name.removesuffix(suffix).strip()

    name = name.replace('"', "")
    name = name.strip("/")
    name = name.strip("\\")

    # Fix lazily written in all caps or lower
    if name == name.lower() or name == name.upper():
        name = name.title()

    name = _uncomma(name)

    return name


suffixes = {"phd", "md", "mph", "mba", "msc", "d. m. d.", "facs", "pharmd", "frsc", "rn", "edd"}
allowed_suffixes = {"jr", "iii", "ii", "sr"}
for letter in ascii_lowercase:
    allowed_suffixes.add(f"{letter}.")
    allowed_suffixes.add(f"{letter}")


def _uncomma(name: str) -> str:
    if "," not in name:
        return name

    # remove spaces before commas
    name = name.replace(" ,", ",")
    parts = [part.strip() for part in name.split()]
    if len(parts) < 2:
        return name

    if parts[0].endswith(","):
        name = " ".join(parts[1:]) + " " + parts[0].rstrip(",").strip()

    if len(parts) > 2 and parts[-2].endswith(","):
        # assume this has some kind of certification at the end
        ss = parts[-1].lower().replace(".", "").strip()
        if ss in suffixes:
            name = " ".join(parts[:-1]).rstrip(",").strip()
        elif ss not in allowed_suffixes:
            pass  # this is an unknown final part, maybe add some logging
    return name


def name_to_synonyms(name: str) -> Iterable[str]:
    """Create a synonym list from a full name.

    :param name: A person's name
    :yield: Variations on the name
    """
    # assume last part is the last name, this isn't always correct, but :shrug:
    # consider alternatives like https://pypi.org/project/nameparser/
    *givens, family = name.split()
    if not givens:
        return

    yield family + ", " + givens[0]
    yield family + ", " + " ".join(givens)

    if len(givens) > 1:
        first_given, *middle_givens = givens
        middle_given_initials = [g[0] for g in middle_givens]
        yield family + ", " + first_given + " " + " ".join(middle_given_initials)
        yield family + ", " + first_given + " " + "".join(f"{i}." for i in middle_given_initials)
        yield family + ", " + first_given + " " + " ".join(f"{i}." for i in middle_given_initials)

        yield first_given + " " + " ".join(middle_given_initials) + " " + family
        yield first_given + " " + "".join(f"{i}." for i in middle_given_initials) + " " + family
        yield first_given + " " + " ".join(f"{i}." for i in middle_given_initials) + " " + family

    firsts = [given[0] for given in givens]
    firsts_unspaced = "".join(firsts)
    firsts_spaced = " ".join(firsts)
    firsts_dotted = [f"{first}." for first in firsts]
    firsts_dotted_unspaced = "".join(firsts_dotted)
    firsts_dotted_spaced = " ".join(firsts_dotted)
    first_first = firsts[0]

    yield first_first + " " + family
    yield first_first + ". " + family

    yield firsts_unspaced + " " + family
    yield firsts_spaced + " " + family
    yield firsts_dotted_unspaced + " " + family
    yield firsts_dotted_spaced + " " + family

    yield family + " " + firsts_unspaced
    yield family + " " + firsts_dotted_unspaced
    yield family + " " + firsts_spaced
    yield family + " " + firsts_dotted_spaced

    yield family + ", " + firsts_unspaced
    yield family + ", " + firsts_dotted_unspaced
    yield family + ", " + firsts_spaced
    yield family + ", " + firsts_dotted_spaced

    yield family + " " + first_first
    yield family + " " + first_first + "."
    yield family + ", " + first_first + "."
    yield family + ", " + first_first


def reconcile_aliases(
    name: str | None,
    aliases: set[str],
    *,
    minimum_name_length: int = 1,
) -> tuple[str | None, set[str]]:
    """Reconcile aliases."""
    # TODO if there is a comma in the main name picked, try and find an alias with no commas
    aliases = {a for a in aliases if len(a) > minimum_name_length}
    if name is not None and len(name) <= minimum_name_length:
        name = None
    if name is None and aliases:
        name = max(aliases, key=len)
        aliases = {a for a in aliases if a != name}
    return name, aliases
