# -*- coding: utf-8 -*-

"""Download and process ORCID in bulk."""

import tarfile
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List, Tuple

import pystow
from lxml import etree  # noqa:S410
from tqdm.auto import tqdm

if TYPE_CHECKING:
    import gilda

__all__ = [
    "ensure_summaries",
    "get_lexicalizations",
    "ground",
]

URL_2023 = "https://orcid.figshare.com/ndownloader/files/42479787"
NAMESPACES = {
    "personal-details": "http://www.orcid.org/ns/personal-details",
    "common": "http://www.orcid.org/ns/common",
    "other-name": "http://www.orcid.org/ns/other-name",
}
VERSION = "2023"
MODULE = pystow.module("orcid", VERSION)
GILDA_PATH = MODULE.join(name="orcid_gilda.tsv.gz")

Lexicalization = Tuple[str, str, List[str]]


def ensure_summaries() -> Path:
    """Ensure the ORCID summaries file (32+ GB) is downloaded."""
    return MODULE.ensure(url=URL_2023, name="ORCID_2023_10_summaries.tar.gz")


def get_lexicalizations() -> List[Lexicalization]:
    """Get lexicalizations for people in ORCID, takes about an hour."""
    rows = []
    path = ensure_summaries()
    tar_file = tarfile.open(path)
    with tqdm(unit_scale=True, total=17_800_000) as pbar:
        while my_member := tar_file.next():
            file = tar_file.extractfile(my_member)
            if not file:
                continue

            tree = etree.parse(file)  # noqa:S320
            orcid = tree.findtext(".//common:path", namespaces=NAMESPACES)
            if not orcid:
                continue

            family_name = tree.findtext(".//personal-details:family-name", namespaces=NAMESPACES)
            given_names = tree.findtext(".//personal-details:given-names", namespaces=NAMESPACES)
            if family_name and given_names:
                name = f"{given_names.strip()} {family_name.strip()}"
                rows.append((orcid, name, sorted(set(_iter_other_names(tree)))))

            pbar.update(1)
    tar_file.close()
    return rows


def _iter_other_names(t) -> Iterable[str]:
    for part in t.findall(".//other-name:content", namespaces=NAMESPACES):
        part = part.text.strip()
        for z in part.split(";"):
            z = z.strip()
            if z and " " in z and len(z) < 60:
                yield z


def ground(name: str) -> List["gilda.ScoredMatch"]:
    """Ground a name based on ORCID names/aliases."""
    return get_gilda_grounder().ground(name)


@lru_cache(1)
def get_gilda_grounder() -> "gilda.Grounder":
    """Get a Gilda grounder from ORCID names/aliases."""
    from gilda import Grounder
    from gilda.term import dump_terms

    if GILDA_PATH.is_file():
        return Grounder(GILDA_PATH)

    lexicalizations = get_lexicalizations()
    terms = _lexicalizations_to_gilda_terms(lexicalizations)
    dump_terms(terms, GILDA_PATH)
    return Grounder(terms)


def _lexicalizations_to_gilda_terms(lexicalizations: List[Lexicalization]) -> List["gilda.Term"]:
    from gilda import Term
    from gilda.process import normalize

    terms = []
    for orcid, name, aliases in tqdm(lexicalizations, unit_scale=True, unit="person"):
        terms.append(
            Term(
                norm_text=normalize(name),
                text=name,
                db="orcid",
                id=orcid,
                entry_name=name,
                status="name",
                source="orcid",
            )
        )
        for alias in aliases:
            if not alias:
                continue
            terms.append(
                Term(
                    norm_text=normalize(alias),
                    text=alias,
                    db="orcid",
                    id=orcid,
                    entry_name=name,
                    status="synonym",
                    source="orcid",
                )
            )
    return terms
