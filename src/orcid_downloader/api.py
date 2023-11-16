# -*- coding: utf-8 -*-

"""Download and process ORCID in bulk."""

from __future__ import annotations

import gzip
import json
import logging
import tarfile
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

import pystow
from lxml import etree  # noqa:S410
from tqdm.auto import tqdm

if TYPE_CHECKING:
    import gilda

__all__ = [
    "ensure_summaries",
    "get_records",
    "ground_researcher",
    "get_gilda_grounder",
]

logger = logging.getLogger(__name__)

URL_2023 = "https://orcid.figshare.com/ndownloader/files/42479787"
NAMESPACES = {
    "personal-details": "http://www.orcid.org/ns/personal-details",
    "common": "http://www.orcid.org/ns/common",
    "other-name": "http://www.orcid.org/ns/other-name",
    "employment": "http://www.orcid.org/ns/employment",
    "activities": "http://www.orcid.org/ns/activities",
    "education": "http://www.orcid.org/ns/education",
}
VERSION = "2023"
MODULE = pystow.module("orcid", VERSION)
RECORDS_PATH = MODULE.join(name="records.json.gz")
GILDA_PATH = MODULE.join(name="gilda.tsv.gz")


def ensure_summaries() -> Path:
    """Ensure the ORCID summaries file (32+ GB) is downloaded."""
    return MODULE.ensure(url=URL_2023, name="ORCID_2023_10_summaries.tar.gz")


# TODO make high quality subset of ORCID for people who actually have something in their profile


def get_records() -> dict:
    """Get lexicalizations for people in ORCID, takes about an hour."""
    if RECORDS_PATH.is_file():
        logger.info("loading ORCID records from %s", RECORDS_PATH)
        with gzip.open(RECORDS_PATH, "rt") as file:
            rv = json.load(file)
        logger.info("done loading ORCID records")
        return rv

    path = ensure_summaries()
    tar_file = tarfile.open(path)
    records = {}
    with tqdm(unit_scale=True, total=17_700_000) as pbar:
        while member := tar_file.next():
            file = tar_file.extractfile(member)
            if not file:
                continue

            tree = etree.parse(file)  # noqa:S320
            # print(etree.tostring(tree, pretty_print=True).decode("utf8"))

            orcid = tree.findtext(".//common:path", namespaces=NAMESPACES)
            if not orcid:
                continue

            family_name = tree.findtext(".//personal-details:family-name", namespaces=NAMESPACES)
            given_names = tree.findtext(".//personal-details:given-names", namespaces=NAMESPACES)
            if family_name and given_names:
                label_name = f"{given_names.strip()} {family_name.strip()}"
            else:
                label_name = None

            credit_name = tree.findtext(".//personal-details:credit-name", namespaces=NAMESPACES)
            if credit_name:
                credit_name = credit_name.strip()

            if not credit_name and not label_name:
                # Skip records that don't have any kinds of labels
                continue

            aliases: set[str] = set()
            if not credit_name:
                name = label_name
            else:
                name = credit_name
                if label_name is not None:
                    aliases.add(label_name)

            r: dict[str, Any] = dict(name=name)
            aliases.update(_iter_other_names(tree))
            if aliases:
                r["aliases"] = sorted(aliases)

            employments = _get_employments(tree)
            if employments:
                r["employments"] = employments

            educations = _get_educations(tree)
            if educations:
                r["educations"] = educations

            records[orcid] = r
            pbar.update(1)

    tar_file.close()

    logger.info("writing ORCID records to %s", RECORDS_PATH)
    with gzip.open(RECORDS_PATH, "wt") as file:
        json.dump(records, file)
    logger.info("done ORCID writing records")

    return records


def _iter_other_names(t) -> Iterable[str]:
    for part in t.findall(".//other-name:content", namespaces=NAMESPACES):
        part = part.text.strip()
        for z in part.split(";"):
            z = z.strip()
            if z is not None and " " in z and len(z) < 60:
                yield z


UNKNOWN_SOURCES = {}
LOWERCASE_THESE_SOURCES = {"RINGGOLD", "GRID", "LEI"}


def _get_employments(tree):
    results = []
    for summary_element in tree.findall(".//employment:employment-summary", namespaces=NAMESPACES):
        if summary_element is None:
            continue

        element = summary_element.find(".//common:organization", namespaces=NAMESPACES)
        if element is None:
            continue

        name = element.findtext(".//common:name", namespaces=NAMESPACES)
        references = {}
        for de in element.findall(".//common:disambiguated-organization", namespaces=NAMESPACES):
            source = de.findtext(".//common:disambiguation-source", namespaces=NAMESPACES)
            link = de.findtext(
                ".//common:disambiguated-organization-identifier", namespaces=NAMESPACES
            )
            if not link:
                continue
            link = link.strip()
            if source == "ROR":
                references["ror"] = link.removeprefix("https://ror.org/")
            elif source in LOWERCASE_THESE_SOURCES:
                references[source.lower()] = link
            elif source == "FUNDREF":
                references["funderregistry"] = link.removeprefix("http://dx.doi.org/10.13039/")
            elif source not in UNKNOWN_SOURCES:
                tqdm.write(f"unhandled source: {source} / link: {link}")
                UNKNOWN_SOURCES[source] = link

        record = dict(name=name, **references)
        start_year = summary_element.findtext(
            ".//common:start-date//common:year", namespaces=NAMESPACES
        )
        if start_year:
            record["start"] = start_year

        end_year = summary_element.findtext(
            ".//common:end-date//common:year", namespaces=NAMESPACES
        )
        if end_year:
            record["end"] = end_year

        results.append(record)

    return results


def _get_educations(tree):
    results = []
    for element in tree.findall(
        ".//activities:educations//education:education-summary", namespaces=NAMESPACES
    ):
        # TODO use ROR to ground these
        name = element.findtext(".//common:organization//common:name", namespaces=NAMESPACES)
        if not name:
            continue

        record = dict(name=name)
        start_year = element.findtext(".//common:start-date//common:year", namespaces=NAMESPACES)
        if start_year:
            record["start"] = start_year

        end_year = element.findtext(".//common:end-date//common:year", namespaces=NAMESPACES)
        if end_year:
            record["end"] = end_year

        results.append(record)
    return results


def ground_researcher(name: str) -> list["gilda.ScoredMatch"]:
    """Ground a name based on ORCID names/aliases."""
    return get_gilda_grounder().ground(name)


@lru_cache(1)
def get_gilda_grounder() -> "gilda.Grounder":
    """Get a Gilda grounder from ORCID names/aliases."""
    from gilda import Grounder
    from gilda.term import dump_terms

    if GILDA_PATH.is_file():
        return Grounder(GILDA_PATH)

    records = get_records()
    terms = list(_records_to_gilda_terms(records))
    # we don't need to filter duplicates globally
    dump_terms(terms, GILDA_PATH)
    return Grounder(terms)


def _records_to_gilda_terms(records: dict) -> Iterable["gilda.Term"]:
    from gilda import Term
    from gilda.process import normalize

    for orcid, data in tqdm(records.items(), unit_scale=True, unit="person", desc="Indexing"):
        name = data["name"]
        norm_name = normalize(name)
        if norm_name:
            yield Term(
                norm_text=norm_name,
                text=name,
                db="orcid",
                id=orcid,
                entry_name=name,
                status="name",
                source="orcid",
            )
        aliases = set(data.get("aliases", []))
        aliases.update(name_to_synonyms(name))
        aliases -= {name}
        for alias in aliases:
            if not alias:
                continue
            norm_alias = normalize(alias)
            if norm_alias:
                yield Term(
                    norm_text=norm_alias,
                    text=alias,
                    db="orcid",
                    id=orcid,
                    entry_name=name,
                    status="synonym",
                    source="orcid",
                )


def name_to_synonyms(name: str) -> Iterable[str]:
    """Create a synonym list from a full name."""
    # assume last part is the last name, this isn't always correct, but :shrug:
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


if __name__ == "__main__":
    print(*ground_researcher("CT Hoyt"), sep="\n")  # noqa:T201
