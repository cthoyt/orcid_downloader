# -*- coding: utf-8 -*-

"""Download and process ORCID in bulk."""

from __future__ import annotations

import gzip
import json
import tarfile
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

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
        with gzip.open("rt") as file:
            return json.load(file)

    path = ensure_summaries()
    tar_file = tarfile.open(path)
    records = {}
    with tqdm(unit_scale=True, total=17_700_000) as pbar:
        while my_member := tar_file.next():
            file = tar_file.extractfile(my_member)
            if not file:
                continue

            tree = etree.parse(file)
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

            aliases = set()
            if not credit_name:
                name = label_name
            else:
                name = credit_name
                if label_name:
                    aliases.add(label_name)

            r = dict(name=name)
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

    with gzip.open(RECORDS_PATH, "wt") as file:
        json.dump(records, file)

    return records


def _iter_other_names(t) -> Iterable[str]:
    for part in t.findall(".//other-name:content", namespaces=NAMESPACES):
        part = part.text.strip()
        for z in part.split(";"):
            z = z.strip()
            if z and " " in z and len(z) < 60:
                yield z


UNKNOWN_SOURCES = {}


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
            elif source == "RINGGOLD":
                references[source.lower()] = link
            elif source == "GRID":
                references[source.lower()] = link
            elif source == "FUNDREF":
                references["funderregistry"] = link.removeprefix("http://dx.doi.org/10.13039/")
            elif source == "LEI":
                references["lei"] = link
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
    for x in tree.findall(
        ".//activities:educations//education:education-summary", namespaces=NAMESPACES
    ):
        # TODO use ROR to ground these
        name = x.findtext(".//common:organization//common:name", namespaces=NAMESPACES)
        if not name:
            continue

        record = dict(name=name)
        start_year = x.findtext(".//common:start-date//common:year", namespaces=NAMESPACES)
        if start_year:
            record["start"] = start_year

        end_year = x.findtext(".//common:end-date//common:year", namespaces=NAMESPACES)
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
    terms = _records_to_gilda_terms(records)
    dump_terms(terms, GILDA_PATH)
    return Grounder(terms)


def _records_to_gilda_terms(records: dict) -> list["gilda.Term"]:
    from gilda import Term
    from gilda.process import normalize

    terms = []
    for orcid, data in tqdm(records.items(), unit_scale=True, unit="person"):
        name = data["name"]
        norm_name = normalize(name)
        if norm_name:
            terms.append(
                Term(
                    norm_text=norm_name,
                    text=name,
                    db="orcid",
                    id=orcid,
                    entry_name=name,
                    status="name",
                    source="orcid",
                )
            )
        for alias in data.get("aliases", []):
            if not alias:
                continue
            norm_alias = normalize(alias)
            if norm_alias:
                terms.append(
                    Term(
                        norm_text=norm_alias,
                        text=alias,
                        db="orcid",
                        id=orcid,
                        entry_name=name,
                        status="synonym",
                        source="orcid",
                    )
                )
    return terms
