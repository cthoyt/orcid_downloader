# -*- coding: utf-8 -*-

"""Download and process ORCID in bulk."""

from __future__ import annotations

import gzip
import json
import logging
import tarfile
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, NamedTuple

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


class VersionInfo(NamedTuple):
    version: str
    url: str
    fname: str
    size: int


#: See https://orcid.figshare.com/articles/dataset/ORCID_Public_Data_File_2023/24204912/1
#: and download the "summaries" file. Skip all the activities files
VERSION_2023 = VersionInfo(
    version="2023",
    url="https://orcid.figshare.com/ndownloader/files/42479943",
    fname="ORCID_2023_10_summaries.tar.gz",
    size=18_600_000,
)

NAMESPACES = {
    "personal-details": "http://www.orcid.org/ns/personal-details",
    "common": "http://www.orcid.org/ns/common",
    "other-name": "http://www.orcid.org/ns/other-name",
    "employment": "http://www.orcid.org/ns/employment",
    "activities": "http://www.orcid.org/ns/activities",
    "education": "http://www.orcid.org/ns/education",
    "external-identifier": "http://www.orcid.org/ns/external-identifier",
    "researcher-url": "http://www.orcid.org/ns/researcher-url",
}
MODULE = pystow.module("orcid", VERSION_2023.version)
RECORDS_PATH = MODULE.join(name="records.json.gz")
RECORDS_HQ_PATH = MODULE.join(name="records_hq.json.gz")
TEST_RECORDS_PATH = MODULE.join(name="test_records.json")
TEST_RECORDS_HQ_PATH = MODULE.join(name="test_records_hq.json")
GILDA_PATH = MODULE.join(name="gilda.tsv.gz")


def _norm_key(id_type):
    return id_type.lower().replace(" ", "").rstrip(":")


EXTERNAL_ID_SKIP = {
    "iAuthor": "Reuses orcid",
    "ORCID": "redundant",
    "ORCID id": "redundant",
    "eScientist": "Reuses orcid",
    "UNE Researcher ID": "not an id",
    "UOW Scholars": "not an id",
    "US EPA VIVO": "not an id",
    "Chalmers ID": "not an id",
}
EXTERNAL_ID_SKIP = {_norm_key(k): v for k, v in EXTERNAL_ID_SKIP.items()}
#: Mapping from ORCID keys to Bioregistry prefixes for external IDs
EXTERNAL_ID_MAPPING = {
    "ResearcherID": "wos.researcher",
    "Web of Science Researcher ID": "wos.researcher",
    "Scopus Author ID": "scopus",
    "Scopus ID": "scopus",
    "ID de autor de Scopus": "scopus",
    "???person.personsources.scopusauthor???": "scopus",
    "Loop profile": "loop",  # TODO add to bioregistry
    "github": "github",
    "ISNI": "isni",
    "Google Scholar": "google.scholar",
    "gnd": "gnd",
    "Digital Author ID": "dai",  # TODO add to bioregistry
    "Digital Author ID (DAI)": "dai",
    "dai": "dai",
    "AuthenticusID": "authenticus",  # TODO add to bioregistry
    "AuthID": "authenticus",
    "ID Dialnet": "dialnet",  # TODO add to bioregistry
    "Dialnet ID": "dialnet",
}
EXTERNAL_ID_MAPPING = {_norm_key(k): v for k, v in EXTERNAL_ID_MAPPING.items()}
UNMAPPED_EXTERNAL_ID: set[str] = set()


def ensure_summaries() -> Path:
    """Ensure the ORCID summaries file (32+ GB) is downloaded."""
    return MODULE.ensure(url=VERSION_2023.url, name=VERSION_2023.fname)


def get_records(*, force: bool = False, test: bool = False) -> dict:
    """Get lexicalizations for people in ORCID, takes about an hour."""
    records_path = TEST_RECORDS_PATH if test else RECORDS_PATH
    records_hq_path = TEST_RECORDS_HQ_PATH if test else RECORDS_HQ_PATH
    opener = open if test else gzip.open

    if not test and not force and records_path.is_file():
        logger.info("loading ORCID records from %s", records_path)
        with opener(records_path, "rt") as file:
            rv = json.load(file)
        logger.info("done loading ORCID records")
        return rv

    ror_grounder = get_ror_grounder()

    path = ensure_summaries()
    tar_file = tarfile.open(path)
    records = {}
    with tqdm(unit_scale=True, total=VERSION_2023.size) as pbar:
        while member := tar_file.next():
            pbar.update(1)
            if not member.name.endswith(".xml"):
                continue

            file = tar_file.extractfile(member)
            if not file:
                continue

            tree = etree.parse(file)  # noqa:S320

            orcid = tree.findtext(".//common:path", namespaces=NAMESPACES)
            if not orcid:
                continue

            if orcid in {
                "0000-0003-4423-4370",  # charlie
                "0000-0002-2579-9002",  # has a public email
            }:
                MODULE.join(name=f"{orcid}.xml").write_bytes(
                    etree.tostring(tree, pretty_print=True)
                )

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

            r: dict[str, Any] = dict(name=name.strip())
            aliases.update(_iter_other_names(tree))
            if aliases:
                r["aliases"] = sorted(aliases)

            employments = _get_employments(tree, grounder=ror_grounder)
            if employments:
                r["employments"] = employments

            educations = _get_educations(tree, grounder=ror_grounder)
            if educations:
                r["educations"] = educations

            ids = _get_external_identifiers(tree, orcid=orcid)
            if ids:
                r["xrefs"] = ids

            if works := _get_works(tree):
                r["works"] = works

            records[orcid] = r

            if test and pbar.n > 50:
                break

    tar_file.close()

    logger.info("writing ORCID records to %s", records_path)
    with opener(records_path, "wt") as file:
        json.dump(records, file, ensure_ascii=False, indent=2 if test else None)
    logger.info("done writing ORCID records")

    hq_records = {
        k: record
        for k, record in tqdm(
            records.items(), desc="Getting hq subset", unit_scale=True, unit="record"
        )
        if _is_hq(record)
    }
    logger.info("writing high quality ORCID records to %s", records_hq_path)
    with opener(records_hq_path, "wt") as file:
        json.dump(hq_records, file, ensure_ascii=False, indent=2 if test else None)
    logger.info("done writing high quality ORCID records")

    return records


def _iter_other_names(t) -> Iterable[str]:
    for part in t.findall(".//other-name:content", namespaces=NAMESPACES):
        part = part.text.strip()
        for z in part.split(";"):
            z = z.strip()
            if z is not None and " " in z and len(z) < 60:
                yield z.strip()


UNKNOWN_SOURCES = {}
LOWERCASE_THESE_SOURCES = {"RINGGOLD", "GRID", "LEI"}


def _get_external_identifiers(tree, orcid) -> dict[str, str]:
    rv = {}
    for i in tree.findall(
        ".//external-identifier:external-identifiers/external-identifier:external-identifier",
        namespaces=NAMESPACES,
    ):
        id_val = i.findtext(".//common:external-id-value", namespaces=NAMESPACES)
        id_type = i.findtext(".//common:external-id-type", namespaces=NAMESPACES)
        id_type_norm = _norm_key(id_type)
        if id_type_norm in EXTERNAL_ID_SKIP:
            continue

        id_type_mapped = EXTERNAL_ID_MAPPING.get(id_type_norm)
        if not id_type_mapped:
            if id_type not in UNMAPPED_EXTERNAL_ID:
                UNMAPPED_EXTERNAL_ID.add(id_type)
                id_url = i.findtext(".//common:external-id-url", namespaces=NAMESPACES)
                tqdm.write(
                    f"[{orcid}] missing external identifier key: {id_type} - {id_val} at {id_url}"
                )
            continue

        rv[id_type_mapped] = id_val

    for v in tree.findall(
        ".//researcher-url:researcher-urls/researcher-url:researcher-url", namespaces=NAMESPACES
    ):
        # name = v.findtext(".//researcher-url:url-name", namespaces=NAMESPACES)
        value = v.findtext(".//researcher-url:url", namespaces=NAMESPACES).rstrip("/")
        if value.startswith("https://github.com/"):
            svalue = value.removeprefix("https://github.com/")
            if "/" not in svalue:  # i.e., this is not a specific repo
                rv["github"] = svalue
        elif value.startswith("https://twitter.com/") or value.startswith("https://x.com/"):
            pass  # skip twitter, it's not reasonable to participate on this platform anymore
        elif value.startswith("https://www.wikidata.org/wiki/"):
            svalue = value.removeprefix("https://www.wikidata.org/wiki/")
            rv["wikidata"] = svalue
        elif value.startswith("https://tools.wmflabs.org/scholia/author/"):
            svalue = value.removeprefix("https://tools.wmflabs.org/scholia/author/")
            rv["wikidata"] = svalue
    return rv


def _get_works(tree) -> list[dict[str, str]]:
    # get a subset of all works with pubmed IDs. TODO extend to other IDs
    pmids = set()
    for g in tree.findall(
        ".//activities:works/activities:group/common:external-ids", namespaces=NAMESPACES
    ):
        if g.findtext(".//common:external-id-type", namespaces=NAMESPACES) == "pmid":
            value = g.findtext(".//common:external-id-value", namespaces=NAMESPACES)
            if value:
                pmids.add(value)
    return [{"pubmed": pmid} for pmid in sorted(pmids)]


def _get_employments(tree, grounder: "gilda.Grounder"):
    results = []
    for element in tree.findall(".//employment:employment-summary", namespaces=NAMESPACES):
        if element is None:
            continue

        organization_element = element.find(".//common:organization", namespaces=NAMESPACES)
        if organization_element is None:
            continue

        name = organization_element.findtext(".//common:name", namespaces=NAMESPACES)
        if not name:
            continue
        references = _get_disambiguated_organization(organization_element, name, grounder)
        record = dict(name=name.strip(), **references)

        start_year = element.findtext(".//common:start-date//common:year", namespaces=NAMESPACES)
        if start_year:
            record["start"] = start_year

        end_year = element.findtext(".//common:end-date//common:year", namespaces=NAMESPACES)
        if end_year:
            record["end"] = end_year

        if role := _get_role(element):
            record["role"] = role

        results.append(record)

    return results


#: Role text needs to be longer than this
MINIMUM_ROLE_LENGTH = 4
BSC_VALUES = {"bsc", "bs c", "bsc candidate", "bsc (honours)", "bsc (honors)"}
MSC_VALUES = {"msc", "ms c", "msc candidate", "msc (honours)", "msc (honors)"}
PHD_VALUES = {"phd", "ph d", "dphil", "phd candidate", "phd researcher"}


def _get_role(element) -> str | None:
    role = element.findtext(".//common:role-title", namespaces=NAMESPACES)
    if not role:
        return None
    role = role.strip()
    role_norm = role.replace(".", "").lower()
    if role_norm in BSC_VALUES or role_norm.startswith("bsc in ") or role_norm.startswith("bsc "):
        return "Bachelor of Science"
    if role_norm in MSC_VALUES or role_norm.startswith("msc in ") or role_norm.startswith("msc "):
        return "Master of Science"
    if role_norm.startswith("ma in ") or role_norm.startswith("ma "):
        return "Master of Arts"
    if (
        role_norm in PHD_VALUES
        or role_norm.startswith("phd in ")
        or role_norm.startswith("phd student in ")
    ):
        return "Doctor of Philosophy"
    if len(role) < MINIMUM_ROLE_LENGTH:
        return None
    return role


def _get_educations(tree, grounder: "gilda.Grounder"):
    results = []
    for element in tree.findall(
        ".//activities:educations//education:education-summary", namespaces=NAMESPACES
    ):
        if element is None:
            continue
        organization_element = element.find(".//common:organization", namespaces=NAMESPACES)
        if organization_element is None:
            continue

        name = organization_element.findtext(".//common:name", namespaces=NAMESPACES)
        if not name:
            continue
        references = _get_disambiguated_organization(organization_element, name, grounder)
        record = dict(name=name.strip(), **references)

        start_year = element.findtext(".//common:start-date//common:year", namespaces=NAMESPACES)
        if start_year:
            record["start"] = start_year

        end_year = element.findtext(".//common:end-date//common:year", namespaces=NAMESPACES)
        if end_year:
            record["end"] = end_year

        if role := _get_role(element):
            record["role"] = role

        results.append(record)
    return results


def _get_disambiguated_organization(organization_element, name, grounder) -> dict[str, str]:
    references = {}
    for de in organization_element.findall(
        ".//common:disambiguated-organization", namespaces=NAMESPACES
    ):
        source = de.findtext(".//common:disambiguation-source", namespaces=NAMESPACES)
        link = de.findtext(".//common:disambiguated-organization-identifier", namespaces=NAMESPACES)
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
    if "ror" not in references and (scored_match := grounder.ground_best(name)):
        references["ror"] = scored_match.term.id
    return references


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


@lru_cache(1)
def get_ror_grounder() -> "gilda.Grounder":
    """Get a grounder for ROR."""
    import pyobo.gilda_utils

    return pyobo.gilda_utils.get_grounder("ror")


def _is_hq(r) -> bool:
    # just see if there's literally anything in there
    return bool(
        any(x.get("ror") for x in r.get("employments", []))
        or any(x.get("ror") for x in r.get("educations", []))
        or r.get("works")
    )


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
    get_records(test=False, force=True)
    # print(*ground_researcher("CT Hoyt"), sep="\n")  # noqa:T201
