"""Download and process ORCID in bulk."""

from __future__ import annotations

import csv
import gzip
import json
import logging
import tarfile
import typing
from collections import Counter
from functools import lru_cache, partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, NamedTuple
from urllib.parse import parse_qs, urlparse

import pystow
from lxml import etree
from pydantic import BaseModel, Field
from tabulate import tabulate
from tqdm.auto import tqdm

from orcid_downloader.standardize import standardize_role

if TYPE_CHECKING:
    import gilda

__all__ = [
    "ensure_summaries",
    "iter_records",
    "get_records",
    "ground_researcher",
    "get_gilda_grounder",
    "Record",
]

logger = logging.getLogger(__name__)


class VersionInfo(NamedTuple):
    """A tuple containing information for downloading ORCID data dumps."""

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
    "email": "http://www.orcid.org/ns/email",
}
MODULE = pystow.module("orcid", VERSION_2023.version)
RECORDS_PATH = MODULE.join(name="records.jsonl.gz")
RECORDS_HQ_PATH = MODULE.join(name="records_hq.jsonl.gz")
SCHEMA_PATH = MODULE.join(name="schema.json")
GILDA_PATH = MODULE.join(name="gilda.tsv.gz")
GILDA_HQ_PATH = MODULE.join(name="gilda_hq.tsv.gz")

URL_NAMES_PATH = MODULE.join(name="url_names.tsv")
EMAIL_PATH = MODULE.join(name="email.tsv")
PUBMEDS_PATH = MODULE.join(name="pubmeds.tsv.gz")

xrefs_folder = MODULE.module("xrefs")
GITHUBS_PATH = xrefs_folder.join(name="github.tsv")
XREFS_SUMMARY_PATH = xrefs_folder.join(name="README.md")
SSSOM_PATH = xrefs_folder.join(name="sssom.tsv.gz")


ROLES = MODULE.module("roles")
AFFILIATION_XREFS_SUMMARY_PATH = ROLES.join(name="affiliation_xref_summary.tsv")
EDUCATION_ROLE_SUMMARY_PATH = ROLES.join(name="education_role_summary.tsv.gz")
EDUCATION_ROLE_UNSTANDARDIZED_SUMMARY_PATH = ROLES.join(
    name="education_role_unstandardized_summary.tsv"
)
EMPLOYMENT_ROLE_SUMMARY_PATH = ROLES.join(name="employment_role_summary.tsv.gz")


def _norm_key(id_type):
    return id_type.lower().replace(" ", "").rstrip(":")


EXTERNAL_ID_SKIP = {
    "iAuthor": "Reuses orcid",
    "JRIN": "Reuses orcid",
    "ORCID": "redundant",
    "ORCID id": "redundant",
    "eScientist": "Reuses orcid",
    "UNE Researcher ID": "not an id",
    "UOW Scholars": "not an id",
    "US EPA VIVO": "not an id",
    "Chalmers ID": "not an id",
    "HKUST Profile": "not an id",
    "Custom": "garb",
    "Profile system identifier": "garb",
    "CTI Vitae": "dead website",
}
EXTERNAL_ID_SKIP = {_norm_key(k): v for k, v in EXTERNAL_ID_SKIP.items()}
#: Mapping from ORCID keys to Bioregistry prefixes for external IDs
EXTERNAL_ID_MAPPING = {
    "ResearcherID": "wos.researcher",
    "RID": "wos.researcher",
    "Web of Science Researcher ID": "wos.researcher",
    "other-id - Web of Science": "wos.researcher",
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
    "Authenticus": "authenticus",  # TODO add to bioregistry
    "AuthenticusID": "authenticus",
    "AuthID": "authenticus",
    "ID Dialnet": "dialnet",  # TODO add to bioregistry
    "Dialnet ID": "dialnet",
    "SciProfiles": "sciprofiles",  # TODO add to bioregistry https://sciprofiles.com/profile/$1
    "Sciprofile": "sciprofiles",
    "Ciência ID": "ciencia",  # TODO add to bioregistry http://www.cienciavitae.pt/$1
    "KAKEN": "kaken",  # TODO add to bioregistry https://nrid.nii.ac.jp/nrid/1000050211371
}
EXTERNAL_ID_MAPPING = {_norm_key(k): v for k, v in EXTERNAL_ID_MAPPING.items()}
UNMAPPED_EXTERNAL_ID: set[str] = set()


def ensure_summaries() -> Path:
    """Ensure the ORCID summaries file (32+ GB) is downloaded."""
    return MODULE.ensure(url=VERSION_2023.url, name=VERSION_2023.fname)


class Work(BaseModel):
    """A model representing a creative work."""

    pubmed: str = Field(..., title="PubMed identifier")


class Affiliation(BaseModel):
    """A model representing an affiliation (either education or employment)."""

    name: str
    start: int | None = Field(None, title="Start Year")
    end: int | None = Field(None, title="End Year")
    role: str | None = None
    xrefs: dict[str, str] = Field(default_factory=dict, title="Database Cross-references")
    # xrefs includes ror, ringgold, grid, funderregistry, lei


class Record(BaseModel):
    """A model representing a person."""

    orcid: str = Field(..., title="ORCID")
    name: str
    aliases: list[str] = Field(default_factory=list)
    xrefs: dict[str, str] = Field(default_factory=dict, title="Database Cross-references")
    works: list[Work] = Field(default_factory=list)
    employments: list[Affiliation] = Field(default_factory=list)
    educations: list[Affiliation] = Field(default_factory=list)
    emails: list[str] = Field(default_factory=list)


def _iter_tarfile_members(path: Path):
    tar_file = tarfile.open(path)
    while member := tar_file.next():
        if not member.name.endswith(".xml"):
            continue
        yield tar_file.extractfile(member)
    tar_file.close()


def iter_records(*, force: bool = False) -> Iterable[Record]:
    """Parse ORCID summary XML files, takes about an hour."""
    if not force and RECORDS_PATH.is_file():
        tqdm.write(f"reading cached records from {RECORDS_PATH}")
        with gzip.open(RECORDS_PATH, "rt") as file:
            for line in tqdm(
                file, unit_scale=True, unit="line", desc="Loading ORCID", total=VERSION_2023.size
            ):
                yield Record.model_validate_json(line)

    else:
        ror_grounder = get_ror_grounder()
        f = partial(_process_file, ror_grounder=ror_grounder)

        path = ensure_summaries()
        it = _iter_tarfile_members(path)
        # TODO use process_map with chunksize=50_000

        with (
            gzip.open(RECORDS_PATH, "wt") as records_file,
            gzip.open(RECORDS_HQ_PATH, "wt") as records_hq_file,
        ):
            for file in tqdm(it, unit_scale=True, unit="record", total=VERSION_2023.size):
                record: Record | None = f(file)
                if record is None:
                    continue
                line = record.model_dump_json(exclude_defaults=True, indent=None) + "\n"
                records_file.write(line)
                if _is_hq(record):
                    records_hq_file.write(line)
                yield record

        with URL_NAMES_PATH.open("w") as file:
            writer = csv.writer(file, delimiter="\t")
            writer.writerow(("norm_name", "name", "count", "example"))
            writer.writerows(
                (norm_name, UNKNOWN_NAMES_FULL[norm_name], count, UNKNOWN_NAMES_EXAMPLES[norm_name])
                for norm_name, count in UNKNOWN_NAMES.most_common()
            )


def get_records(*, force: bool = False) -> dict[str, Record]:
    """Parse ORCID summary XML files, takes about an hour."""
    return {record.orcid: record for record in iter_records(force=force)}


def _process_file(file, ror_grounder: gilda.Grounder) -> Record | None:  # noqa:C901
    tree = etree.parse(file)  # noqa:S320

    orcid = tree.findtext(".//common:path", namespaces=NAMESPACES)
    if not orcid:
        return None

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
        return None

    aliases: set[str] = set()
    if not credit_name:
        name = label_name
    else:
        name = credit_name
        if label_name is not None:
            aliases.add(label_name)

    record: dict[str, Any] = dict(orcid=orcid, name=name)
    aliases.update(_iter_other_names(tree))
    if aliases:
        record["aliases"] = sorted(aliases)

    employments = _get_employments(tree, grounder=ror_grounder)
    if employments:
        record["employments"] = employments

    educations = _get_educations(tree, grounder=ror_grounder)
    if educations:
        record["educations"] = educations

    ids = _get_external_identifiers(tree, orcid=orcid)
    if ids:
        record["xrefs"] = ids

    if works := _get_works(tree, orcid=orcid):
        record["works"] = works

    if emails := _get_emails(tree):
        record["emails"] = emails

    return Record.parse_obj(record)


def _iter_other_names(t) -> Iterable[str]:
    for part in t.findall(".//other-name:content", namespaces=NAMESPACES):
        part = part.text.strip()
        for z in part.split(";"):
            z = z.strip()
            if z is not None and " " in z and len(z) < 60:
                yield z.strip()


UNKNOWN_SOURCES = {}
LOWERCASE_THESE_SOURCES = {"RINGGOLD", "GRID", "LEI"}
UNKNOWN_NAMES: typing.Counter[str] = Counter()
UNKNOWN_NAMES_EXAMPLES: dict[str, str] = {}
UNKNOWN_NAMES_FULL: dict[str, str] = {}


def _get_external_identifiers(tree, orcid) -> dict[str, str]:  # noqa:C901
    rv = {}
    for i in tree.findall(
        ".//external-identifier:external-identifiers/external-identifier:external-identifier",
        namespaces=NAMESPACES,
    ):
        id_val = i.findtext(".//common:external-id-value", namespaces=NAMESPACES)
        if not id_val:
            continue
        id_type = i.findtext(".//common:external-id-type", namespaces=NAMESPACES)
        id_type_norm = _norm_key(id_type)
        if id_type_norm in EXTERNAL_ID_SKIP:
            continue

        id_type_mapped = EXTERNAL_ID_MAPPING.get(id_type_norm)
        if not id_type_mapped:
            if id_type not in UNMAPPED_EXTERNAL_ID:
                UNMAPPED_EXTERNAL_ID.add(id_type)
                id_url = i.findtext(".//common:external-id-url", namespaces=NAMESPACES)
                tqdm.write(f"[{orcid}] unknown id '{id_type}' w/ val '{id_val}' at {id_url}")
            continue

        rv[id_type_mapped] = id_val

    for element in tree.findall(
        ".//researcher-url:researcher-urls/researcher-url:researcher-url", namespaces=NAMESPACES
    ):
        url = element.findtext(".//researcher-url:url", namespaces=NAMESPACES).rstrip("/")
        url = url.removeprefix("https://")
        url = url.removeprefix("http://")
        if url.startswith("github.com/"):
            identifier = url.removeprefix("github.com/")
            if "/" not in identifier:  # i.e., this is not a specific repo
                rv["github"] = identifier
        elif url.startswith("twitter.com/") or url.startswith("x.com/"):
            pass  # skip twitter, it's not reasonable to participate on this platform anymore
        elif "facebook" in url or "instagram" in url:
            continue  # skip social media
        elif url.startswith("www.wikidata.org/wiki/"):
            identifier = url.removeprefix("www.wikidata.org/wiki/")
            rv["wikidata"] = identifier
        elif url.startswith("tools.wmflabs.org/scholia/author/"):
            identifier = url.removeprefix("tools.wmflabs.org/scholia/author/")
            rv["wikidata"] = identifier
        elif "linkedin.com/in/" in url:  # multiple languages subdomains, so startswith doesn't work
            identifier = url.rstrip("/").split("linkedin.com/in")[1]
            rv["linkedin"] = identifier
        elif "scholar.google" in url:
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            identifier = query_params.get("user", [None])[0]
            if identifier:
                rv["google.scholar"] = identifier
        elif url.startswith("publons.com/author/"):
            identifier = url.removeprefix("publons.com/author/").split("/")[0]
            rv["publons"] = identifier
        elif url.startswith("www.researchgate.net/profile/"):
            identifier = url.removeprefix("www.researchgate.net/profile/")
            rv["researchgate"] = identifier
        elif url.startswith("www.scopus.com/authid/detail.uri?authorId="):
            identifier = url.removeprefix("www.scopus.com/authid/detail.uri?authorId=")
            rv["scopus"] = identifier
        elif url.startswith("www.webofscience.com/wos/author/record/"):
            identifier = url.removeprefix("www.webofscience.com/wos/author/record/")
            rv["wos.researcher"] = identifier
        else:
            name = element.findtext(".//researcher-url:url-name", namespaces=NAMESPACES)
            if name:
                norm_name = _norm_key(name)
                UNKNOWN_NAMES[norm_name] += 1
                UNKNOWN_NAMES_FULL[norm_name] = name
                UNKNOWN_NAMES_EXAMPLES[norm_name] = url

    return rv


def _get_emails(tree) -> list[str]:
    return [
        email.text.strip()
        for email in tree.findall(".//email:emails/email:email/email:email", namespaces=NAMESPACES)
    ]


def _get_works(tree, orcid) -> list[dict[str, str]]:
    # get a subset of all works with pubmed IDs. TODO extend to other IDs
    pmids = set()
    for g in tree.findall(
        ".//activities:works/activities:group/common:external-ids", namespaces=NAMESPACES
    ):
        if g.findtext(".//common:external-id-type", namespaces=NAMESPACES) == "pmid":
            value: str | None = g.findtext(".//common:external-id-value", namespaces=NAMESPACES)
            if not value:
                continue
            value_std = _standardize_pubmed(value)
            if not value_std:
                continue
            if not value_std.isnumeric():
                tqdm.write(f"[{orcid}] unstandardized PubMed: '{value}'")
                continue
            pmids.add(value_std)
    return [{"pubmed": pmid} for pmid in sorted(pmids)]


PUBMED_PREFIXES = [
    "http://www.ncbi.nlm.nih.gov/pubmed/",
    "https://www.ncbi.nlm.nih.gov/pubmed/",
    "https://www-ncbi-nlm-nih-gov.proxy.bib.ucl.ac.be:2443/pubmed/",
    "http://europepmc.org/abstract/med/",
    "https://pubmed.ncbi.nlm.nih.gov/",
    "www.ncbi.nlm.nih.gov/pubmed/",
    "PMID: ",
    "PMID:",
    "PubMed PMID: ",
    "MEDLINE:",
    "[PMID: ",
    "PubMed:",
    "PubMed ID: ",
    "ncbi.nlm.nih.gov/pubmed/",
    "PMid:",
    "PubMed ",
    "PMID",
    "PMID ",
]


def _standardize_pubmed(pubmed: str) -> str | None:
    """Standardize a pubmed field.

    :param pubmed: A string that might somehow represent a pubmed identifier
    :returns: A cleaned pubmed identifier, if possible

    2023 statistics:

    - correct: 3,175,196 (99.85%)
    - needs processing: 2,832 (0.09%)
    - junk: 2,080 (0.07%)

    what was in here? a mashup of:

    - DOIs
    - PMC identifiers,
    - a few stray strings that contain a combination of pubmed, PMC,
    - a lot with random text (keywords)
    - some with full text citations
    """
    pubmed = pubmed.strip().strip(".").rstrip("/").strip()
    if pubmed.isnumeric():
        return pubmed
    for x in PUBMED_PREFIXES:
        if pubmed.startswith(x):
            parts = pubmed.removeprefix(x).strip().split()
            if parts:
                return parts[0]
    if pubmed.endswith("E7"):
        pubmed = str(int(float(pubmed)))
        return pubmed
    return None


def _get_employments(tree, grounder: gilda.Grounder):
    elements = tree.findall(".//employment:employment-summary", namespaces=NAMESPACES)
    return _get_affiliations(elements, grounder)


def _get_educations(tree, grounder: gilda.Grounder):
    elements = tree.findall(
        ".//activities:educations//education:education-summary", namespaces=NAMESPACES
    )
    return _get_affiliations(elements, grounder)


def _get_affiliations(elements, grounder: gilda.Grounder):
    results = []
    for element in elements:
        if element is None:
            continue
        organization_element = element.find(".//common:organization", namespaces=NAMESPACES)
        if organization_element is None:
            continue

        name = organization_element.findtext(".//common:name", namespaces=NAMESPACES)
        if not name:
            continue
        references = _get_disambiguated_organization(organization_element, name, grounder)
        record = dict(name=name.strip(), xrefs=references)

        start_year = element.findtext(".//common:start-date//common:year", namespaces=NAMESPACES)
        if start_year:
            record["start"] = int(start_year)

        end_year = element.findtext(".//common:end-date//common:year", namespaces=NAMESPACES)
        if end_year:
            record["end"] = int(end_year)

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


#: Role text needs to be longer than this
MINIMUM_ROLE_LENGTH = 4


def _get_role(element) -> str | None:
    role = element.findtext(".//common:role-title", namespaces=NAMESPACES)
    if not role:
        return None
    role, _ = standardize_role(role)
    if len(role) < MINIMUM_ROLE_LENGTH:
        return None
    return role


def ground_researcher(name: str, *, hq_filter: bool = False) -> list[gilda.ScoredMatch]:
    """Ground a name based on ORCID names/aliases."""
    return get_gilda_grounder(hq_filter=hq_filter).ground(name)


@lru_cache(2)
def get_gilda_grounder(*, hq_filter: bool = False) -> gilda.Grounder:
    """Get a Gilda grounder from ORCID names/aliases."""
    from gilda import Grounder
    from gilda.term import TERMS_HEADER

    if not GILDA_PATH.is_file() or not GILDA_HQ_PATH.is_file():
        tqdm.write("indexing for gilda")
        with (
            gzip.open(GILDA_PATH, "wt") as gilda_file,
            gzip.open(GILDA_HQ_PATH, "wt") as gilda_hq_file,
        ):
            writer = csv.writer(gilda_file, delimiter="\t")
            hq_writer = csv.writer(gilda_hq_file, delimiter="\t")
            writer.writerow(TERMS_HEADER)
            hq_writer.writerow(TERMS_HEADER)

            # we don't need to filter duplicates globally
            for record in iter_records():
                is_hq = _is_hq(record)
                for term in _record_to_gilda_terms(record):
                    row = term.to_list()
                    writer.writerow(row)
                    if is_hq:
                        hq_writer.writerow(row)
        tqdm.write("done indexing for gilda")

    return Grounder(GILDA_HQ_PATH if hq_filter else GILDA_PATH)


@lru_cache(1)
def get_ror_grounder() -> gilda.Grounder:
    """Get a grounder for ROR."""
    import pyobo.gilda_utils

    return pyobo.gilda_utils.get_grounder("ror")


def _is_hq(record: Record) -> bool:
    # just see if there's literally anything in there
    return bool(
        any("ror" in employment.xrefs for employment in record.employments)
        or any("ror" in education.xrefs for education in record.educations)
        or record.works
    )


def _record_to_gilda_terms(record: Record) -> Iterable[gilda.Term]:
    from gilda import Term
    from gilda.process import normalize

    name = record.name
    norm_name = normalize(name).strip()
    if norm_name:
        yield Term(
            norm_text=norm_name,
            text=name,
            db="orcid",
            id=record.orcid,
            entry_name=name,
            status="name",
            source="orcid",
        )
    aliases = set(record.aliases)
    aliases.update(name_to_synonyms(name))
    aliases -= {name}
    for alias in aliases:
        if not alias:
            continue
        norm_alias = normalize(alias).strip()
        if norm_alias:
            yield Term(
                norm_text=norm_alias,
                text=alias,
                db="orcid",
                id=record.orcid,
                entry_name=name,
                status="synonym",
                source="orcid",
            )


def name_to_synonyms(name: str) -> Iterable[str]:
    """Create a synonym list from a full name."""
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


def write_schema() -> None:
    """Write the JSON schema."""
    schema = Record.model_json_schema()
    SCHEMA_PATH.write_text(json.dumps(schema, indent=2))


def _summarize():  # noqa:C901
    # count affiliations (breakdown by employer, education, combine)
    # count roles
    # count records with email

    has_email = 0
    has_github = 0
    xrefs_counter = Counter()
    affiliation_xrefs_counter = Counter()
    education_roles = Counter()
    unstandardized_education_roles = Counter()
    employment_roles = Counter()
    with (
        open(GITHUBS_PATH, "w") as githubs_file,
        open(EMAIL_PATH, "w") as emails_file,
        gzip.open(PUBMEDS_PATH, "wt") as pubmeds_file,
        gzip.open(SSSOM_PATH, "wt") as sssom_file,
    ):
        emails_writer = csv.writer(emails_file, delimiter="\t")
        emails_writer.writerow(("orcid", "email"))
        githubs_writer = csv.writer(githubs_file, delimiter="\t")
        githubs_writer.writerow(("orcid", "github"))
        pubmeds_writer = csv.writer(pubmeds_file, delimiter="\t")
        pubmeds_writer.writerow(("orcid", "pubmed"))
        sssom_writer = csv.writer(sssom_file, delimiter="\t")
        sssom_writer.writerow(
            ("subject_id", "subject_label", "predicate_id", "object_id", "mapping_justification")
        )

        for record in iter_records():
            if record.emails:
                has_email += 1
                for email in record.emails:
                    emails_writer.writerow((record.orcid, email))

            sssom_writer.writerows(
                (
                    f"orcid:{record.orcid}",
                    record.name,
                    "skos:exactMatch",
                    f"{k}:{v}",
                    "semapv:ManualMappingCuration",
                )
                for k, v in sorted(record.xrefs.items())
            )

            if github := record.xrefs.get("github"):
                githubs_writer.writerow((record.orcid, github))
                has_github += 1

            for k in record.xrefs:
                xrefs_counter[k] += 1

            for x in record.educations:
                if x.role:
                    role_std, did_std = standardize_role(x.role)
                    education_roles[role_std] += 1
                    if not did_std:
                        unstandardized_education_roles[x.role] += 1
                for k in x.xrefs:
                    affiliation_xrefs_counter[k] += 1
            for x in record.employments:
                if x.role:
                    employment_roles[x.role] += 1
                for k in x.xrefs:
                    affiliation_xrefs_counter[k] += 1

            for work in record.works:
                pubmed = _standardize_pubmed(work.pubmed)
                if pubmed:
                    pubmeds_writer.writerow((record.orcid, pubmed))

    XREFS_SUMMARY_PATH.write_text(
        f"""\
# Cross References Summary

{tabulate(xrefs_counter.most_common(), tablefmt='github', headers=['prefix', 'count'])}
    """.rstrip()
    )

    with AFFILIATION_XREFS_SUMMARY_PATH.open("w") as file:
        write_counter(file, ("prefix", "count"), affiliation_xrefs_counter)

    with gzip.open(EDUCATION_ROLE_SUMMARY_PATH, "wt") as file:
        write_counter(file, ("role", "count"), education_roles)

    with open(EDUCATION_ROLE_UNSTANDARDIZED_SUMMARY_PATH, "w") as file:
        write_counter(file, ("role", "count"), unstandardized_education_roles)

    with gzip.open(EMPLOYMENT_ROLE_SUMMARY_PATH, "wt") as file:
        write_counter(file, ("role", "count"), employment_roles)


def write_counter(file, header, counter) -> None:
    """Write a counter to a TSV file."""
    writer = csv.writer(file, delimiter="\t")
    writer.writerow(header)
    writer.writerows(counter.most_common())


def _main():
    # write_schema()  # noqa:ERA001
    # iter_records(force=True)  # noqa:ERA001
    _summarize()
    # get_gilda_grounder()  # noqa:ERA001
    # print(*ground_researcher("CT Hoyt"), sep="\n")  # noqa:ERA001


if __name__ == "__main__":
    _main()
