"""Download and process ORCID in bulk."""

from __future__ import annotations

import csv
import gzip
import json
import logging
import tarfile
import typing
from collections import Counter
from collections.abc import Iterable
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple
from urllib.parse import parse_qs, unquote, urlparse

import pystow
from lxml import etree
from pydantic import BaseModel, Field
from pydantic_extra_types.country import CountryAlpha2, _index_by_alpha2
from semantic_pydantic import SemanticField
from tqdm.auto import tqdm

from orcid_downloader.ror import get_ror_grounder
from orcid_downloader.standardize import standardize_role

if TYPE_CHECKING:
    import gilda

__all__ = [
    "ensure_summaries",
    "iter_records",
    "get_records",
    "ground_researcher",
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
    "keyword": "http://www.orcid.org/ns/keyword",
    "membership": "http://www.orcid.org/ns/membership",
    "address": "http://www.orcid.org/ns/address",
    "preferences": "http://www.orcid.org/ns/preferences",
}
MODULE = pystow.module("orcid", VERSION_2023.version)
RECORDS_PATH = MODULE.join(name="records.jsonl.gz")
RECORDS_HQ_PATH = MODULE.join(name="records_hq.jsonl.gz")
SCHEMA_PATH = MODULE.join(name="schema.json")


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
AFFILIATION_NO_ROR_PATH = ROLES.join(name="affiliation_missing_ror.tsv")


def _norm_key(id_type):
    return id_type.lower().replace(" ", "").rstrip(":")


EXTERNAL_ID_SKIP = {
    "iAuthor": "Reuses orcid",
    "中国科学家在线": "iAuthor, but site is dead",
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
    "Pitt ID": "dead website",
    "VIVO Cornell": "dead website",
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
    "ID Dialnet": "dialnet",  # TODO add to bioregistry dialnet.unirioja.es/servlet/autor?codigo=$1
    "Dialnet ID": "dialnet",
    "SciProfiles": "sciprofiles",  # TODO add to bioregistry https://sciprofiles.com/profile/$1
    "Sciprofile": "sciprofiles",
    "Ciência ID": "ciencia",  # TODO add to bioregistry http://www.cienciavitae.pt/$1
    "KAKEN": "kaken",  # TODO add to bioregistry https://nrid.nii.ac.jp/nrid/1000050211371
    "Researcher Name Resolver ID": "kaken",
    # TODO add Social Science Research Network to bioregistry
    #  https://papers.ssrn.com/sol3/cf_dev/AbsByAuth.cfm?per_id=3194769
    "SSRN": "ssrn",
    "socialscienceresearchnetwork": "ssrn",
    "ssrnauthorpage": "ssrn",
    "ssrnpage": "ssrn",
}
EXTERNAL_ID_MAPPING = {_norm_key(k): v for k, v in EXTERNAL_ID_MAPPING.items()}
UNMAPPED_EXTERNAL_ID: set[str] = set()
PERSONAL_KEYS = {
    "website",
    "homepage",
    "blog",
    "personalpage",
    "personalhomepage",
    "personalwebsite",
    "personalwebsites",
    "personalweb-page",
    "personalwebpage",
    "webpage",
    "personal",
    "professionalwebsite",
    "personalsite",
    "personalblog",
    "mywebsite",
    "mysite",
    "officialweb-page",
    "sitiowebpersonal",
    "paginaweb",
    "personelwebsite",
    "blogpessoal",
    "mypersonalsite",
    "mypersonalblog",
    "personalweb-site",
    "web-site",
    "professionalblog",
    "personalwebsiteandblog",
    "myweb",
    "homewebsite",
    "personalweb",
    "mypersonalwebsite",
    "blogpersonal",
}


def ensure_summaries() -> Path:
    """Ensure the ORCID summaries file (32+ GB) is downloaded."""
    return MODULE.ensure(url=VERSION_2023.url, name=VERSION_2023.fname)


class Work(BaseModel):
    """A model representing a creative work."""

    pubmed: str = Field(..., title="PubMed identifier")


class Date(BaseModel):
    """A model representing a date."""

    year: int
    month: int | None = None
    day: int | None = None


class Affiliation(BaseModel):
    """A model representing an affiliation (either education or employment)."""

    name: str
    start: Date | None = Field(None, title="Start Year")
    end: Date | None = Field(None, title="End Year")
    role: str | None = None
    xrefs: dict[str, str] = Field(default_factory=dict, title="Database Cross-references")
    # xrefs includes ror, ringgold, grid, funderregistry, lei
    # LEI see https://www.gleif.org/en/lei-data/gleif-concatenated-file/download-the-concatenated-file

    @property
    def ror(self) -> str | None:
        """Get the affiliation's ROR identifier, if available."""
        return self.xrefs.get("ror")


class Record(BaseModel):
    """A model representing a person."""

    orcid: str = SemanticField(..., prefix="orcid")
    name: str
    homepage: str | None = Field(None)
    locale: str | None = Field(None)
    countries: list[CountryAlpha2] = Field(
        default_factory=list, description="The ISO 3166-1 alpha-2 country codes (uppercase)"
    )
    aliases: list[str] = Field(default_factory=list)
    xrefs: dict[str, str] = Field(default_factory=dict, title="Database Cross-references")
    works: list[Work] = Field(default_factory=list)
    employments: list[Affiliation] = Field(default_factory=list)
    educations: list[Affiliation] = Field(default_factory=list)
    memberships: list[Affiliation] = Field(default_factory=list)
    emails: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)

    @property
    def email(self) -> str | None:
        """Get the first email, if available."""
        return self.emails[0] if self.emails else None

    @property
    def country(self) -> str | None:
        """Get the first country, if available."""
        return self.countries[0] if self.countries else None

    @property
    def github(self) -> str | None:
        """Get the researcher's GitHub username, if available."""
        return self.xrefs.get("github")

    @property
    def linkedin(self) -> str | None:
        """Get the researcher's LinkedIn username, if available."""
        return self.xrefs.get("linkedin")

    @property
    def loop(self) -> str | None:
        """Get the researcher's Loop identifier, if available."""
        return self.xrefs.get("loop")

    @property
    def wos(self) -> str | None:
        """Get the researcher's Web of Science identifier, if available."""
        return self.xrefs.get("wos.researcher")

    @property
    def dblp(self) -> str | None:
        """Get the researcher's DBLP identifier, if available."""
        return self.xrefs.get("dblp")

    @property
    def scopus(self) -> str | None:
        """Get the researcher's Scopus identifier, if available."""
        return self.xrefs.get("scopus")

    @property
    def google(self) -> str | None:
        """Get the researcher's Google Scholar identifier, if available."""
        return self.xrefs.get("google.scholar")

    @property
    def wikidata(self) -> str | None:
        """Get the researcher's Wikidata identifier, if available."""
        return self.xrefs.get("wikidata")

    @property
    def mastodon(self) -> str | None:
        """Get the researcher's Mastodon handle, if available."""
        return self.xrefs.get("mastodon")

    @property
    def current_affiliation_ror(self) -> str | None:
        """Guess the current affiliation and return its ROR identifier, if available."""
        # assume that if there are employments listed that are not over yet,
        # then these surpass education
        employments = [e for e in self.employments if e.end is None]
        if employments:
            return employments[0].ror
        educations = [e for e in self.educations if e.end is None]
        if educations:
            return educations[0].ror
        return None


def _iter_tarfile_members(path: Path):
    tar_file = tarfile.open(path)
    while member := tar_file.next():
        if not member.name.endswith(".xml"):
            continue
        yield tar_file.extractfile(member)
    tar_file.close()


def iter_records(*, force: bool = False, records_path: Path | None = None) -> Iterable[Record]:
    """Parse ORCID summary XML files, takes about an hour."""
    if records_path is None:
        records_path = RECORDS_PATH
    if not force and records_path.is_file():
        tqdm.write(f"reading cached records from {records_path}")
        with gzip.open(records_path, "rt") as file:
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
            gzip.open(records_path, "wt") as records_file,
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

    name = name and _clean_name(name)
    aliases.update(_iter_other_names(tree))
    if name in aliases:  # make sure there's no duplicate
        aliases.remove(name)
    name, aliases = _reconcile_aliass(name, aliases)

    record: dict[str, Any] = dict(orcid=orcid, name=name)
    if aliases:
        record["aliases"] = sorted(aliases)

    employments = _get_employments(tree, grounder=ror_grounder)
    if employments:
        record["employments"] = employments

    educations = _get_educations(tree, grounder=ror_grounder)
    if educations:
        record["educations"] = educations

    memberships = _get_memberships(tree, grounder=ror_grounder)
    if memberships:
        record["memberships"] = memberships

    ids, homepage = _get_external_identifiers(tree, orcid=orcid)
    if ids:
        record["xrefs"] = ids
    if homepage:
        record["homepage"] = homepage

    if works := _get_works(tree, orcid=orcid):
        record["works"] = works

    if emails := _get_emails(tree):
        record["emails"] = emails

    if keywords := _get_keywords(tree):
        record["keywords"] = sorted(keywords)

    if countries := _get_countries(tree, orcid=orcid):
        record["countries"] = countries
    if locale := _get_locale(tree, orcid=orcid):
        record["locale"] = locale

    return Record.parse_obj(record)


def _reconcile_aliass(name: str | None, aliases: set[str]) -> tuple[str | None, set[str]]:
    # TODO if there is a comma in the main name picked, try and find an alias with no commas
    return name, aliases


def _clean_name(name: str) -> str:
    # strip titles like Dr. and DR. from begininng of all names/aliases
    # strip post-titles Francess Dufie Azumah (DR.)
    lower = name.lower()
    if lower.startswith("professor "):
        name = name[len("professor ") :].strip()
    if lower.startswith("prof."):
        name = name[len("prof.") :].strip()
    if lower.startswith("dr "):
        name = name[len("dr ") :]
    if lower.startswith("dr."):
        name = name[len("dr.") :].strip()
    if lower.endswith("(dr)"):
        name = name[: len("(dr)")].strip()
    if lower.endswith("(dr.)"):
        name = name[: len("(dr.)")].strip()
    if lower.endswith(", m.d."):
        name = name[: len(", m.d.")].strip()
    return name


def _iter_other_names(t) -> Iterable[str]:
    for part in t.findall(".//other-name:content", namespaces=NAMESPACES):
        part = part.text.strip()
        for z in part.split(";"):
            z = z.strip()
            if z is not None and " " in z and len(z) < 60:
                yield _clean_name(z.strip())


UNKNOWN_SOURCES = {}
LOWERCASE_THESE_SOURCES = {"RINGGOLD", "GRID", "LEI"}
UNKNOWN_NAMES: typing.Counter[str] = Counter()
UNKNOWN_NAMES_EXAMPLES: dict[str, str] = {}
UNKNOWN_NAMES_FULL: dict[str, str] = {}


def _get_external_identifiers(tree, orcid) -> tuple[dict[str, str], str | None]:  # noqa:C901
    rv = {}
    homepage = None
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
        name = element.findtext(".//researcher-url:url-name", namespaces=NAMESPACES)
        url = element.findtext(".//researcher-url:url", namespaces=NAMESPACES).rstrip("/")
        if name and homepage is None and _norm_key(name) in PERSONAL_KEYS:
            homepage = url
            continue
        url = url.removeprefix("https://")
        url = url.removeprefix("Https://")
        url = url.removeprefix("http://")
        if url.startswith("github.com/"):
            identifier = url.removeprefix("github.com/")
            if "/" not in identifier:  # i.e., this is not a specific repo
                rv["github"] = identifier
        elif url.startswith("www.github.com/"):
            identifier = url.removeprefix("www.github.com/")
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
            identifier = url.rstrip("/").split("linkedin.com/in/")[1]
            rv["linkedin"] = unquote(identifier)
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
        elif url.startswith("lattes.cnpq.br/"):
            rv["curriculolattes"] = url.removeprefix("lattes.cnpq.br/")
        elif url.startswith("dialnet.unirioja.es/servlet/autor?codigo="):
            rv["dialnet"] = url.removeprefix("dialnet.unirioja.es/servlet/autor?codigo=")
        elif url.startswith("papers.ssrn.com/sol3/cf_dev/AbsByAuth.cfm?per_id="):
            rv["ssrn"] = url.removeprefix("papers.ssrn.com/sol3/cf_dev/AbsByAuth.cfm?per_id=")
        elif url.startswith("osf.io/"):
            rv["osf"] = url.removeprefix("osf.io/")
        elif url.startswith("viaf.org/viaf/"):
            rv["viaf"] = url.removeprefix("viaf.org/viaf/")
        elif url.startswith("ieeexplore.ieee.org/author/"):
            rv["ieee.author"] = url.removeprefix("ieeexplore.ieee.org/author/")
        elif url.startswith("loop.frontiersin.org/people/"):
            loop_identifier = (
                url.removeprefix("loop.frontiersin.org/people/")
                .removesuffix("/overview")
                .removesuffix("/bio")
            )
            rv["loop"] = loop_identifier
        elif url.startswith("dblp.org/pid/"):
            rv["dblp"] = url.removeprefix("dblp.org/pid/").removesuffix(".html")
        elif url.startswith("dblp.uni-trier.de/pid/"):
            rv["dblp"] = url.removeprefix("dblp.uni-trier.de/pid/").removesuffix(".html")
        elif url.startswith("hub.docker.com/u/"):
            rv["dockerhub.user"] = url.removeprefix("hub.docker.com/u/")
        elif name:
            if name.lower() == "mastodon":
                try:
                    host, username = url.rstrip("/").rsplit("/", 1)
                except ValueError:
                    tqdm.write(f"[{orcid}] malformed mastodon URL: {url}")
                else:
                    host = host.removesuffix("/web")
                    host = host.removesuffix("/media")
                    rv["mastodon"] = f"{username}@{host}"
            else:
                norm_name = _norm_key(name)
                UNKNOWN_NAMES[norm_name] += 1
                UNKNOWN_NAMES_FULL[norm_name] = name
                UNKNOWN_NAMES_EXAMPLES[norm_name] = url
        # else, no name, nothing to do here. maybe add some logging?

    return rv, homepage


def _get_emails(tree) -> list[str]:
    return [
        email.text.strip()
        for email in tree.findall(".//email:emails/email:email/email:email", namespaces=NAMESPACES)
    ]


def _get_keywords(tree) -> Iterable[str]:
    return [
        keyword.text.strip()
        for keyword in tree.findall(
            ".//keyword:keywords/keyword:keyword/keyword:content", namespaces=NAMESPACES
        )
        if keyword.text
    ]


def _get_countries(tree, orcid) -> list[str]:
    rv = []
    for country in tree.findall(
        ".//address:addresses/address:address/address:country", namespaces=NAMESPACES
    ):
        value = country.text
        if not value:
            continue
        value = value.strip().upper()
        if value == "XK":
            # XK is a proposed code for Kosovo, but isn't valid.
            # Only an issue for a few dozen records
            continue
        elif value not in _index_by_alpha2():
            tqdm.write(f"[{orcid}] invalid 2 letter country code: {value}")
            continue
        rv.append(value)
    return rv


def _get_locale(tree, orcid) -> str | None:
    value = tree.findtext(".//preferences:preferences/preferences:locale", namespaces=NAMESPACES)
    if value is None:
        return None
    return value.strip()


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


def _get_memberships(tree, grounder: gilda.Grounder):
    elements = tree.findall(
        ".//activities:memberships//membership:membership-summary", namespaces=NAMESPACES
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

        if (start_date := element.find(".//common:start-date", namespaces=NAMESPACES)) is not None:
            record["start"] = _get_date(start_date)
        if (end_date := element.find(".//common:end-date", namespaces=NAMESPACES)) is not None:
            record["end"] = _get_date(end_date)

        if role := _get_role(element):
            record["role"] = role

        results.append(record)
    return results


def _get_date(date_element) -> Date | None:
    year = date_element.findtext(".//common:year", namespaces=NAMESPACES)
    if year is None:
        return None
    month = date_element.findtext(".//common:month", namespaces=NAMESPACES)
    day = date_element.findtext(".//common:day", namespaces=NAMESPACES)
    return Date(year=year, month=month, day=day)


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


def _is_hq(record: Record) -> bool:
    # just see if there's literally anything in there
    return bool(
        any("ror" in employment.xrefs for employment in record.employments)
        or any("ror" in education.xrefs for education in record.educations)
        or record.works
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


def ground_researcher(name: str) -> list[gilda.ScoredMatch]:
    """Ground a name based on ORCID names/aliases."""
    from .lexical import get_orcid_grounder

    return get_orcid_grounder().ground(name)


def write_schema() -> None:
    """Write the JSON schema."""
    schema = Record.model_json_schema()
    SCHEMA_PATH.write_text(json.dumps(schema, indent=2))


def write_summaries(*, force: bool = False):  # noqa:C901
    """Write summary files."""
    from tabulate import tabulate

    # count affiliations (breakdown by employer, education, combine)
    # count roles
    # count records with email

    has_email = 0
    has_github = 0
    xrefs_counter: Counter[str] = Counter()
    affiliation_xrefs_counter: Counter[str] = Counter()
    education_roles: Counter[str] = Counter()
    unstandardized_education_roles: Counter[str] = Counter()
    employment_roles: Counter[str] = Counter()
    affiliation_no_ror: Counter[str] = Counter()
    grounder = get_ror_grounder()
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

        for record in iter_records(force=force):
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

            for education in record.educations:
                if education.role:
                    role_std, did_std = standardize_role(education.role)
                    education_roles[role_std] += 1
                    if not did_std:
                        unstandardized_education_roles[education.role] += 1
                for k in education.xrefs:
                    affiliation_xrefs_counter[k] += 1
                if "ror" not in education.xrefs and not grounder.ground(education.name):
                    affiliation_no_ror[education.name] += 1

            for employment in record.employments:
                if employment.role:
                    employment_roles[employment.role] += 1
                for k in employment.xrefs:
                    affiliation_xrefs_counter[k] += 1
                if "ror" not in employment.xrefs and not grounder.ground(education.name):
                    affiliation_no_ror[employment.name] += 1

            for membership in record.memberships:
                # TODO role standardization?
                if "ror" not in employment.xrefs and not grounder.ground(education.name):
                    affiliation_no_ror[membership.name] += 1

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

    with open(AFFILIATION_NO_ROR_PATH, "w") as file:
        write_counter(file, ("name", "count"), affiliation_no_ror)

    with gzip.open(EMPLOYMENT_ROLE_SUMMARY_PATH, "wt") as file:
        write_counter(file, ("role", "count"), employment_roles)


def write_counter(file, header, counter) -> None:
    """Write a counter to a TSV file."""
    writer = csv.writer(file, delimiter="\t")
    writer.writerow(header)
    writer.writerows(counter.most_common())


def _process_example() -> Record | None:
    import gilda

    here = Path(__file__).parent.parent.parent.resolve()
    example_path = here.joinpath("example.xml")
    grounder = gilda.Grounder([])
    with example_path.open() as file:
        res = _process_file(file, grounder)
    return res


def _main():
    from .lexical import write_gilda, write_lexical
    from .owl import write_owl_rdf
    from .sqldb import write_sqlite

    write_schema()
    write_summaries()
    tqdm.write("Writing SQLite")
    write_sqlite()
    tqdm.write("Writing OWL")
    write_owl_rdf()
    tqdm.write("Generating Gilda TSV (~30 min)")
    write_gilda()
    tqdm.write("Generating Gilda TSV (~30 min)")
    write_lexical()
    # print(*ground_researcher("CT Hoyt"), sep="\n")  # noqa:ERA001


if __name__ == "__main__":
    _main()
