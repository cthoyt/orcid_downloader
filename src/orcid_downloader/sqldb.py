"""Write SQLite."""

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Literal, overload

import bioregistry
from pydantic import BaseModel
from pydantic_extra_types.country import CountryAlpha2
from semantic_pydantic import SemanticField
from tqdm import tqdm

from orcid_downloader.api import VersionInfo, _get_output_module, iter_records

__all__ = [
    "Metadata",
    "Organization",
    "get_metadata",
    "get_name",
    "write_sqlite",
]

COLUMNS = [
    "orcid",
    "name",
    "country",
    "locale",
    "ror",
    "email",
    "homepage",
    "github",
    "wos",
    "dblp",
    "scopus",
    "google",
    "linkedin",
    "wikidata",
    "mastodon",
    "commons_image",
    "works",
]


def write_sqlite(
    *,
    version_info: VersionInfo | None = None,
    researcher_table_name: str = "person",
    organization_table_name="organization",
    name_index: bool = False,
    force: bool = False,
) -> None:
    """Write a SQLite database."""
    import pandas as pd
    from pyobo.sources.geonames.geonames import get_code_to_country
    from pyobo.sources.ror import get_latest

    _, _, records = get_latest()
    id_to_name = {country.identifier: country.name for country in get_code_to_country().values()}
    ror_rows = []
    for record in tqdm(records, unit_scale=True, unit="record", desc="Parsing ROR"):
        identifier = record["id"].removeprefix("https://ror.org/")
        name = record["name"]
        country_name = None
        for address in record.get("addresses", []):
            country_id = address["country_geonames_id"]
            country_name = id_to_name[str(country_id)]
            break
        ror_rows.append((identifier, name, country_name))
    ror_df = pd.DataFrame(ror_rows, columns=["ror", "name", "country"])
    orcid_df = pd.DataFrame(
        (
            (
                record.orcid,
                record.name,
                record.country,
                record.locale,
                record.current_affiliation_ror,
                record.email,
                record.homepage,
                record.github,
                record.wos,
                record.dblp,
                record.scopus,
                record.google,
                record.linkedin,
                record.wikidata,
                record.mastodon,
                record.commons_image,
                len(record.works),
            )
            for record in iter_records(
                desc="Writing SQL database", version_info=version_info, force=force
            )
            if record.name
        ),
        columns=COLUMNS,
    )
    with sqlite3.connect(_get_orcid_db_path(version_info)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {organization_table_name};")
            cursor.execute(f"DROP TABLE IF EXISTS {researcher_table_name};")
            cursor.execute(
                f"""\
                CREATE TABLE {organization_table_name} (
                    ror text not null primary key,
                    name text not null,
                    country text
                )
            """
            )
            cursor.execute(
                f"""\
                CREATE TABLE {researcher_table_name} (
                    orcid text not null primary key,
                    name text not null,
                    country CHAR(2),
                    locale text,
                    ror text,
                    email text,
                    homepage text,
                    github text,
                    wos text,
                    dblp text,
                    scopus text,
                    google text,
                    linkedin text,
                    wikidata text,
                    mastodon text,
                    commons_image text,
                    works int
                );
            """
            )

        ror_df.to_sql(organization_table_name, conn, if_exists="append", index=False)
        orcid_df.to_sql(researcher_table_name, conn, if_exists="append", index=False)

        if name_index:
            # this adds nearly a gigabyte...
            with closing(conn.cursor()) as cursor:
                q = f"CREATE INDEX name_index ON {researcher_table_name} (name);"
                cursor.execute(q)


class Organization(BaseModel):
    """A model representing an organization."""

    ror: str = SemanticField(..., prefix="ror")
    name: str
    country: str


class Metadata(BaseModel):
    """A model representing the metadata associated with a researcher."""

    orcid: str = SemanticField(..., prefix="orcid")
    name: str
    country: CountryAlpha2 | None = None
    locale: str | None = None
    organization: Organization | None = None
    email: str | None = None
    homepage: str | None = None
    github: str | None = SemanticField(None, prefix="github")
    wos: str | None = SemanticField(None, prefix="wos.researcher")
    dblp: str | None = SemanticField(None, prefix="dblp.author")
    scopus: str | None = SemanticField(None, prefix="scopus")
    google: str | None = SemanticField(None, prefix="google.scholar")
    linkedin: str | None = None
    wikidata: str | None = SemanticField(None, prefix="wikidata")
    mastodon: str | None = None
    commons_image: str | None = None


def get_metadata(orcid: str, *, version_info: VersionInfo | None = None) -> Metadata | None:
    """Get metadata for a given ORCID."""
    path = _get_orcid_db_path(version_info=version_info)
    with sqlite3.connect(path) as conn:
        res = conn.execute(
            """\
                SELECT orcid, person.name, person.country, person.locale, person.ror,
                    organization.name, organization.country, email, homepage,
                    github, wos, dblp, scopus,
                    google, linkedin, wikidata, mastodon, commons_image
                FROM person
                LEFT JOIN organization ON person.ror = organization.ror
                WHERE orcid = ?
            """,
            (orcid,),
        )
        row = res.fetchone()
        if row is None:
            return None
        (
            orcid,
            name,
            country,
            locale,
            organization_ror,
            organization_name,
            organization_country,
            email,
            homepage,
            github,
            wos,
            dblp,
            scopus,
            google,
            linkedin,
            wikidata,
            mastodon,
            commons_image,
        ) = row
        if not wos or not bioregistry.is_valid_identifier("wos.researcher", wos):
            wos = None

    organization = organization_ror and Organization(
        ror=organization_ror, name=organization_name, country=organization_country
    )

    return Metadata(
        orcid=orcid,
        name=name,
        country=country,
        locale=locale,
        organization=organization,
        email=email,
        homepage=homepage,
        github=github,
        wos=wos,
        dblp=dblp,
        scopus=scopus,
        google=google,
        linkedin=linkedin,
        wikidata=wikidata,
        mastodon=mastodon,
        commons_image=commons_image,
    )


def _get_orcid_db_path(version_info: VersionInfo | None = None) -> Path:
    return _get_output_module(version_info).join(name="orcid.db")


def get_example_missing_wikidata(version_info: VersionInfo | None = None) -> str | None:
    """Get an example that has a current organization but no Wikidata."""
    path = _get_orcid_db_path(version_info)
    sql = """\
        SELECT orcid
        FROM person
        WHERE ror IS NOT NULL AND github IS NOT NULL AND wikidata IS NULL
        LIMIT 1
    """
    # there were 4,720 from the 2023 data when I started working
    # on this that had ROR + GitHub - Wikidata
    with sqlite3.connect(path) as conn:
        res = conn.execute(sql)
        row = res.fetchone()
        if row is None:
            return None  # though not likely
        return row[0]


# docstr-coverage:excused `overload`
@overload
def get_name(
    orcid: str, *, strict: Literal[True] = True, version_info: VersionInfo | None = ...
) -> str: ...


# docstr-coverage:excused `overload`
@overload
def get_name(
    orcid: str, *, strict: Literal[False] = False, version_info: VersionInfo | None = ...
) -> str | None: ...


def get_name(
    orcid: str, *, strict: bool = False, version_info: VersionInfo | None = None
) -> str | None:
    """Get the name from ORCID."""
    metadata = get_metadata(orcid, version_info=version_info)
    if metadata:
        return metadata.name
    elif strict:
        raise ValueError
    else:
        return None


if __name__ == "__main__":
    print(get_metadata("0000-0001-5049-4000"))  # noqa:T201
    example_orcid = get_example_missing_wikidata()
    if example_orcid:
        print(get_metadata(example_orcid))  # noqa:T201
