"""Write SQLite."""

import sqlite3
from contextlib import closing

from pydantic import BaseModel
from pydantic_extra_types.country import CountryAlpha2
from tqdm import tqdm

from orcid_downloader.api import MODULE, iter_records

__all__ = [
    "write_sqlite",
    "Organization",
    "Metadata",
    "get_metadata",
]


PATH = MODULE.join(name="orcid.db")
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
    "works",
]


def write_sqlite(
    researcher_table_name: str = "person",
    organization_table_name="organization",
    *,
    name_index: bool = False,
    force: bool = False,
) -> None:
    """Write a SQLite database."""
    if not force and PATH.is_file():
        return

    import pandas as pd
    from pyobo.sources.geonames import get_code_to_country
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
                len(record.works),
            )
            for record in iter_records()
            if record.name
        ),
        columns=COLUMNS,
    )
    with sqlite3.connect(PATH) as conn:
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

    ror: str
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
    github: str | None = None
    wos: str | None = None
    dblp: str | None = None
    scopus: str | None = None
    google: str | None = None
    linkedin: str | None = None
    wikidata: str | None = None
    mastodon: str | None = None


def get_metadata(orcid: str) -> Metadata | None:
    """Get metadata for a given ORCID."""
    with sqlite3.connect(PATH) as conn:
        res = conn.execute(
            """\
                SELECT orcid, person.name, person.country, person.locale, person.ror,
                    organization.name, organization.country, email, homepage,
                    github, wos, dblp, scopus,
                    google, linkedin, wikidata, mastodon
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
            ror,
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
        ) = row

    organization = ror and Organization(
        ror=ror, name=organization_name, country=organization_country
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
    )


if __name__ == "__main__":
    write_sqlite(force=False)
    print(get_metadata("0000-0001-5049-4000"))  # noqa:T201
