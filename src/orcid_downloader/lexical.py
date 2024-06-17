"""Tools for lexical indexing and matching."""

from __future__ import annotations

import csv
import gzip
import json
import sqlite3
from collections.abc import Iterable
from contextlib import closing
from functools import lru_cache
from itertools import batched

import gilda
import pandas as pd
from gilda import Grounder, ScoredMatch, Term
from gilda.resources.sqlite_adapter import SqliteEntries
from gilda.term import TERMS_HEADER
from tqdm import tqdm

from orcid_downloader.api import MODULE, Record, _is_hq, iter_records, name_to_synonyms

__all__ = [
    "write_lexical",
    "write_gilda",
    "get_orcid_grounder",
]

GILDA_PATH = MODULE.join(name="gilda.tsv.gz")
GILDA_HQ_PATH = MODULE.join(name="gilda_hq.tsv.gz")
GILDA_DB_PATH = MODULE.join(name="orcid-gilda.db")


@lru_cache(1)
def get_orcid_grounder() -> Grounder:
    """Get a Gilda Grounder object for ORCID."""
    entries = UngroupedSqliteEntries(GILDA_DB_PATH)
    return ORCIDGrounder(entries)


class UngroupedSqliteEntries(SqliteEntries, dict):
    """An interface to the SQLite lexical index compatible with Gilda."""

    def get(self, key, default=None):
        """Get a term from the lexical index."""
        with sqlite3.connect(self.db) as conn:
            res = conn.execute("SELECT term FROM terms WHERE norm_text=?", (key,))
            terms = res.fetchall()
            if not terms:
                return default
            return [Term(**json.loads(term)) for (term,) in terms]

    def values(self):
        """Iterate over the terms in the lexical index."""
        with sqlite3.connect(self.db) as conn:
            res = conn.execute("SELECT term FROM terms")
            for (result,) in res.fetchall():
                yield Term(**json.loads(result))

    def __len__(self) -> int:
        """Get the number of unique keys in the lexical index."""
        with sqlite3.connect(self.db) as conn:
            res = conn.execute("SELECT COUNT(DISTINCT norm_text) FROM terms")
            return res.fetchone()[0]

    def __iter__(self):
        """Iterate over the keys in the lexical index."""
        with sqlite3.connect(self.db) as conn:
            res = conn.execute("SELECT DISTINCT norm_text FROM terms")
            for (norm_text,) in tqdm(
                res.fetchall(), desc="Iterating over lexical index", unit_scale=True
            ):
                yield norm_text


class ORCIDGrounder(Grounder):
    """A custom grounder for ORCID."""

    def _build_prefix_index(self):
        pass  # override building this to save 60 seconds on startup

    def ground(self, raw_str, context=None, organisms=None, namespaces=None) -> list[ScoredMatch]:
        """Ground a string to a researcher, also trying synonym generation during lookup."""
        if rv := super().ground(
            raw_str, context=context, organisms=organisms, namespaces=namespaces
        ):
            return rv
        for x in name_to_synonyms(raw_str):
            if rv := super().ground(x, context=context, organisms=organisms, namespaces=namespaces):
                return rv
        return []


def write_lexical():
    """Build a SQLite database file from a set of grounding entries."""
    if GILDA_DB_PATH.is_file():
        GILDA_DB_PATH.unlink()
    with sqlite3.connect(GILDA_DB_PATH) as conn:
        with closing(conn.cursor()) as cur:
            # Create the table
            q = "CREATE TABLE terms (norm_text text not null, term text not null)"
            cur.execute(q)

        rows = (
            (term.norm_text, json.dumps(term.to_json()))
            for record in iter_records()
            if record.name
            for term in _record_to_gilda_terms(record)
        )
        for x in batched(rows, 1_000_000):
            df = pd.DataFrame(x, columns=["norm_text", "term"])
            df.to_sql(
                "terms", con=conn, if_exists="append", index=False
            )  # dtype={'name_of_json_column_in_source_table': sqlalchemy.types.JSON}

        with closing(conn.cursor()) as cur:
            # Build index
            q = "CREATE INDEX norm_index ON terms (norm_text);"
            cur.execute(q)


def write_gilda() -> None:
    """Write Gilda indexes."""
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
            if not record.name:
                continue
            for term in _record_to_gilda_terms(record):
                row = term.to_list()
                writer.writerow(row)
                if is_hq:
                    hq_writer.writerow(row)
    tqdm.write("done indexing for gilda")


def _record_to_gilda_terms(record: Record) -> Iterable[gilda.Term]:
    from gilda import Term
    from gilda.process import normalize

    name = record.name
    if not name:
        return
    norm_name = normalize(name).strip()
    if not norm_name:
        return
    yield Term(
        norm_text=norm_name,
        text=name,
        db="orcid",
        id=record.orcid,
        entry_name=name,
        status="name",
        source="orcid",
    )
    aliases: set[str] = set()
    aliases.update(name_to_synonyms(name))
    for alias in record.aliases:
        aliases.add(alias)
        aliases.update(name_to_synonyms(alias))
    aliases -= {name}
    for alias in sorted(aliases):
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


if __name__ == "__main__":
    grounder = get_orcid_grounder()
    print(grounder.ground_best("Joel A Gordon"))  # noqa:T201
    print(grounder.ground_best("Joel A. Gordon"))  # noqa:T201
    print(grounder.ground_best("J A Gordon"))  # noqa:T201
    print(grounder.ground_best("J. A. Gordon"))  # noqa:T201
    print(grounder.ground_best("J.A. Gordon"))  # noqa:T201
