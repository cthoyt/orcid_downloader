"""Tools for lexical indexing and matching."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from contextlib import closing
from functools import lru_cache
from itertools import batched
from pathlib import Path
from typing import Any

import gilda
import pandas as pd
import ssslm
from curies import NamedReference
from curies.vocabulary import exact_match, has_label
from gilda.resources.sqlite_adapter import SqliteEntries
from pystow.utils import safe_open_writer
from ssslm import LiteralMapping, LiteralMappingTuple
from tqdm import tqdm

from orcid_downloader.api import Record, VersionInfo, _get_output_module, iter_records
from orcid_downloader.name_utils import name_to_synonyms

__all__ = [
    "get_orcid_grounder",
    "write_lexical",
    "write_lexical_sqlite",
]


def get_orcid_grounder(version_info: VersionInfo | None = None) -> ssslm.Grounder:
    """Get a grounder object for ORCID."""
    module = _get_output_module(version_info)
    path = module.join(name="orcid-gilda.db")
    return _get_orcid_grounder_helper(path)


@lru_cache(1)
def _get_orcid_grounder_helper(path: str | Path) -> ssslm.Grounder:
    path_norm = Path(path).expanduser().resolve().as_posix()
    entries = UngroupedSqliteEntries(path_norm)  # type:ignore[arg-type]
    gilda_grounder = NonIndexingGildaGrounder(entries)
    return ExtendedMatcher(gilda_grounder)


class NonIndexingGildaGrounder(gilda.Grounder):
    """A custom grounder for ORCID."""

    def _build_prefix_index(self) -> None:
        pass  # override building this to save 60 seconds on startup


class UngroupedSqliteEntries(SqliteEntries, dict):
    """An interface to the SQLite lexical index compatible with Gilda."""

    def get(self, key, default=None):
        """Get a term from the lexical index."""
        with sqlite3.connect(self.db) as conn:
            res = conn.execute("SELECT term FROM terms WHERE norm_text=?", (key,))
            terms = res.fetchall()
            if not terms:
                return default
            return [gilda.Term(**json.loads(term)) for (term,) in terms]

    def values(self):
        """Iterate over the terms in the lexical index."""
        with sqlite3.connect(self.db) as conn:
            res = conn.execute("SELECT term FROM terms")
            for (result,) in res.fetchall():
                yield gilda.Term(**json.loads(result))

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


class ExtendedMatcher(ssslm.GildaGrounder):
    """A matcher that had an alternate text generator function."""

    def get_matches(self, text, **kwargs: Any) -> list[ssslm.Match]:
        """Ground a string to a researcher, also trying synonym generation during lookup."""
        if matches := super().get_matches(text, **kwargs):
            return matches
        for synonym in name_to_synonyms(text):
            if matches := super().get_matches(synonym, **kwargs):
                return matches
        return []


def write_lexical_sqlite(*, version_info: VersionInfo | None = None, force: bool = False) -> None:
    """Build a SQLite database file from a set of grounding entries."""
    from gilda.process import normalize

    path = _get_output_module(version_info).join(name="orcid-gilda.db")

    if path.is_file():
        path.unlink()
    with sqlite3.connect(path) as conn:
        with closing(conn.cursor()) as cur:
            # Create the table
            q = "CREATE TABLE terms (norm_text text not null, term text not null)"
            cur.execute(q)

        rows = (
            (term.norm_text, json.dumps(term.to_json()))
            for record in iter_records(
                desc="Writing SQLite index", version_info=version_info, force=force
            )
            if record.name
            for literal_mapping in _record_to_literal_mappings(record)
            # TODO upstream this so no term gets constructed if it doesn't have text
            if normalize(literal_mapping.text).strip() and (term := literal_mapping.to_gilda())
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


def write_lexical(*, version_info: VersionInfo | None = None, force: bool = False) -> None:
    """Write SSSLM."""
    module = _get_output_module(version_info)
    lq_path = module.join(name="orcid.lq.ssslm.tsv.gz")
    hq_path = module.join(name="orcid.ssslm.tsv.gz")

    tqdm.write("writing for SSSLM")
    with safe_open_writer(lq_path) as lq_writer, safe_open_writer(hq_path) as hq_writer:
        lq_writer.writerow(LiteralMappingTuple._fields)
        hq_writer.writerow(LiteralMappingTuple._fields)
        for record in iter_records(desc="Writing SSSLM", version_info=version_info, force=force):
            if not record.name:
                continue
            is_hq = record.is_high_quality()
            for literal_mapping in _record_to_literal_mappings(record):
                row = literal_mapping._as_row()
                lq_writer.writerow(row)
                if is_hq:
                    hq_writer.writerow(row)
    tqdm.write("done writing SSSLM")


def _record_to_literal_mappings(record: Record) -> Iterable[LiteralMapping]:
    from gilda.process import normalize

    name = record.name
    if not name or not normalize(name).strip():
        return

    reference = NamedReference(
        prefix="orcid",
        identifier=record.orcid,
        name=name,
    )
    yield LiteralMapping(
        reference=reference,
        text=name,
        predicate=has_label,
        source="orcid",
    )
    aliases: set[str] = set()
    aliases.update(name_to_synonyms(name))
    for alias in record.aliases:
        if not normalize(alias).strip():
            continue
        aliases.add(alias)
        aliases.update(name_to_synonyms(alias))
    aliases -= {name}
    for alias in sorted(aliases):
        if not alias:
            continue
        yield LiteralMapping(
            reference=reference,
            predicate=exact_match,
            text=alias,
            source="orcid",
        )


if __name__ == "__main__":
    grounder = get_orcid_grounder()
    print(grounder.get_best_match("Joel A Gordon"))  # noqa:T201
    print(grounder.get_best_match("Joel A. Gordon"))  # noqa:T201
    print(grounder.get_best_match("J A Gordon"))  # noqa:T201
    print(grounder.get_best_match("J. A. Gordon"))  # noqa:T201
    print(grounder.get_best_match("J.A. Gordon"))  # noqa:T201
