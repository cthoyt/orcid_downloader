"""Utilities for analyzing PubMed."""

from collections import Counter, defaultdict
from itertools import batched
from typing import Any, NamedTuple

from indra.literature import pubmed_client
from tqdm import tqdm

from orcid_downloader.lexical import get_orcid_grounder

MAXIMUM_BATCH_SIZE = 200
type PubmedMetadata = dict[str, dict[str, Any]]


def get_metadata_batched(pubmed_ids: list[str]) -> PubmedMetadata:
    """Get metadata for multiple PubMed identifiers, batched."""
    results = {}
    for batch in tqdm(
        batched(sorted(pubmed_ids), MAXIMUM_BATCH_SIZE),
        total=1 + len(pubmed_ids) // MAXIMUM_BATCH_SIZE,
        unit=f"batch of {MAXIMUM_BATCH_SIZE}",
        desc="Looking up",
    ):
        # Requires development version of INDRA
        results.update(pubmed_client.get_metadata_for_ids(batch, detailed_authors=True))
    return results


class Results(NamedTuple):
    """A results object when processing PMID results."""

    annotations: list[tuple[str, str]]
    orcid_to_papers: dict[str, list[str]]
    ambiguous: Counter[str]
    misses: Counter[str]


def process_pmid_results(pubmed_metadata: PubmedMetadata) -> Results:
    """Process metadata from PubMed records."""
    annotations: list[tuple[str, str]] = []
    ambiguous: Counter[str] = Counter()
    misses: Counter[str] = Counter()
    for pubmed, data in tqdm(pubmed_metadata.items(), unit_scale=True, desc="Grounding"):
        authors = data["authors"]
        for author in authors:
            first_name = author["first_name"]
            if not first_name:
                continue
            last_name = author["last_name"]
            matches = get_orcid_grounder().ground(f"{first_name} {last_name}")
            if len(matches) == 1:
                annotations.append((pubmed, matches[0].term.id))
            elif matches:
                ambiguous[first_name + " " + last_name] += 1
                # print(pubmed, name, len(matches), author['affiliations']) # noqa:ERA001
                # 2. if there are multiple, see if we can match any affiliations
                pass
            else:
                if "Steven" in first_name:
                    tqdm.write(first_name)
                    tqdm.write(last_name)
                misses[first_name + " " + last_name] += 1

    orcid_to_papers_dd: defaultdict[str, set[str]] = defaultdict(set)
    for pubmed, orcid in annotations:
        orcid_to_papers_dd[orcid].add(pubmed)
    orcid_to_papers = {k: sorted(v) for k, v in orcid_to_papers_dd.items()}

    return Results(annotations, orcid_to_papers, ambiguous, misses)


def search_pubmed_for_name(name: str) -> list[str]:
    """Get PubMed identifiers based on an author's name."""
    return pubmed_client.get_ids(f'"{name}"', use_text_word=False)
