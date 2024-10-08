"""Utilities for querying wikidata."""

import csv

import pystow
import requests

__all__ = [
    "get_orcid_to_commons_image",
    "get_orcid_to_wikidata",
]

IMAGE_PATH = pystow.join("orcid", name="orcid_to_image.csv")

#: Wikidata SPARQL endpoint. See https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service#Interfacing
WIKIDATA_ENDPOINT = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"

#: SPARQL that can be used with query.wikidata.org to map ORCID to wikidata
ORCID_TO_WIKIDATA = "SELECT ?orcid ?item WHERE { ?item wdt:P496 ?orcid }"

ORCID_TO_IMAGE_SPARQL = """\
SELECT ?orcid ?image
WHERE { ?orcid ^wdt:P496/wdt:P18 ?image . }
"""

IRI_PREFIX = "http://www.wikidata.org/entity/"


def get_orcid_to_wikidata() -> dict[str, str]:
    """Get all ORCID to wikidata mappings."""
    res = _query(ORCID_TO_WIKIDATA)
    res.raise_for_status()
    res_json = res.json()
    return {
        record["orcid"]["value"]: record["item"]["value"].removeprefix(IRI_PREFIX)
        for record in res_json["results"]["bindings"]
    }


def get_orcid_to_commons_image() -> dict[str, str]:
    """Get all ORCID to image mappings, using a cache if pre-downloaded."""
    if IMAGE_PATH.is_file():
        with IMAGE_PATH.open() as f:
            # note that this can have multiple values,
            # so just do what python feels is right to aggregate them
            reader = csv.reader(f)
            rv = {
                orcid: v.removeprefix("http://commons.wikimedia.org/wiki/Special:FilePath/")
                for orcid, v in reader
            }
            return rv

    # ran on web in 43,678 ms, but for some reason stalls out when run this way
    res = _query(ORCID_TO_IMAGE_SPARQL)
    res.raise_for_status()
    res_json = res.json()
    rv = {
        record["orcid"]["value"]: record["image"]["value"].removeprefix(IRI_PREFIX)
        for record in res_json["results"]["bindings"]
    }
    return rv


def _query(query: str) -> requests.Response:
    return requests.get(
        WIKIDATA_ENDPOINT,
        params={"query": query, "format": "json"},
        headers={"User-Agent": "orcid_downloader"},
        timeout=60 * 5,
    )
