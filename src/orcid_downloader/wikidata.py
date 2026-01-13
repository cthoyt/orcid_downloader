"""Utilities for querying wikidata."""

from __future__ import annotations

import json
from typing import cast

import pystow
import wikidata_client

__all__ = [
    "get_orcid_to_commons_image",
    "get_orcid_to_wikidata",
]

IMAGE_PATH = pystow.join("orcid", name="orcid_to_image.csv")
QLEVER = "https://qlever.dev/api/wikidata"

#: Wikidata SPARQL endpoint. See https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service#Interfacing
WIKIDATA_ENDPOINT = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"

#: SPARQL that can be used with query.wikidata.org to map ORCID to wikidata
ORCID_TO_WIKIDATA = """\
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT ?orcid ?item
WHERE { ?item wdt:P496 ?orcid }
"""

ORCID_TO_IMAGE_SPARQL = """\
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT ?orcid ?image
WHERE { ?orcid ^wdt:P496/wdt:P18 ?image . }
"""


def get_orcid_to_wikidata() -> dict[str, str]:
    """Get all ORCID to wikidata mappings."""
    records = wikidata_client.query(ORCID_TO_WIKIDATA, endpoint=QLEVER)
    return {record["orcid"]: record["item"] for record in records}


IMAGE_URL_PREFIX = "http://commons.wikimedia.org/wiki/Special:FilePath/"


def get_orcid_to_commons_image() -> dict[str, str]:
    """Get all ORCID to image mappings, using a cache if pre-downloaded."""
    if IMAGE_PATH.is_file():
        with IMAGE_PATH.open() as file:
            return cast(dict[str, str], json.load(file))

    # ran on web in 43,678 ms, but for some reason stalls out when run this way
    records = wikidata_client.query(
        ORCID_TO_IMAGE_SPARQL,
        timeout=60 * 5,
        endpoint=QLEVER,
    )
    rv = {record["orcid"]: record["image"].removeprefix(IMAGE_URL_PREFIX) for record in records}

    with IMAGE_PATH.open("w") as file:
        json.dump(rv, file, indent=2)

    return rv
