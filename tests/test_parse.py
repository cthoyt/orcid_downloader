"""Test parsing XML."""

import unittest
from pathlib import Path

import ssslm

from orcid_downloader.api import _process_file

HERE = Path(__file__).parent.resolve()
EXAMPLE_PATH = HERE.joinpath("example.xml")


class TestParse(unittest.TestCase):
    """Test parsing an XML file."""

    def test_parse_works(self) -> None:
        """Test parsing works."""
        grounder = ssslm.make_grounder([])
        orcid_to_wikimedia_commons = {}
        orcid_to_wikidata = {}
        with EXAMPLE_PATH.open() as file:
            res = _process_file(file, grounder, orcid_to_wikidata, orcid_to_wikimedia_commons)

        self.assertTrue(
            any(
                work.pubmed == "36151740"
                and work.title
                == "A review of biomedical datasets relating to drug discovery: "
                "a knowledge graph perspective"
                for work in res.works
            ),
            msg=f"works:\n\n{res.works}",
        )
