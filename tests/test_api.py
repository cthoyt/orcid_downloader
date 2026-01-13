"""Test the API."""

import unittest


class TestAPI(unittest.TestCase):
    """Test the API."""

    def test_grounding_database(self) -> None:
        """Test grounding."""
        try:
            from orcid_downloader.lexical import _get_database_path, get_orcid_grounder
        except ImportError:
            raise unittest.SkipTest("dependencies for lexical tools missing") from None

        if not _get_database_path().is_file():
            raise unittest.SkipTest("database hasn't been pre-cached")

        grounder = get_orcid_grounder()
        match = grounder.get_best_match("Charles Tapley Hoyt")
        self.assertIsNotNone(match)
        self.assertEqual("0000-0003-4423-4370", match.identifier)

        match = grounder.get_best_match("abcdefghijklmnop")
        self.assertIsNone(match)
