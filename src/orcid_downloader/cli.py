"""A command line interface for orcid-downloader."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import click
from more_click import verbose_option

if TYPE_CHECKING:
    from .api import VersionInfo

__all__ = ["main"]


@click.group()
def main() -> None:
    """Run the orcid-downloader CLI."""


def _get_test_version_info() -> VersionInfo:
    from .api import VERSION_DEFAULT, VersionInfo

    return VersionInfo(
        version=VERSION_DEFAULT.version,
        url=VERSION_DEFAULT.url,
        fname=VERSION_DEFAULT.fname,
        size=VERSION_DEFAULT.size,
        output_directory_name="output-test",
    )


@main.command()
@click.option("--test", is_flag=True)
@click.option("--ror-version")
@click.option("-f", "--force", is_flag=True)
@verbose_option
def cache(test: bool, ror_version: str | None, force: bool) -> None:
    """Process ORCID."""
    import sys

    from .api import (
        VERSION_DEFAULT,
        _get_output_module,
        ground_researcher,
        iter_records,
        write_schema,
        write_summaries,
    )

    if test:
        from ssslm.ner import EmptyGrounder

        list(
            iter_records(
                force=True,
                version_info=_get_test_version_info(),
                head=10_000,
                ror_grounder=EmptyGrounder(),
                orcid_to_wikidata={},
                orcid_to_wikimedia_commons={},
            )
        )
        sys.exit(0)

    if ror_version is None:
        from ror_downloader import get_version_info

        click.echo("Looking up ROR version")
        ror_version_info = get_version_info(download=False, authenticate_zenodo=False)
        ror_version = ror_version_info.version

    from .ror import get_ror_grounder

    click.echo(f"Getting ROR v{ror_version}")
    ror_grounder = get_ror_grounder(version=ror_version)
    version_info = VERSION_DEFAULT

    click.echo(f"Using ORCiD version: {version_info}")

    from .lexical import write_lexical, write_lexical_sqlite
    from .owl import write_owl_rdf
    from .sqldb import write_sqlite

    schema_path = _get_output_module(version_info).join(name="schema.json")
    click.echo(f"Writing schema to {schema_path}")
    write_schema(schema_path)

    # only force on the first one
    click.echo("Writing summaries")
    write_summaries(version_info=version_info, force=force, ror_grounder=ror_grounder)

    click.echo("Writing SQLite")
    write_sqlite(version_info=version_info, force=False, ror_grounder=ror_grounder)

    click.echo("Writing OWL")
    write_owl_rdf(
        version_info=version_info, force=False, ror_grounder=ror_grounder, ror_version=ror_version
    )

    click.echo("Generating SSSLM TSV (~30 min)")
    write_lexical(version_info=version_info, force=False, ror_grounder=ror_grounder)

    click.echo("Generating SQLite lexical index (~30 min)")
    write_lexical_sqlite(version_info=version_info, force=False, ror_grounder=ror_grounder)

    # Test grounding
    x = time.time()
    res = ground_researcher("CT Hoyt", version_info=version_info)
    delta = time.time() - x
    click.echo(f"Grounded in {delta:.2f} seconds:\n\n{res!r}")


@main.command()
def ground() -> None:
    """Ground a researcher."""
    from .api import ground_researcher

    version_info = _get_test_version_info()
    res = ground_researcher("Luana Licata", version_info=version_info)
    click.echo(f"res: {res}")


if __name__ == "__main__":
    main()
