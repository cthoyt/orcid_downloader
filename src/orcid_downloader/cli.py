"""A command line interface for orcid-downloader."""

import time

import click

from orcid_downloader.api import (
    VERSION_DEFAULT,
    VersionInfo,
    _get_output_module,
    ground_researcher,
    iter_records,
    write_schema,
    write_summaries,
)

__all__ = ["main"]


@click.command()
@click.option("--test", is_flag=True)
def main(test: bool) -> None:
    """Process ORCID."""
    if test:
        version_info = VersionInfo(
            version=VERSION_DEFAULT.version,
            url=VERSION_DEFAULT.url,
            fname=VERSION_DEFAULT.fname,
            size=VERSION_DEFAULT.size,
            output_directory_name="output-test",
        )
        list(iter_records(force=True, version_info=version_info, head=10_000))
    else:
        version_info = VERSION_DEFAULT

    click.echo(f"Using version: {version_info}")

    from .lexical import write_lexical, write_lexical_sqlite
    from .owl import write_owl_rdf
    from .sqldb import write_sqlite

    schema_path = _get_output_module(version_info).join(name="schema.json")
    click.echo(f"Writing schema to {schema_path}")
    write_schema(schema_path)

    click.echo("Writing summaries")
    write_summaries(version_info=version_info, force=not test)

    click.echo("Writing SQLite")
    write_sqlite(version_info=version_info, force=False)

    click.echo("Writing OWL")
    write_owl_rdf(version_info=version_info, force=False)

    click.echo("Generating SSSLM TSV (~30 min)")
    write_lexical(version_info=version_info, force=False)

    click.echo("Generating SQLite lexical index (~30 min)")
    write_lexical_sqlite(version_info=version_info, force=False)

    # Test grounding
    x = time.time()
    res = ground_researcher("CT Hoyt", version_info=version_info)
    delta = time.time() - x
    click.echo(f"Grounded in {delta:.2f} seconds:\n\n{res!r}")


if __name__ == "__main__":
    main()
