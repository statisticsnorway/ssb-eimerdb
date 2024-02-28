"""Command-line interface."""

import click


@click.command()
@click.version_option()
def main() -> None:
    """EimerDB."""


if __name__ == "__main__":
    main(prog_name="ssb-eimerdb")  # pragma: no cover
