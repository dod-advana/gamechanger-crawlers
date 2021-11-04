import click
from rpa.edl_zip_mover import move_zips


@click.group()
def cli():
    pass


@cli.command()
def move_from_edl():
    move_zips()
