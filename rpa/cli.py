import click
from rpa.edl_zip_mover import move_from_edl


@click.group()
def cli():
    pass


@cli.command()
def move_from_edl():
    move_from_edl()
