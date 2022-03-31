import click
from dataPipelines.gc_downloader.cli import download, add_to_manifest
from dataPipelines.gc_scrapy.cli import crawl
from dataPipelines.scanner import scan
from dataPipelines.utils import setup_logging
import logging


@click.group()
@click.option("--debug/--no-debug", default=False)
def entry_point(debug):
    if debug:
        logger = setup_logging(level=logging.DEBUG)
        logger.debug("enabled debug mode.")
    else:
        logger = setup_logging()
    pass


entry_point.add_command(crawl)
entry_point.add_command(download)
entry_point.add_command(scan)
entry_point.add_command(add_to_manifest)

entry_point(auto_envvar_prefix="GC")
