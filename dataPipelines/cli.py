import logging

import click
from dataPipelines.gc_downloader.cli import download, add_to_manifest
from dataPipelines.gc_scrapy.cli import crawl
from dataPipelines.scanner import scan
from dataPipelines.utils import LogFormatter

loghandler = logging.StreamHandler()
loghandler.setFormatter(LogFormatter())
logging.basicConfig(handlers=[loghandler], level=logging.DEBUG)


@click.group()
@click.option("--debug/--no-debug", default=False)
def entry_point(debug):
    if debug:
        logging.basicConfig(handlers=[loghandler], level=logging.DEBUG)
        logging.info("DEBUG mode activated.")
    else:
        logging.basicConfig(handlers=[loghandler], level=logging.INFO)
    pass


entry_point.add_command(crawl)
entry_point.add_command(download)
entry_point.add_command(scan)
entry_point.add_command(add_to_manifest)

entry_point(auto_envvar_prefix="GC")
