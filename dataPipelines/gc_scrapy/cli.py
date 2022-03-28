import os
import copy
from tempfile import NamedTemporaryFile, TemporaryDirectory, mkdtemp
from pathlib import Path
import importlib
import typing as t
import logging

import click
from textwrap import dedent

from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.spider import iter_spider_classes

from notification import slack

from dataPipelines.utils import get_git_branch
from twisted.internet import reactor, defer

####
# CLI to run scrapy crawlers
####
SPIDER_MODULE_PREFIX = "gc_scrapy.spiders"
FULL_MODULE_PREFIX = f"dataPipelines.gc_scrapy.{SPIDER_MODULE_PREFIX}"
FULL_MODULE_PATH = os.path.join(*FULL_MODULE_PREFIX.split("."))

logger = logging.getLogger()


@click.group(context_settings=dict(max_content_width=120))
def cli():
    """gc_scrapy CLI"""
    pass


def list_spiders(ctx, param, value):
    if value:
        for spider in get_spider_class_references():
            print(spider)
        ctx.exit()


default_output_path = Path(os.getcwd(), "output")
default_crawler_output_path = Path(default_output_path, "crawler-output.json")


@cli.command(name="crawl")
@click.option(
    "--download-output-dir",
    help="Directory to store crawled and downloaded files.",
    type=click.Path(
        exists=False,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
        allow_dash=False,
    ),
    default=default_output_path,
    required=False,
)
@click.option(
    "--crawler-output-location",
    help="File location for crawler output to be stored post-spider execution.",
    type=click.Path(exists=False, file_okay=True, dir_okay=False, resolve_path=True),
    default=default_crawler_output_path,
    required=False,
)
@click.option(
    "--previous-manifest-location",
    help="File location of previous manifest.",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, resolve_path=True),
    default=None,
    required=False,
)
@click.option(
    "--list-spiders",
    help="Print full path of each configured spider and exit.",
    default=False,
    required=False,
    is_flag=True,
    expose_value=False,
    callback=list_spiders,
)
@click.option(
    "--run-spider",
    "-s",
    "spiders",
    help="Specify spider(s) to run by name. Will concatenate these to those in --spiders-file-location, if given.",
    multiple=True,
    required=False,
)
@click.option(
    "--spiders-file-location",
    help="File location with spider filenames to run, one per line. Defaults to using all spiders in module.",
    type=click.Path(exists=False, file_okay=True, dir_okay=False, resolve_path=True),
    default=None,
    required=False,
)
@click.option(
    "--dont-filter-previous-hashes",
    help="Flag to skip filtering of downloads.",
    default=False,
    required=False,
    type=click.BOOL,
)
@click.confirmation_option(
    "--skip-confirmation",
    help="Start crawling without confirmation prompt.",
    prompt="Do you want to start crawling and downloading data with configured spiders?",
)
def crawl(
    download_output_dir: str,
    crawler_output_location: t.Union[str, None],
    previous_manifest_location: t.Union[str, None],
    spiders: t.Union[list, None],
    spiders_file_location: t.Union[str, None],
    dont_filter_previous_hashes,
):
    """Execute given spider(s) and store metadata for downloading and cataloging."""

    # perform logic for creating any missing directories/paths here
    download_output_path = Path(download_output_dir)
    if not download_output_path.exists():
        try:
            logger.debug(
                f"Attempting to create download output directory {download_output_path.resolve()}"
            )
            download_output_path.mkdir(parents=True)
        except OSError as ose:
            logger.error(f"Unable to create {download_output_path.resolve()}")
            raise ose
    residual_manifest_path = Path(download_output_dir, "manifest.json")

    if residual_manifest_path.exists():
        new_manifest_path = Path(download_output_dir, "previous-manifest.json")
        logger.debug(
            "Encountered previously-created manifest in output directory; moving it prior to starting crawl."
        )
        logger.debug(
            f"{residual_manifest_path.resolve()} -> {new_manifest_path.resolve()}"
        )
        os.rename(residual_manifest_path.resolve(), new_manifest_path.resolve)
        previous_manifest_location = residual_manifest_path.resolve()

    logger.debug(
        dedent(
            f"""
    -- ARGS/VARS --
    download_output_dir={download_output_dir}
    crawler_output_location={crawler_output_location}
    previous_manifest_location={previous_manifest_location}
    spiders_file_location={spiders_file_location}
    dont_filter_previous_hashes={dont_filter_previous_hashes}
    additional spiders={', '.join(spiders) if spiders else "None"}
    """
        )
    )
    logger.debug("Attempting to resolve spiders.")
    spider_class_refs = get_spider_class_references(spiders_file_location, spiders)

    if not spider_class_refs:
        if spiders_file_location:
            raise RuntimeError(
                f"NO SPIDERS FOUND FROM {spiders_file_location}... EXITING"
            )
        else:
            raise RuntimeError("NO SPIDERS CONFIGURED TO RUN ... EXITING")
    logger.debug(
        f"Done resolving spiders, will run [{len(spider_class_refs)}] spider(s)."
    )

    settings = get_project_settings()
    settings.set("FEED_URI", crawler_output_location)
    runner = CrawlerRunner(settings)

    crawl_kwargs = {
        "download_output_dir": download_output_dir,
        "previous_manifest_location": previous_manifest_location,
        "dont_filter_previous_hashes": dont_filter_previous_hashes,
        "output": crawler_output_location,
    }

    try:
        queue_spiders_sequentially(runner, spider_class_refs, crawl_kwargs)
        reactor.run()
        all_stats = copy.deepcopy(spider_class_refs[0].stats)
        send_stats(all_stats)
    except Exception as e:
        logger.error(f"ERROR RUNNING SPIDERS SEQUENTIALLY: {e}")


def get_spider_class_references(
    spiders_file_location: str = None, spiders: list = []
) -> list:
    """Get all spiders from module, or from a given (optional) spider file."""
    if spiders_file_location:
        spiders += [
            spider_spec.strip()
            for spider_spec in open(Path(spiders_file_location)).readlines()
        ]
    elif not spiders:
        spiders = [
            f.name
            for f in Path(FULL_MODULE_PATH).iterdir()
            if f.is_file() and not f.name.startswith("_")
        ]
    spider_class_refs = []
    for spider_module_name in spiders:
        try:
            spider_path = f"{FULL_MODULE_PREFIX}.{spider_module_name}"
            spider_class = resolve_spider(spider_path)
            if not spider_class:
                logger.info(f"Failed to resolve spider from {spider_path}, skipping")
                continue
            spider_class_refs.append(spider_class)
        except Exception as e:
            logger.info(e)
            logger.info(f"Error running spider at path{spider_path}")
            raise e
    return spider_class_refs


def send_stats(all_stats: dict) -> None:
    branch = get_git_branch()
    msg = f"[STATS] Crawler ran on branch: {branch}"

    for spider_name, stats in all_stats.items():
        msg += f"\n {spider_name}"
        for k, v in stats.items():
            msg += f"\n        {k}: {v}"

    try:
        slack.send_notification(message=msg)
    except Exception as e:
        logger.info(f"Slack send error {e}")


@defer.inlineCallbacks
def queue_spiders_sequentially(
    runner: CrawlerRunner, spiders: list, crawl_kwargs: dict
) -> None:
    """
    Args:
        runner: CrawlerRunner instance
        spiders: list of spider class references to run
        crawl_kwards: dict of args to pass CrawlerRunner
    """

    try:
        for spider in spiders:
            try:
                yield runner.crawl(spider, **crawl_kwargs)
            except Exception as e:
                logger.error(f"ERROR RUNNING SPIDER CLASS: {spider}")
                logger.error(e)
    finally:
        logger.info("Done running spiders, stopping twisted.reactor and sending stats")
        try:
            reactor.stop()
        except Exception as e:
            logger.info(e)
            exit(1)


def resolve_spider(spider_path):
    """
    Args:
        spider_path: path of spider to run

    Returns:
        spider class to run
    """

    if SPIDER_MODULE_PREFIX not in spider_path:
        raise Exception(
            f'"{spider_path}" not recognized as valid scrapy spider path. Spiders must come from within "{SPIDER_MODULE_PREFIX}" module.'
        )
    try:
        module_path = spider_path.strip().replace(".py", "")
        spider_module = importlib.import_module(module_path)
        spider = next(iter_spider_classes(spider_module))
        return spider

    except Exception as e:
        logger.error(f"Error getting spider to run: {e}")
        logger.error(f"Skipping {spider_path} because of error")
