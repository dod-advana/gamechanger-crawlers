import click
from textwrap import dedent

from scrapy.crawler import CrawlerRunner
import importlib
import os
from pathlib import Path
from scrapy.utils.project import get_project_settings
from scrapy.utils.spider import iter_spider_classes
from twisted.internet import reactor, defer
from notification import slack
import copy
from pathlib import Path

####
# CLI to run scrapy crawlers
####


@click.group(context_settings=dict(max_content_width=120))
def cli():
    pass


@cli.command(name='crawl')
@click.option(
    '--download-output-dir',
    help='Directory to download files from crawling in to',
    type=click.Path(
        exists=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
        allow_dash=False
    ),
    required=True
)
@click.option(
    '--crawler-output-location',
    help='File location for crawler output to go',
    type=click.Path(
        exists=False,
        file_okay=True,
        dir_okay=False,
        resolve_path=True
    ),
    required=True
)
@click.option(
    '--previous-manifest-location',
    help='File location of previous manifest',
    type=click.Path(
        exists=True,
        file_okay=True,
        dir_okay=False,
        resolve_path=True
    ),
    required=True
)
@click.option(
    '--spiders-file-location',
    help='Location to put the new manifest file',
    type=click.Path(
        exists=False,
        file_okay=True,
        dir_okay=False,
        resolve_path=True
    ),
    default=None,
    required=False
)
@click.option(
    '--dont-filter-previous-hashes',
    help='Flag to skip filtering of downloads',
    default=False,
    required=False,
    type=click.BOOL
)
def crawl(
    download_output_dir,
    crawler_output_location,
    previous_manifest_location,
    spiders_file_location,
    dont_filter_previous_hashes,
):

    print(dedent(f"""
    CRAWLING INITIATED

    -- ARGS/VARS --
    download_output_dir={download_output_dir}
    crawler_output_location={crawler_output_location}
    previous_manifest_location={previous_manifest_location}
    spiders_file_location={spiders_file_location}
    dont_filter_previous_hashes={dont_filter_previous_hashes}
    """))

    current_dir = os.path.dirname(os.path.realpath(__file__))
    spiders_to_run = []
    if spiders_file_location:
        with open(spiders_file_location) as f:
            for line in f.readlines():
                if line.strip():
                    spiders_to_run.append(line.strip())
    else:
        print('No spider file location specified, running everything in')
        spiders_to_run = [
            f.name for f in Path(f'{current_dir}/gc_scrapy/spiders').iterdir()
            if f.is_file() and not f.name.startswith("_")
        ]

    if not spiders_to_run:
        if spiders_file_location:
            raise RuntimeError(
                f'NO SPIDERS FOUND FROM {spiders_file_location}... EXITING')
        else:
            raise RuntimeError('NO SPIDERS FOUND IN SPIDERS DIR... EXITING')

    print('Done resolving spiders, will run', len(spiders_to_run))
    for s in spiders_to_run:
        print(' - ', s)
    print()

    settings = get_project_settings()
    settings.set('FEED_URI', crawler_output_location)
    runner = CrawlerRunner(settings)

    spider_class_refs = []
    for spider_module_name in spiders_to_run:
        try:
            spider_path = f'dataPipelines.gc_scrapy.gc_scrapy.spiders.{spider_module_name}'
            spider_class = resolve_spider(spider_path)
            if not spider_class:
                print(f'Failed to resolve spider from {spider_path}, skipping')
                continue
            spider_class_refs.append(spider_class)
        except Exception as e:
            print(e)
            print('Error running spider at path', spider_path)
            raise e

    crawl_kwargs = {
        'download_output_dir': download_output_dir,
        'previous_manifest_location': previous_manifest_location,
        'dont_filter_previous_hashes': dont_filter_previous_hashes,
        'output': crawler_output_location
    }

    try:
        queue_spiders_sequentially(runner, spider_class_refs, crawl_kwargs)
        reactor.run()
        all_stats = copy.deepcopy(spider_class_refs[0].stats)
        send_stats(all_stats)
    except Exception as e:
        print("ERROR RUNNING SPIDERS SEQUENTIALLY", e)


def get_git_branch() -> str:
    """
    Get the git branch to be logged.
    Returns:
        str: The git branch name.
    """
    import subprocess

    try:
        branch_list = subprocess.check_output(['git', 'branch']).splitlines()
    except (subprocess.CalledProcessError, OSError) as e:
        print("git error: ", e)
        return 'ERROR GETTING BRANCH NAME'

    for branch_name in branch_list:
        if '*' in branch_name.decode():
            return branch_name.decode()[2:]
    else:
        return 'NOT FOUND'


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
        print('Slack send error', e)


@defer.inlineCallbacks
def queue_spiders_sequentially(runner: CrawlerRunner, spiders: list, crawl_kwargs: dict) -> None:
    """
    Args:
        runner: CrawlerRunner instance
        spiders: list of spider class references to run
        crawl_kwards: dict of args to pass CrawlerRunner
    """

    try:
        for spider in spiders:
            try:
                yield runner.crawl(
                    spider,
                    **crawl_kwargs
                )
            except Exception as e:
                print(f'ERROR RUNNING SPIDER CLASS: {spider}')
                print(e)
    finally:
        print("Done running spiders, stopping twisted.reactor and sending stats")
        try:
            reactor.stop()
        except Exception as e:
            print(e)
            exit(1)


def resolve_spider(spider_path):
    """
    Args:
        spider_path: path of spider to run

    Returns:
        spider class to run
    """

    if 'gc_scrapy.spiders' not in spider_path:
        raise Exception(
            'spider_path not recognized, currently parsers must come from within gc_scrapy/spiders')
    try:
        module_path = spider_path.strip().replace('.py', '')
        spider_module = importlib.import_module(module_path)
        spider = next(iter_spider_classes(spider_module))
        return spider

    except Exception as e:
        print('Error getting spider to run:', e)
        print('Skipping', spider_path, 'because of error')
