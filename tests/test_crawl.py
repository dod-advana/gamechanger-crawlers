import pytest

from click.testing import CliRunner
from dataPipelines.gc_scrapy.cli import crawl, get_spider_class_references


def create_test_manifest(path="test.json", manifest_data: list = []):
    with open(path, "rw") as f:
        f.writelines(manifest_data)


@pytest.fixture(scope="module")
def runner():
    return CliRunner()


def test_list_spiders(runner):
    """Simple/naive test that counts number of spiders & compares that to number of return lines in crawl output cmd"""
    result = runner.invoke(crawl, ["--list-spiders"])
    number_spiders = len([s for s in get_spider_class_references()]) + 1
    assert result.exit_code == 0
    assert len(result.output.split("\n")) == number_spiders


def test_test():
    pass
