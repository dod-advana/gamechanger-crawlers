from click.testing import CliRunner
from dataPipelines.gc_scrapy.cli import crawl
from dataPipelines.cli import entry_point

# from dataPipelines.cli import entry_point
from dataPipelines.gc_scrapy.gc_scrapy.spiders.us_code_spider import USCodeSpider


def create_test_manifest(path="test.json", manifest_data: list = []):
    with open(path, "rw") as f:
        f.writelines(manifest_data)


def test_list_spiders():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(entry_point, ["crawl", "--list-spiders"])
        print(result)
        assert result.exit_code == 0
        assert result.output == []


def test_test():
    pass
