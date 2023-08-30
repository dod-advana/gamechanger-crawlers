import json
from pathlib import Path
import shutil
from typing import Dict, Iterable
import pytest
from dataPipelines.gc_scrapy.cli import resolve_spider
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from scrapy.utils.project import get_project_settings

from tests.test_spiders import (
    TEST_SPIDER_OUTPUT_TMP as _TEST_SPIDER_OUTPUT_TMP,
    TEST_RESOURCES_SPIDERS_ROOT as _TEST_RESOURCES_SPIDERS_ROOT,
    TEST_SPIDER_OUTPUT_FILENAME as _TEST_SPIDER_OUTPUT_FILENAME,
    TEST_SPIDER_PREV_MANIFEST_FILENAME as _TEST_SPIDER_PREV_MANIFEST_FILENAME,
)
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor

# This is a temporary thing for testing the outputs of the crawlers
def read_semi_json_file(file_path: Path) -> Iterable[Dict[str, str]]:
    """Iterate over input semi-json outputs from given file path"""
    with file_path.open(mode="r") as f:
        for json_str in f.readlines():
            if not json_str.strip():
                continue
            try:
                doc = dict(json.loads(json_str))
                yield doc
            except json.decoder.JSONDecodeError:
                print("Encountered JSON decode error while parsing crawler output.")
                continue


@pytest.mark.parametrize("spider_class", ["us_code_spider"])
def test_us_code_spider_metadata(spider_class: GCSpider):

    spider_path = f"dataPipelines.gc_scrapy.gc_scrapy.spiders.{spider_class}"
    spider_class = resolve_spider(spider_path)
    assert spider_class is not None
    spider_class.test_title_42 = True

    download_output_dir: Path = _TEST_SPIDER_OUTPUT_TMP / spider_class.name
    spider_resources_dir: Path = _TEST_RESOURCES_SPIDERS_ROOT / f"{spider_class.name}_42"

    if download_output_dir.exists():
        shutil.rmtree(download_output_dir)
    download_output_dir.mkdir(exist_ok=False)

    prev_manifest_file = _TEST_RESOURCES_SPIDERS_ROOT / _TEST_SPIDER_PREV_MANIFEST_FILENAME
    previous_manifest_location = download_output_dir / _TEST_SPIDER_PREV_MANIFEST_FILENAME
    shutil.copyfile(prev_manifest_file, previous_manifest_location)

    settings = get_project_settings()
    crawler_output_location = download_output_dir / _TEST_SPIDER_OUTPUT_FILENAME
    settings.set("FEED_URI", crawler_output_location)

    runner = CrawlerRunner(settings)

    crawl_kwargs = {
        "download_output_dir": download_output_dir,
        "previous_manifest_location": previous_manifest_location,
        "dont_filter_previous_hashes": False,
        "output": crawler_output_location,
    }

    d = runner.crawl(spider_class, **crawl_kwargs)
    d.addBoth(lambda _: reactor.stop())
    reactor.run()  # the script will block here until the crawling is finished

    metadata_files = list((spider_resources_dir / "metadata").iterdir())
    metadata_files.sort()
    output_metadata_files = list((download_output_dir).glob("*.metadata"))
    output_metadata_files.sort()

    assert len(metadata_files) == len(output_metadata_files)

    for true_metadata_file, new_metadata_file in zip(metadata_files, output_metadata_files):
        true_item = next(read_semi_json_file(true_metadata_file))
        new_item = next(read_semi_json_file(new_metadata_file))
        # check if keys are the same
        assert list(true_item.keys()) == list(new_item.keys())

        # check doc_type
        assert true_item["doc_type"] == new_item["doc_type"]
        assert true_item["cac_login_required"] == new_item["cac_login_required"]
        assert true_item["publication_date"] == new_item["publication_date"]
        assert true_item["source_fqdn"] == new_item["source_fqdn"]
        assert true_item["source_page_url"] == new_item["source_page_url"]
        assert true_item["crawler_used"] == new_item["crawler_used"]
        # assert true_item["version_hash_raw_data"]["item_currency"] == new_item["version_hash_raw_data"]["item_currency"]
        assert true_item["version_hash_raw_data"]["doc_name"] == new_item["version_hash_raw_data"]["doc_name"]
        assert true_item["doc_name"] == new_item["doc_name"]
        assert true_item["doc_num"] == new_item["doc_num"]
        assert true_item["doc_title"] == new_item["doc_title"]

    output_objects = {
        item["doc_num"]: item for item in read_semi_json_file(spider_resources_dir / _TEST_SPIDER_OUTPUT_FILENAME)
    }

    new_output_objects = {
        item["doc_num"]: item for item in read_semi_json_file(download_output_dir / _TEST_SPIDER_OUTPUT_FILENAME)
    }

    for index in range(1, len(output_objects) + 1):
        if index == 53:
            continue
        true_item = output_objects[str(index)]
        new_item = new_output_objects[str(index)]
        # check if keys are the same
        assert list(true_item.keys()) == list(new_item.keys())

        # check doc_type
        assert true_item["doc_type"] == new_item["doc_type"]
        assert true_item["cac_login_required"] == new_item["cac_login_required"]
        assert true_item["publication_date"] == new_item["publication_date"]
        assert true_item["source_fqdn"] == new_item["source_fqdn"]
        assert true_item["source_page_url"] == new_item["source_page_url"]
        assert true_item["crawler_used"] == new_item["crawler_used"]
        assert true_item["version_hash_raw_data"]["item_currency"] == new_item["version_hash_raw_data"]["item_currency"]
        assert true_item["version_hash_raw_data"]["doc_name"] == new_item["version_hash_raw_data"]["doc_name"]
        assert true_item["doc_name"] == new_item["doc_name"]
        assert true_item["doc_num"] == new_item["doc_num"]
        assert true_item["doc_title"] == new_item["doc_title"]
