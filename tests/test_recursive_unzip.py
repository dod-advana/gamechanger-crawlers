import pytest

from pathlib import Path
from typing import Union, List
import zipfile
import copy
from twisted.internet import reactor, defer
import shutil
from dataPipelines.gc_scrapy.gc_scrapy.spiders.us_code_spider import USCodeSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import unzip_docs_as_needed
from tests import TEST_RESOURCES as _TEST_RESOURCES

from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor

_TEST_RESOURCES_ZIP = _TEST_RESOURCES / "zip"
_TEST_RESOURCES_SPIDERS = _TEST_RESOURCES / "spiders"


@pytest.mark.skip
@pytest.mark.parametrize(
    "zip_file,output_dir",
    [
        (
            _TEST_RESOURCES_ZIP / "pdf_usc42@117-114.zip",
            _TEST_RESOURCES_ZIP / "output_dir" / "pdf_usc42@117-114.zip",
        )
    ],
)
def test_unzip_all(zip_file: Union[Path, str], output_dir: Union[Path, str]) -> List[Path]:
    if output_dir.parent.exists():
        shutil.rmtree(output_dir.parent)
    output_dir.parent.mkdir(exist_ok=True)

    shutil.copyfile(zip_file, (input_file_path := _TEST_RESOURCES_ZIP / "input_dir" / zip_file.name))
    unzipped_file_paths = unzip_docs_as_needed(input_dir=input_file_path, output_dir=output_dir, doc_type="pdf")
    unzipped_file_paths.sort()
    true_file_paths = list((_TEST_RESOURCES_ZIP / "correct").glob("**/*.pdf"))
    true_file_paths.sort()
    assert sum(
        [new_file.name == correct_file.name for new_file, correct_file in zip(unzipped_file_paths, true_file_paths)]
    ) == len(unzipped_file_paths)

    shutil.rmtree(output_dir.parent)


@pytest.mark.parametrize(
    "zip_file,output_dir",
    [
        (
            _TEST_RESOURCES_ZIP / "pdf_usc42@117-114.zip",
            _TEST_RESOURCES_ZIP / "output_dir" / "pdf_usc42@117-114.zip",
        )
    ],
)
def test_unzipping(zip_file: Union[Path, str], output_dir: Union[Path, str]) -> List[Path]:

    spider_class = USCodeSpider
    settings = get_project_settings()

    download_output_dir = _TEST_RESOURCES_SPIDERS / spider_class.name

    if download_output_dir.exists():
        shutil.rmtree(download_output_dir)
    download_output_dir.mkdir(exist_ok=True)

    prev_manifest_main = _TEST_RESOURCES_SPIDERS / "prev_manifest.json"
    previous_manifest_location = download_output_dir / "prev_manifest.json"
    shutil.copyfile(prev_manifest_main, previous_manifest_location)

    crawler_output_location = download_output_dir / "outputs.json"
    settings.set("FEED_URI", crawler_output_location)

    runner = CrawlerRunner(settings)

    crawl_kwargs = {
        "download_output_dir": download_output_dir,
        "previous_manifest_location": previous_manifest_location,
        "dont_filter_previous_hashes": False,
        "output": crawler_output_location,
    }

    try:
        output = runner.crawl(USCodeSpider, **crawl_kwargs)
        reactor.run()
    finally:
        reactor.stop()
    # if output_dir.parent.exists():
    #     shutil.rmtree(output_dir.parent)
    # output_dir.parent.mkdir(exist_ok=True)
    # shutil.copyfile(zip_file, (input_file_path := _TEST_RESOURCES_ZIP / "input_dir" / zip_file.name))

    zfile = zipfile.ZipFile(zip_file, mode="r")

    if len(zfile.filelist) > 1:
        pass
    # unzipped_file_paths = unzip_docs_as_needed(input_dir=input_file_path, output_dir=output_dir, doc_type="pdf")
    # unzipped_file_paths.sort()
    # true_file_paths = list((_TEST_RESOURCES_ZIP / "correct").glob("**/*.pdf"))
    # true_file_paths.sort()
    # assert sum(
    #     [new_file.name == correct_file.name for new_file, correct_file in zip(unzipped_file_paths, true_file_paths)]
    # ) == len(unzipped_file_paths)

    # shutil.rmtree(output_dir.parent)
