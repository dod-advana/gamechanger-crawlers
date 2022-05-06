import json
from pathlib import Path
import shutil
import os
from typing import Dict, Iterable
from tests import REPO_ROOT as _REPO_ROOT
from tests import TEST_RESOURCES as _TEST_RESOURCES
import pytest

_TEST_SPIDER_RESOURCES = _TEST_RESOURCES / "spiders"
_TEST_TMP_PATH = _REPO_ROOT / "tmp"


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


@pytest.mark.parametrize(
    "spider_type,manifest_file",
    [("test_us_code_spider", "test_us_code_manifest.json"), ("us_code_spider", "us_code_manifest.json")],
)
def test_spider_manifest_output(spider_type: str, manifest_file: str):
    command = f"{_REPO_ROOT}/.venv/bin/python -m scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/{spider_type}.py -a download_output_dir=tmp -a previous_manifest_location=tmp/prev_manifest.json -o tmp/output.json"
    if _TEST_TMP_PATH.exists():
        shutil.rmtree(_TEST_TMP_PATH)
    _TEST_TMP_PATH.mkdir()
    shutil.copyfile(_TEST_SPIDER_RESOURCES / "prev_manifest.json", _TEST_TMP_PATH / "prev_manifest.json")

    true_manifest_data = sorted(
        list(read_semi_json_file(_TEST_SPIDER_RESOURCES / manifest_file)), key=lambda d: d["doc_name"]
    )

    os.system(command)

    manifest_data = sorted(list(read_semi_json_file(_TEST_TMP_PATH / "manifest.json")), key=lambda d: d["doc_name"])
    for current, true in zip(manifest_data, true_manifest_data):
        _ = current.pop("access_timestamp")
        _ = true.pop("access_timestamp")
        assert current == true
    shutil.rmtree(_TEST_TMP_PATH)
