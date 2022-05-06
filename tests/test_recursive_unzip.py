import pytest

from pathlib import Path
from typing import Union, List
import zipfile
import copy
import tempfile
import shutil
import os
from dataPipelines.gc_scrapy.gc_scrapy.utils import unzip_docs_as_needed
from tests import TEST_RESOURCES as _TEST_RESOURCES

_TEST_RESOURCES_ZIP = _TEST_RESOURCES / "zip"


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
