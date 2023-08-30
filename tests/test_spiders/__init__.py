from dataPipelines import REPO_ROOT as _REPO_ROOT
from tests import TEST_RESOURCES_ROOT as _TEST_RESOURCES_ROOT

TEST_RESOURCES_SPIDERS_ROOT = _TEST_RESOURCES_ROOT / "spiders"

TEST_SPIDER_OUTPUT_FILENAME = "output.json"
TEST_SPIDER_MANIFEST_FILENAME = "manifest.json"
TEST_SPIDER_PREV_MANIFEST_FILENAME = "prev_manifest.json"

TEST_SPIDER_OUTPUT_TMP = _REPO_ROOT / "tmp"
TEST_SPIDER_OUTPUT_TMP.mkdir(exist_ok=True, parents=True)
