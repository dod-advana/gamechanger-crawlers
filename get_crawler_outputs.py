import shutil
import subprocess
from pathlib import Path
from time import time as t

REPO_ROOT = Path(".").resolve()
OUTPUT_ROOT = REPO_ROOT / "testoutput"

DATA_PACKAGE_ROOT = REPO_ROOT / "dataPipelines"
SCRAPY_PACKAGE_ROOT = DATA_PACKAGE_ROOT / "gc_scrapy"
SPIDERS_ROOT = SCRAPY_PACKAGE_ROOT / "gc_scrapy" / "spiders"

print("Deleting previous run's output folder")
if OUTPUT_ROOT.exists():
    shutil.rmtree(OUTPUT_ROOT)
OUTPUT_ROOT.mkdir()


print("Getting all spider types and paths")
spiders_list = [spider_path for spider_path in SPIDERS_ROOT.iterdir() if not spider_path.name.startswith("_")]

print("Beginning runspider loop")
processes = []
for spider in spiders_list:
    # get spider name without extensions
    spider_name = spider.stem
    output_path = OUTPUT_ROOT / spider_name
    # create output folders for spiders
    output_path.mkdir(exist_ok=True, parents=True)
    # create prev_manifest.json file for each of the spiders
    (output_path / "prev_manifest.json").touch(exist_ok=True)
    run_command = f"{str(REPO_ROOT / '.venv/bin/python')} -m scrapy runspider {str(spider)} -a download_output_dir={str(output_path)} -a previous_manifest_location={str(output_path)}/prev_manifest.json -o {str(output_path)}/output.json 2> {str(output_path / (spider_name+'.log'))}"
    print(run_command)
    # process = subprocess.Popen(run_command, shell=True, capture_output=True)
    # processes.append(process)

# output = [p.wait() for p in processes]

# cleanup
print("Deleting PDF files for space saving")
# for pdf_filepath in OUTPUT_ROOT.glob("**/*.pdf"):
#    pdf_filepath.unlink()
