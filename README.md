<img src="./img/tags/GAMECHANGER-NoPentagon_RGB@3x.png" align="right"
     alt="Mission Vision Icons" width="300" >
# Introduction

Over 15 thousand documents govern how the Department of Defense (DoD) operates. The documents exist in different repositories, often exist on different networks, are discoverable to different communities, are updated independently, and evolve rapidly. No single ability has ever existed that would enable navigation of the vast universe of governing requirements and guidance documents, leaving the Department unable to make evidence-based, data-driven decisions. Today GAMECHANGER offers a scalable solution with an authoritative corpus comprising a single trusted repository of all statutory and policy driven requirements based on Artificial-Intelligence (AI) enabled technologies.

#
<img src="./img/original/Brand_Platform.png" align="right"
     alt="Mission Vision Icons" width="320" >

### Vision

Fundamentally changing the way in which the DoD navigates its universe of requirements and makes decisions

### Mission
GAMECHANGER aspires to be the Department’s trusted solution for evidence-based, data-driven decision-making across the universe of DoD requirements by:

- Building the DoD’s authoritative corpus of requirements and policy to drive search, discovery, understanding, and analytic capabilities
- Operationalizing cutting-edge technologies, algorithms, models and interfaces to automate and scale the solution
- Fusing best practices from industry, academia, and government to advance innovation and research
- Engaging the open-source community to build generalizable and replicable technology

## License & Contributions
See LICENSE.md (including licensing intent - INTENT.md) and CONTRIBUTING.md

### Difference In Branch

As time goes on, installation and requirements change throughout the company; this was created to be tailored specifically to Booz Allen Hamilton (BAH) employee's local set up. We have specific limitations that prevent us from following the steps in the dev branch. 
Updated as of: 12 Jul 2023


## How to Setup Local Env for Development (MacOS or Linux)
1. Install Google Chrome Enterprise and ChromeDriver
    - https://chromeenterprise.google/browser/download/#windows-tab
    - https://chromedriver.chromium.org/getting-started
    - after a successful installation you should be able to run the following from the shell:
         ```shell
         chromedriver --version
         ```
     - - changed --> Google Chrome (GC) Ent. is the company default; regular GC installation is prohibited and not able to be installed on a BAH machine as of recent.
2. Install Miniconda or Anaconda (Miniconda is much smaller)
    - https://docs.conda.io/en/latest/miniconda.html
    - after a successful installation you should be able to run the following from the shell:
         ```shell
         conda --version
         ```
3. Create a gamechanger crawlers python3.6 environment:
     ```shell
     conda create -n gc-crawlers python=3.6
     ```
4. Clone the repo and change into that dir:
     ```shell
     git clone https://github.com/dod-advana/gamechanger-crawlers.git
     cd gamechanger-crawlers
     ```
5. Activate the conda environment and install requirements:
     ```shell
     conda activate gc-crawlers
     pip install --upgrade pip setuptools wheel
     pip install -r ./docker/core/minimal-requirements.txt
     ```
6. That's it.


## Quickstart Guide: Running a Crawler (MacOS or Linux)
1. Follow the environment setup guide above if you have not already
2. Change to the gamechanger crawlers directory and export the repository path to the PYTHONPATH environment variable:
     ```shell
     cd /path/to/gamechanger-crawlers
     export PYTHONPATH="$(pwd)"
     ```
3. Create an empty directory for the crawler file outputs:
     ```shell
     CRAWLER_DATA_ROOT=/path/to/download/location
     mkdir -p "$CRAWLER_DATA_ROOT"
     ```
4. Create an empty previous manifest file:
     ```shell
     touch "$CRAWLER_DATA_ROOT/prev-manifest.json"
     ```
5. Run the desired crawler spider from the `gamechanger-crawlers` directory (in this example we will use the `executive_orders_spider.py`):
     ```shell
     scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/executive_orders_spider.py \
       -a download_output_dir="$CRAWLER_DATA_ROOT" \
       -a previous_manifest_location="$CRAWLER_DATA_ROOT/prev-manifest.json" \
       -o "$CRAWLER_DATA_ROOT/output.json"
     ```
6. After the crawler finishes running, you should have all files downloaded into the crawler output directory

## Quickstart Guide: Running a Crawler (Windows OS)
1. Follow the environment setup guide above if you have not already
2. Change to the gamechanger crawlers directory and export the repository path to the PYTHONPATH environment variable:
     ```shell
     cd /path/to/gamechanger-crawlers
     $export PYTHONPATH="$(pwd)"
     ```
3. Create an empty directory for the crawler file outputs:
     ```shell
     $CRAWLER_DATA_ROOT= "./path/to/download/location"
     New-Item -ItemType Directory -Force -Path $CRAWLER_DATA_ROOT| Out-Null
     ```
4. Create an empty previous manifest file:
     ```shell
     $prevManifestFile = Join-Path $CRAWLER_DATA_ROOT "pre-manifest.json"
     New-Item -ItemType File -Force -Path $prevManifestFile | Out-Null
     ```
5. Run the desired crawler spider from the `gamechanger-crawlers` directory (in this example we will use the `executive_orders_spider.py`):
     ```shell
     $executiveOrdersSpider = "dataPipelines/gc_scrapy/gc_scrapy/spiders/executive_orders_spider.py"
     $outputFile = Join-Path $CRAWLER_DATA_ROOT "output.json"
     scrapy runspider $executiveOrdersSpider `
>>   -a download_output_dir=$CRAWLER_DATA_ROOT `
>>   -a previous_manifest_location=$prevManifestFile `
>>   -o $outputFile
     ```
6. After the crawler finishes running, you should have all files downloaded into the crawler output directory
