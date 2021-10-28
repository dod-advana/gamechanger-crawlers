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

## How to Setup Local Env for Development
> The following should be done in a MacOS or Linux environment (including WSL on Windows)
1. Install Google Chrome and ChromeDriver
    - https://chromedriver.chromium.org/getting-started
    - after a successful installation you should be able to run the following from the shell:
         ```shell
         chromedriver --version
         ```
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
     pip install -r ./docker/minimal-requirements.txt
     ```
6. That's it.
