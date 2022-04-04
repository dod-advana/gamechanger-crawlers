"""GAMECHANGER-crawler utilities
"""
import os
from os.path import isfile, join as join_path
import logging
from logging.config import dictConfig as load_log_config_dict
import codecs
import subprocess
import yaml

from pathlib import Path

from dataPipelines import PACKAGE_PATH

DEFAULT_LOG_CFG_PATH = join_path(PACKAGE_PATH, "log_cfg.yml")


def setup_logging(log_cfg_path: str = DEFAULT_LOG_CFG_PATH, level: int = logging.INFO):
    if Path(log_cfg_path).exists():
        with codecs.open(log_cfg_path, "r", encoding="utf-8") as fd:
            config = yaml.safe_load(fd)
            str_level = logging.getLevelName(level)
            for _logger in config["loggers"]:
                config["loggers"][_logger]["level"] = str_level
            load_log_config_dict(config)
    elif level:
        logging.basicConfig(level=level)
    return logging.getLogger(__name__)


logger = logging.getLogger(__name__)


def get_git_branch() -> str:
    """
    Get the git branch to be logged.
    Returns:
        str: The git branch name.
    """

    try:
        branch_list = subprocess.check_output(["git", "branch"]).splitlines()
    except (subprocess.CalledProcessError, OSError) as gitError:
        logger.error(f"git error: {gitError}")

    for branch_name in branch_list:
        if "*" in branch_name.decode():
            return branch_name.decode()[2:]
    return "branch-undefined"


def checkdiff(required, testing):
    diff = [x for x in required if x not in testing]
    return diff


def verify_spiders_are_scheduled():
    from dataPipelines import PACKAGE_PATH

    spiders_dir = f"{PACKAGE_PATH}/gc_scrapy/gc_scrapy/spiders"
    spiders_in_dir = [
        f.replace(".py", "")
        for f in os.listdir(spiders_dir)
        if isfile(join_path(spiders_dir, f)) and not f.startswith("_")
    ]

    schedule_dir = f"{PACKAGE_PATH}/../config/crawler_schedule"
    spiders_in_schedule = []
    for f_name in os.listdir(schedule_dir):
        if isfile(join_path(schedule_dir, f_name)) and f_name.endswith(".txt"):
            with open(join_path(schedule_dir, f_name)) as f:
                for line in f.readlines():
                    if line.strip():
                        spiders_in_schedule.append(line.strip().replace(".py", ""))

    unused = checkdiff(spiders_in_dir, spiders_in_schedule)
    if len(unused):
        message = f"ERROR: Spider(s) not used in a schedule: {unused}"
        raise RuntimeError(message)
    else:
        print("All spiders are in a schedule file")


if __name__ == "__main__":
    verify_spiders_are_scheduled()
