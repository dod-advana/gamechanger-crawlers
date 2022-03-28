"""GAMECHANGER-crawler utilities
"""
import os
from os.path import isfile, join
import logging

import subprocess

logger = logging.getLogger()


class LogFormatter(logging.Formatter):

    err_fmt = "ERROR: %(message)s"
    dbg_fmt = "DEBUG: %(module)s: %(lineno)d: %(message)s"
    info_fmt = "%(message)s"

    def format(self, record):

        # Replace the original format with one customized by logging level
        if record.levelno == logging.DEBUG:
            self._fmt = LogFormatter.dbg_fmt

        elif record.levelno == logging.INFO:
            self._fmt = LogFormatter.info_fmt

        elif record.levelno == logging.ERROR:
            self._fmt = LogFormatter.err_fmt

        # Call the original formatter class to do the grunt work
        return logging.Formatter.format(self, record)


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
        if isfile(join(spiders_dir, f)) and not f.startswith("_")
    ]

    schedule_dir = f"{PACKAGE_PATH}/config/crawler_schedule"
    spiders_in_schedule = []
    for f_name in os.listdir(schedule_dir):
        if isfile(join(schedule_dir, f_name)) and f_name.endswith(".txt"):
            with open(join(schedule_dir, f_name)) as f:
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
