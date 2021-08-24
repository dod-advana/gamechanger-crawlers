
import os
from os.path import isfile, join


def checkdiff(required, testing):
    diff = [x for x in required if x not in testing]
    return diff


def verify_spiders_are_scheduled():
    current_file = os.path.realpath(__file__)
    current_dir = os.path.dirname(current_file)

    spiders_dir = f'{current_dir}/dataPipelines/gc_scrapy/gc_scrapy/spiders'
    spiders_in_dir = [f.replace('.py', '') for f in os.listdir(spiders_dir) if isfile(
        join(spiders_dir, f)) and not f.startswith("_")]

    schedule_dir = f"{current_dir}/paasJobs/crawler_schedule"
    spiders_in_schedule = []
    for f_name in os.listdir(schedule_dir):
        if isfile(join(schedule_dir, f_name)) and f_name.endswith('.txt'):
            with open(join(schedule_dir, f_name)) as f:
                for line in f.readlines():
                    if line.strip():
                        spiders_in_schedule.append(
                            line.strip().replace('.py', ''))

    unused = checkdiff(spiders_in_dir, spiders_in_schedule)
    if len(unused):
        msg = f"ERROR: Spider(s) not used in a schedule: {unused}"
        raise RuntimeError(msg)
    else:
        print("All spiders are in a schedule file")


if __name__ == "__main__":
    verify_spiders_are_scheduled()
