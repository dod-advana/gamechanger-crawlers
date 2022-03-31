import typing as t
import multiprocessing as mp
import subprocess as sub
from concurrent.futures.process import ProcessPoolExecutor
from pathlib import Path
import logging

import click


logger = logging.getLogger(__name__)


@click.group(context_settings=dict(max_content_width=120))
def cli():

    pass


def scan_file(script_path: t.Union[str, Path], input_path: t.Union[str, Path]) -> bool:
    """

    :param script_path: path to dlp scanner
    :param input_file: path to input file to scan
    """
    try:
        input_path = Path(input_path).resolve()
        script_path = Path(script_path).resolve()
        completed_process = sub.run(["/bin/bash", str(script_path), str(input_path)])
        return completed_process.returncode == 0
    except Exception:
        return False


def _starmap_run_scanner(args):
    scan_file(*args)


def scan_directory(
    script_path: t.Union[str, Path], input_path: t.Union[str, Path], cores: int
):
    """

    :param script_path: path to scanner function
    :param input_path: files or directory of files to scan
    :param cores: num cores for parallel processing

    """
    # parallelize dlp scanner using map; map takes chunk 1 default begins new process when pid is free
    input_path = Path(input_path).resolve()
    script_path = Path(script_path).resolve()
    total_count = len([p for p in input_path.iterdir() if p.is_file()])
    good = 0
    bad = 0

    def report_progress(result):
        nonlocal good
        nonlocal bad
        good += 1 if result else 0
        bad += 1 if not result else 0
        print(f"[JOB INFO] PROGRESS:{good + bad}/{total_count}")

    with ProcessPoolExecutor(max_workers=cores) as pp:
        results = pp.map(
            _starmap_run_scanner,
            (
                (script_path, p)
                for p in input_path.iterdir()
                if p.is_file() and p.name != "manifest.json"
            ),
        )
        for r in results:
            report_progress(r)

    # process manifest json last to signify upload is over
    if Path(input_path, "manifest.json").is_file():
        manifest_result = scan_file(script_path, Path(input_path, "manifest.json"))
        report_progress(manifest_result)


@cli.command(name="scan")
@click.option(
    "--input-path",
    help='Specify directory or file to scan"',
    type=click.Path(
        exists=True, file_okay=True, dir_okay=True, resolve_path=True, allow_dash=False
    ),
    required=True,
)
@click.option(
    "-c",
    "--cores",
    help="Specify number of CPU cores to use",
    default=mp.cpu_count(),
    required=False,
)
@click.option(
    "--scanner-path",
    help="Overwrite path of scanner",
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, resolve_path=True, allow_dash=False
    ),
    required=False,
    default="/app/scripts/dlp-scanner.sh",
)
def scan(input_path, cores, scanner_path):
    """Scan a given directory or path with a given, or default (ClamAV), scanner."""

    if Path(input_path).is_dir():
        scan_directory(scanner_path, input_path, cores)
    else:
        scan_file(scanner_path, input_path)
