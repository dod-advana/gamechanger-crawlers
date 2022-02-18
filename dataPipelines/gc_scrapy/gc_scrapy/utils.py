# -*- coding: utf-8 -*-
"""
gc_crawler.utils
-----------------
Various gc_crawler util functions/classes used in other modules
"""
from pathlib import Path
from typing import Union, List, Any, Dict, Generator, Iterable
import zipfile
import copy
import tempfile
import shutil
from hashlib import sha256
from functools import reduce
from urllib.parse import urljoin, urlparse
import re
import os


def str_to_sha256_hex_digest(_str: str) -> str:
    """Converts string to sha256 hex digest"""
    if not _str and not isinstance(_str, str):
        raise ValueError("Arg should be a non-empty string")

    return sha256(_str.encode("utf-8")).hexdigest()


def dict_to_sha256_hex_digest(_dict: Dict[Any, Any]) -> str:
    """Converts dictionary to sha256 hex digest.

      Sensitive to changes in presence and string value of any k/v pairs.
    """
    if not _dict and not isinstance(_dict, dict):
        raise ValueError("Arg should be a non-empty dictionary")

    # order dict k/v pairs & concat their values as strings
    value_string = reduce(
        lambda t1, t2: "".join(map(str, (t1, t2))),
        sorted(_dict.items(), key=lambda t: str(t[0])),
        "",
    )

    return str_to_sha256_hex_digest(value_string)


def is_valid_web_url(url_string: str) -> bool:
    """Checks if given string is a valid URI"""
    try:
        result = urlparse(url_string)
        # true if ...
        return all(
            [
                # only certain schemes
                result.scheme in ['http', 'https'],
                # fqdn without any spaces
                result.netloc and not re.findall(r"\s", result.netloc),
                # path without any spaces
                not re.findall(r"\s", result.path or ""),
            ]
        )
    except AttributeError:
        return False


def abs_url(base_url: str, target_url: str) -> str:
    """returns absolute url given base and relative target"""
    return urljoin(base_url, target_url)


def get_fqdn_from_web_url(url_string: str) -> str:
    """Parses out just the FQDN from the url"""
    return urlparse(url_string).netloc

def get_available_path(desired_path: Union[str, Path]) -> Path:
    """Given desired path, returns one that uses desired path as prefix but won't overwrite existing files
    :param desired_path: proposed file/dir path
    :returns: available file/dir path
    """
    original_path = Path(desired_path)
    base_dir = Path(original_path).parent
    base_ext = original_path.suffix
    base_name = original_path.name[
        : (-len(base_ext) if original_path.is_file() else None)
    ]

    if not base_dir.is_dir():
        raise ValueError(f"Base dir for path does not exist: {base_dir.absolute()}")

    def suffix_generator() -> Generator[str, None, None]:
        base = "dup"
        counter = 1
        while True:
            yield f"{base}{counter}"
            counter += 1

    path_candidate = Path(desired_path)
    suffixes = suffix_generator()

    _sanity_check_limit = (
        100_000  # to avoid infinite loops if there are issues with file cleanup
    )
    while True:
        if path_candidate.exists():
            new_suffix = next(suffixes)
            new_filename = f"{base_name}_{new_suffix}{base_ext}"
            path_candidate = Path(base_dir, new_filename)
        else:
            break
        if _sanity_check_limit <= 0:
            raise RuntimeError(
                "File name generator exceeded sensible number of retries."
            )
        _sanity_check_limit -= 1

    return path_candidate.resolve()


def iter_all_files(dir_path: Union[Path, str], recursive: bool = True) -> Iterable[Path]:
    """Iterate over all files in dir tree
    :param dir_path: path to directory where the files are located
    :param recursive: whether to return files for entire dir tree

    :returns: iterable of file pathlib.Path objects in the dir tree
    """
    _dir_path = Path(dir_path)
    if not _dir_path.is_dir():
        raise ValueError(f"Got invalid dir_path: {_dir_path}")

    if recursive:
        for file in filter(lambda p: p.is_file(), _dir_path.rglob("*")):
            yield file
    else:
        for file in filter(lambda p: p.is_file(), _dir_path.glob("*")):
            yield file


def unzip_all(zip_file: Union[Path, str], output_dir: str) -> List[Path]:
    """ Unzip all items in the input file and place them inside output_dir
    :param zip_file: path to zip file
    :param output_dir: path to desired output directory

    :return: flat list of files unzipped to the output_dir
    """

    input_file_path = Path(zip_file).resolve()
    output_dir_path = Path(output_dir).resolve()

    if not input_file_path.is_file():
        raise ValueError(f"Given zip_file is invalid: {zip_file}")
    if not output_dir_path.is_dir():
        raise ValueError(f"Given output_dir is invalid: {output_dir}")

    if any(output_dir_path.iterdir()):
        output_dir_path = get_available_path(output_dir_path)

    def unzip_nested(zip_path: Path, dir_path: Path) -> None:
        with zipfile.ZipFile(Path(zip_path).absolute()) as zip_ref:
            zip_ref.extractall(dir_path)
        for path in iter_all_files(dir_path):
            if path.suffix == ".zip":
                new_output_dir = Path(get_available_path(Path(dir_path, "tmp_unzip")))
                new_output_dir.mkdir()
                unzip_nested(path.absolute(), new_output_dir.absolute())
                path.unlink()

    unzip_nested(input_file_path, output_dir_path)
    unzipped_file_paths = list(iter_all_files(output_dir_path))

    return unzipped_file_paths

def safe_move_file(file_path: Union[Path, str], output_path: Union[Path, str], copy: bool = False) -> Path:
    """Safely moves/copies file to given directory
    by changing file suffix (sans extension) to avoid collisions, if necessary

    :param file_path: Source file, must exist
    :param output_path: Destination directory, must exist
    :param copy: Flag to perform copy instead of move
    :return: Path to moved/copied file location
    """
    _file_path = Path(file_path).resolve()
    _output_path = Path(output_path).resolve()

    desired_path = Path(_output_path, _file_path.name) if _output_path.is_dir() else _output_path
    available_dest_path = Path(get_available_path(desired_path))

    if not _file_path.is_file():
        raise ValueError(f"Given path is not a file: {_file_path!s}")
    if (not available_dest_path.parent.is_dir()) or (available_dest_path.is_file() and available_dest_path.exists()):
        raise ValueError(f"Given path parent is not a directory or is a file that already exists: {available_dest_path!s}")

    if copy:
        shutil.copy(_file_path, available_dest_path)  # type: ignore
    else:
        shutil.move(_file_path, available_dest_path)  # type: ignore

    return available_dest_path

def unzip_docs_as_needed(input_dir: Union[Path, str], output_dir: Union[Path, str], doc_type: str) -> List[Path]:
    """Handles zipped/packaged download artifacts by expanding them into their individual components

    :param input_dir: Path of the zip file
    :param output_dir: Directory where files, unzipped or not, should be placed
    :param doc_type: Document file type, e.g. "pdf", "html", "txt"
    :return: iterable of Downloaded documents, len > 1 for bundles
    """

    # TODO: create set of recursive unzip methods for other archive types and a dispatcher
    try:
        # unzip & move
        temp_dir = tempfile.TemporaryDirectory()
        unzipped_files = unzip_all(zip_file=input_dir, output_dir=temp_dir.name)
        unzipped_files = [f for f in unzipped_files if f.suffix.lower()[1:] == doc_type]
        if not unzipped_files:
            raise RuntimeError(f"Tried to unzip {input_dir}, but could not find any expected files inside")
        final_ddocs = []

        # TODO: Add capibility to unzip multiple zips and add corresponding metadata for each
        # do just the first iteration to unzip only the first file
        unzipped_files.sort()   # messy solution. sorting to make sure we grab the first in us_code
        for pdf_file in [unzipped_files[0]]:
            new_ddoc = copy.deepcopy(input_dir)
            safe_move_file(file_path=pdf_file, output_path=output_dir)
            final_ddocs.append(new_ddoc)
    finally:
        temp_dir.cleanup()

        # remove zip. check in case a bad input was put in
        if input_dir.is_file() and input_dir.suffix.lower() == ".zip":
            os.remove(input_dir)

    return final_ddocs
