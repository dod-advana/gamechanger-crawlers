from .filenames import Filenames


class ContainerPaths:
    """The ContainerPaths class is used to store paths relative to the Docker
    container.

    Attributes:
        TMP_DIR (str)

        DOWNLOAD_DIR (str)

        PREVIOUS_MANIFEST (str): Path to previous manifest file.

        SPIDER_LIST_FILE (str): Path to list of spiders to run.
    """

    TMP_DIR = "/tmp"

    DOWNLOAD_DIR = f"/var/tmp/output"

    PREVIOUS_MANIFEST = f"{TMP_DIR}/{Filenames.PREVIOUS_MANIFEST}"

    SPIDER_LIST_FILE = f"{TMP_DIR}/{Filenames.SPIDERS_TO_RUN}"
