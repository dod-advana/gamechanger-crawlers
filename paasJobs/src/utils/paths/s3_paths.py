from .filenames import Filenames


class S3Paths:
    """Paths relative to S3.

    Attributes:
        BUCKET (str): The main bucket used in S3.

        CUMULATIVE_MANIFEST_KEY (str): S3 Key for the cumulative manifest file.

        UPLOAD_BASE (str)
    """

    BUCKET = "advana-data-zone"

    CUMULATIVE_MANIFEST_KEY = f"bronze/gamechanger/data-pipelines/orchestration/crawlers/{Filenames.CUMULATIVE_MANIFEST}"

    UPLOAD_BASE = "/bronze/gamechanger/external-uploads/crawler-downloader"
