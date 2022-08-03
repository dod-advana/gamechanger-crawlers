"""Run the crawler job and upload results."""

import sys
from os.path import dirname, realpath
from argparse import ArgumentParser

sys.path.insert(0, dirname(dirname(realpath(__file__))))
from src import CrawlerJob, S3Service, Environment, Timestamp, configure_logger


def parse_args():
    """Parse command line arguments for this script.

    These arguments are helpful when running/ testing locally, but are not meant
    to be used in the develop/ production environments.

    Returns:
        argparse.Namespace: Arguments namespace with:
            aws_profile (str or None, optional)
            aws_access_key (str or None, optional)
            aws_secret_key (str or None, optional)
            aws_session_token (str or None, optional)
    """
    parser = ArgumentParser()

    parser.add_argument(
        "--aws-profile", "-p", required=False, dest="aws_profile", type=str
    )
    parser.add_argument(
        "--aws-access-key",
        "-a",
        required=False,
        dest="aws_access_key",
        type=str,
    )
    parser.add_argument(
        "--aws-secret-key",
        "-s",
        required=False,
        dest="aws_secret_key",
        type=str,
    )
    parser.add_argument(
        "--aws-session-token",
        "-t",
        required=False,
        dest="aws_session_token",
        type=str,
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    timestamp = Timestamp()
    env = Environment()
    logger = configure_logger(
        filepath=f"{env.HOST_JOB_TMP_DIR}/logs/gc-crawler-downloader.{timestamp.iso_format}.log"
    )
    s3 = S3Service(
        logger,
        env.AWS_DEFAULT_REGION,
        args.aws_profile,
        args.aws_access_key,
        args.aws_secret_key,
        args.aws_session_token,
    )
    crawler = CrawlerJob(
        env,
        s3,
        logger,
        timestamp,
    )

    crawler.crawl_then_upload()
