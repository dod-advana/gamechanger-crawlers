from sqlite3 import Timestamp
from subprocess import run
from os import makedirs
from os.path import isdir
from shutil import rmtree
from itertools import chain

from .services import S3Service
from .environment import Environment
from .utils import (
    EnvironmentArgument,
    VolumeArgument,
    UserArgument,
    HostPaths,
    ContainerPaths,
    S3Paths,
    Timer,
    get_file_handler_paths,
    configure_logger,
)


class CrawlerJob:
    """CrawlerJob class. Executes crawling and uploading.

    Args:
        env (Environment): Environment variables
        s3_service (S3Service): Connection to S3
        logger (logging.Logger)
        timestamp (Timestamp): Timestamp to assign this job

    Attributes:
        env (Environment): Environment variables
        s3_service (S3Service): Connection to S3
        timestamp (Timestamp): Timestamp of this job
        job_name (str): Name of this job (pulled from env)
        host_paths (HostPaths): Job paths relative to the host machine
        logger (logging.Logger)
    """

    def __init__(self, env, s3_service, logger, timestamp):
        self.env = env
        self.job_name = self.env.JOB_NAME
        self.timestamp = timestamp
        self.host_paths = HostPaths(
            env.HOST_JOB_TMP_DIR, self.job_name, self.timestamp.str
        )
        self.logger = logger
        self.s3_service = s3_service

    def crawl_then_upload(self):
        """Main function for this class. Executes crawling and uploading."""
        self._log_progress("STARTING")
        self.logger.info(
            "\n".join(
                [
                    f"DEPLOYMENT ENV is {self.env.DEPLOYMENT_ENV}",
                    f"HOST_JOB_TMP_DIR is {self.env.HOST_JOB_TMP_DIR}",
                    f"JOB_LOG_FILE is {get_file_handler_paths(self.logger)}",
                ]
            )
        )
        timer = Timer("", logger=self.logger)

        with timer:
            self.recreate_job_dir()

            success = self.grab_manifest()
            self._verify_or_exit("grab latest manifest", success)

            self.remove_container()

            success = self.run_docker_subprocess()
            self._verify_or_exit("docker subprocess", success)

            success = self.backup_manifest()
            self._verify_or_exit("backup manifest", success)

            success = self.update_manifest()
            self._verify_or_exit("update manifest", success)

        self._log_progress("FINISHED")

    def run_docker_subprocess(self):
        """Execute the `docker run` command.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        args = list(
            chain.from_iterable([x.split() for x in self.make_docker_args()])
        )

        self.logger.info("Executing `docker run` command.")
        try:
            process = run(args)
        except Exception:
            self.logger.exception(
                "Exception occurred during Docker subprocess."
            )
            return False
        success = True if process.returncode == 0 else False

        return success

    def make_docker_args(self):
        """Create arguments for the `docker run` command.

        Returns:
            list of str
        """
        self.logger.info("Creating Docker arguments.")
        args = [
            f"docker run --name {self.job_name}",
            UserArgument(),
            VolumeArgument(
                self.host_paths.previous_manifest,
                ContainerPaths.PREVIOUS_MANIFEST,
            ),
            VolumeArgument(
                self.host_paths.job_dir, ContainerPaths.DOWNLOAD_DIR
            ),
            VolumeArgument(
                self.env.SPIDER_LIST_FILE, ContainerPaths.SPIDER_LIST_FILE
            ),
            EnvironmentArgument(
                "LOCAL_DOWNLOAD_DIRECTORY_PATH", ContainerPaths.DOWNLOAD_DIR
            ),
            EnvironmentArgument(
                "LOCAL_PREVIOUS_MANIFEST_LOCATION",
                self.host_paths.previous_manifest,
            ),
            EnvironmentArgument(
                "AWS_DEFAULT_REGION", self.env.AWS_DEFAULT_REGION
            ),
            EnvironmentArgument("BUCKET", S3Paths.BUCKET),
            EnvironmentArgument(
                "S3_UPLOAD_BASE_PATH",
                f"{S3Paths.UPLOAD_BASE}/{self.timestamp.iso_format}",
            ),
            EnvironmentArgument("DELETE_AFTER_UPLOAD", "no"),
            EnvironmentArgument("SKIP_S3_UPLOAD", self.env.SKIP_S3_UPLOAD),
            EnvironmentArgument("TEST_RUN", self.env.TEST_RUN),
            EnvironmentArgument(
                "LOCAL_SPIDER_LIST_FILE", ContainerPaths.SPIDER_LIST_FILE
            ),
            self.env.CRAWL_DOWNLOAD_UPLOAD_IMAGE,
        ]

        args = [arg.formatted if type(arg) != str else arg for arg in args]

        return args

    def remove_container(self):
        """Remove the Docker container with the name equal to the object's
        `job_name` attribute.

        Note: If a container does not exist with the job name, an error will
        print.

        Returns:
            None
        """
        run(["docker", "rm", "--force", self.job_name])

    def grab_manifest(self):
        """Grab the cumulative manifest file from S3.

        The file is saved to the path defined in the object's
        `host_paths.previous_manifest` attribute.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        self.logger.info("Grabbing latest manifest.")
        success = self.s3_service.download_file(
            S3Paths.BUCKET,
            S3Paths.CUMULATIVE_MANIFEST_KEY,
            self.host_paths.previous_manifest,
        )

        return success

    def backup_manifest(self):
        """Backup the cumulative manifest file in S3.

        Makes a copy of the cumulative manifest file in S3 and saves it in the
        same bucket with a timestamp added to the file name.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        self.logger.info("Backing up the old manifest.")
        success = self.s3_service.copy(
            S3Paths.BUCKET,
            S3Paths.CUMULATIVE_MANIFEST_KEY,
            S3Paths.BUCKET,
            S3Paths.CUMULATIVE_MANIFEST_KEY.replace(
                ".json", f"{self.timestamp.str}.json"
            ),
        )
        return success

    def update_manifest(self):
        """Upload the latest manifest file to S3.

        Uploads the file from path defined as the object's
        `host_paths.cumulative_manifest` attribute. Saves it to the S3 bucket
        `S3Paths.BUCKET` with key `S3Paths.CUMULATIVE_MANIFEST_KEY`.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        self.logger.info("Uploading latest manifest.")
        success = self.s3_service.upload_file(
            self.host_paths.cumulative_manifest,
            S3Paths.BUCKET,
            S3Paths.CUMULATIVE_MANIFEST_KEY,
        )
        return success

    def recreate_job_dir(self):
        """Make the object's `host_paths.job_dir` an empty directory."""
        d = self.host_paths.job_dir
        if isdir(d):
            rmtree(d)

        makedirs(d)

    def _log_progress(self, progress):
        date = self.timestamp.timestamp.date().strftime("%Y-%m-%d")
        time = self.timestamp.timestamp.time().strftime("%H:%M:%S")
        self.logger.info(
            f"{progress} JOB - {self.job_name}"
            + "\n"
            + f"DATE: {date} "
            + f"TIME: {time}"
        )

    def _verify_or_exit(self, process, success):
        if not success:
            self.logger.error(f"Failed to {process}. Exiting.")
            exit(1)
