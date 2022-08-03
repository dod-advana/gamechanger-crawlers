from dotenv import load_dotenv, find_dotenv
from sys import exit
from errno import ENOENT
from os import environ

from .environment_type import EnvironmentType
from ..utils import ContainerPaths


class Environment:
    """Environment class.

    This class contains the environment variables for crawler scripts. If the
    environment is inside a Docker container, typically the env file does
    not need to be provided because the command to spin it up (`docker`
    or `docker-compose`) may have provided those variables into the
    container's environment. If the script is running in the host machine,
    then a file will need to be provided or it will use the host machine's
    or virtual environment's environment variables.

    Args:
        env_file (str, optional): Absolute path to the .env file. Default is
            None.

    Attributes:
        DEPLOYMENT_ENV (EnvironmentType): Default is EnvironmentType.PRODUCTION

        AWS_DEFAULT_REGION (str): Depends on DEPLOYMENT_ENV. 
            See get_aws_default_region().

        HOST_JOB_TMP_DIR (str): Default is "/gamechanger/jobs"

        CRAWL_DOWNLOAD_UPLOAD_IMAGE (str): Default is 
            "advana/gc-crawl-download-upload:latest"

        SKIP_S3_UPLOAD (str): Default is "no"

        TEST_RUN (str): Default is "no"

        SPIDER_LIST_FILE (str): Default is ContainerPaths.SPIDER_LIST_FILE

        JOB_NAME (str): Default is "gc_crawl_download_upload"
    """

    def __init__(self, env_file=None):
        try:
            if env_file is not None:
                load_dotenv(find_dotenv(env_file), True)
        except Exception as e:
            print(f"Failed to load .env file. Exception: {e}.")
            exit(ENOENT)

        self.DEPLOYMENT_ENV = self.get_env_var(
            "DEPLOYMENT_ENV", EnvironmentType.PRODUCTION
        )
        self.verify_env_type()

        self.AWS_DEFAULT_REGION = self.get_aws_default_region(
            self.DEPLOYMENT_ENV
        )

        self.HOST_JOB_TMP_DIR = self.get_env_var(
            "HOST_JOB_TMP_DIR", "/gamechanger/jobs"
        )
        self.CRAWL_DOWNLOAD_UPLOAD_IMAGE = self.get_env_var(
            "CRAWL_DOWNLOAD_UPLOAD_IMAGE",
            "advana/gc-crawl-download-upload:latest",
        )
        self.SKIP_S3_UPLOAD = self.get_env_var("SKIP_S3_UPLOAD", "no")
        self.TEST_RUN = self.get_env_var("TEST_RUN", "no")
        self.SPIDER_LIST_FILE = self.get_env_var(
            "LOCAL_SPIDER_LIST_FILE", ContainerPaths.SPIDER_LIST_FILE
        )
        self.JOB_NAME = self.get_env_var(
            "JOB_NAME", "gc_crawl_download_upload"
        )

    def get_env_var(self, name, default):
        """Get an environment variable.

        Args:
            name (str): Name of the environment variable to get.
            default (any): Default value to use if the environment variable
                does not exist.

        Returns:
            any
        """
        try:
            var = environ[name]
        except:
            var = default

        return var

    def verify_env_type(self):
        """Verify that the environment variable `DEPLOYMENT_ENV` is a valid
        EnvironmentType.

        If invalid, exits with code 1.

        Returns:
            None
        """
        is_valid = EnvironmentType.is_valid(self.DEPLOYMENT_ENV)
        if not is_valid:
            print(
                f"`{self.DEPLOYMENT_ENV}` is not a valid environment type. "
                f"Options are: `{EnvironmentType.all()}`."
            )
            exit(1)

    @staticmethod
    def get_aws_default_region(env_type):
        """Returns the AWS Default Region for the given environment type.

        If invalid env_type given, exits with code 1.

        Args:
            env_type (str): The environment type. One of EnvironmentType.all().

        Returns:
            str: AWS Default Region.
        """
        if env_type == EnvironmentType.DEVELOPMENT:
            return "us-east-1"
        elif env_type == EnvironmentType.PRODUCTION:
            return "us-gov-west-1"

        else:
            print(
                f"`{env_type}` is not a valid environment type. Options are: "
                f"`{EnvironmentType.all()}`."
            )
            exit(1)
