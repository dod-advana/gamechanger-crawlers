from boto3 import Session


class S3Service:
    """This class is responsible providing the connection to the S3 and
    performing data transfer operations.

    This service requires the following Environment variables to be
    configured:

    * AWS_SHARED_CREDENTIALS_FILE - path to the aws credentials file
    * AWS_CONFIG_FILE - path to the aws config file

    These paths must exist in the machine or Docker container. If these
    variables are not defined, then the service will default to using
    the `~/.aws/` path to find these files.

    When developing locally, you may also need to define this Environment
    variable:

    * AWS_PROFILE - profile to use

    If not defined, the service will use the default role (which is sufficient
    when running in AWS, but not necessarily locally).

    Also when running locally, you will need to supply aws_access_key, 
    aws_secret_key, and aws_session_token.

    Args:
        logger (logging.Logger)

        region_name (str): AWS Region name.

        profile_name (str or None, optional): AWS Profile name.

        aws_access_key (str or None, optional): AWS access key ID.

        aws_secret_key (str or None, optional): AWS secret key.

        aws_session_token (str or None, optional): AWS temporary session token.

    Attributes:
        logger (logging.Logger)

        region_name (str): AWS Region name.

        profile_name (str or None): AWS Profile name.

        client (botocore.client.S3): S3 client.

        aws_access_key (str or None): AWS access key ID.

        aws_secret_key (str or None): AWS secret key.

        aws_session_token (str or None): AWS temporary session token.
    """

    def __init__(
        self,
        logger,
        region_name,
        profile_name=None,
        aws_access_key=None,
        aws_secret_key=None,
        aws_session_token=None,
    ):
        self.region_name = region_name
        self.profile_name = profile_name
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_session_token = aws_session_token
        self.logger = logger
        self._create_client()

    def _create_client(self):
        """Create the S3 client.

        Establishes the boto3 S3 client which can be used for downloading/
        uploading data from/ to an S3 bucket.

        Creates the object's `client` attribute.
        """
        self.logger.info("Creating S3 client.")
        try:
            # Note: the Session() pulls from the default `~/.aws directory for
            # config/ credentials.
            self.client = Session(
                profile_name=self.profile_name,
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                aws_session_token=self.aws_session_token,
            ).client("s3", region_name=self.region_name)
        except Exception:
            self.logger.exception("Failed to create S3 client.")

    def download_file(self, bucket, key, local_path):
        """Download a file from S3.

        Args:
            bucket (str): Name of the bucket to download from.
            key (str): Name of the key to download from.
            local_path (str): Path to download the file to.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        try:
            self.client.download_file(bucket, key, local_path)
        except Exception as e:
            self.logger.exception(
                self._fail_msg("download", bucket, key, local_path, e)
            )
            return False
        else:
            return True

    def upload_file(self, filepath, bucket, key):
        """Upload a file to S3.

        Args:
            filepath (str): Path to the file to upload.
            bucket (str): Bucket to upload to.
            key (str): Key to upload to.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        try:
            self.client.upload_file(filepath, bucket, key)
        except Exception as e:
            self.logger.exception(
                self._fail_msg("upload", bucket, key, filepath, e)
            )
            return False
        else:
            return True

    def copy(self, source_bucket, source_key, copy_bucket, copy_key):
        """Copy a file in S3.

        Args:
            source_bucket (str): Bucket of the file to copy.
            source_key (str): Key of the file to copy.
            copy_bucket (str): Bucket to copy to.
            copy_key (str): Key to copy to.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        source = ({"Bucket": source_bucket, "Key": source_key},)

        try:
            self.client.copy(
                source,
                copy_bucket,
                copy_key,
            )
        except Exception:
            self.logger.exception(
                self._fail_msg("copy", copy_bucket, copy_key, source, e)
            )
            return False
        else:
            return True

    def _fail_msg(self, operation, bucket, key, source, exception=""):
        """Create a fail message.

        Args:
            operation (str): The operation that failed. Ex: "upload",
                "download", "copy".
            bucket (str)
            key (str)
            local_path (str)
            exception (str, optional): Exception message

        Returns:
            str
        """
        if operation == "download":
            how = "from"
        elif operation == "upload":
            how = "to"
        else:
            how = "in"

        return (
            f"Failed to {operation} file {how} S3. Bucket: `{bucket}`. "
            f"Key: `{key}`. Source: `{source}` Exception: {exception}."
        )
