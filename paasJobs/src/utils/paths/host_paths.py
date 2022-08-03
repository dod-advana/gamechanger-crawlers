from .filenames import Filenames


class HostPaths:
    """Paths relative to the host machine.

    Args:
        tmp_dir (str): Use the environment variable HOST_JOB_TMP_DIR.

        job_name (str): Name of the job. Used in creating the object's
            `job_dir` attribute.

        timestamp_str (str): Timestamp of the job. Used in creating the
            object's `job_dir` attribute.

    Attributes:
        tmp_dir (str): tmp_dir argument passed in init.

        job_dir (str): Path to directory with the format:
            {self.tmp_dir}/{job_name}/{timestamp_str}
        
        previous_manifest (str): Path to previous manifest file.

        cumulative_manifest (str): Path to cumulative manifest file.
    """

    def __init__(self, tmp_dir, job_name, timestamp_str):
        self.tmp_dir = tmp_dir
        self.job_dir = f"{self.tmp_dir}/{job_name}/{timestamp_str}"
        self.previous_manifest = (
            f"{self.tmp_dir}/{Filenames.PREVIOUS_MANIFEST}"
        )
        self.cumulative_manifest = (
            f"{self.job_dir}/{Filenames.CUMULATIVE_MANIFEST}"
        )
