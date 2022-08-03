class VolumeArgument:
    """Volume argument for a Docker command.

    Args:
        host_path (str): Name of the volume on the host machine.

        container_path (str): Where the file or directory is mounted in the
            container.

        z_option (bool, optional): True add the ":z" option to the end of the
            argument, which tells Docker to share the volume content between
            containers. False otherwise. Default is True.

    Attributes:
        flag (str): The flag used for this type of argument.

        formatted (str): The formatted argument to pass in a command.
    """

    def __init__(self, host_path, container_path, z_option=True):
        self.flag = "-v"
        self.formatted = self._format(host_path, container_path, z_option)

    def _format(self, host_path, container_path, z_option):
        """Format the argument to be passed in a command.
        
        Returns: 
            str
        """
        arg = f"{self.flag} {host_path}:{container_path}"
        if z_option:
            arg += ":z"

        return arg
