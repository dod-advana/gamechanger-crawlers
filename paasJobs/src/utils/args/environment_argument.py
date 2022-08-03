class EnvironmentArgument:
    """Environment argument for a Docker command.

    Attributes:
        flag (str): The flag used for this type of argument.

        formatted (str): The formatted argument to pass in a command.
    """

    def __init__(self, name, value):
        self.flag = "-e"
        self.formatted = self._format(name, value)

    def _format(self, name, value):
        """Format the argument to be passed in a command.

        Returns:
            str
        """
        return f"{self.flag} {name}={value}"
