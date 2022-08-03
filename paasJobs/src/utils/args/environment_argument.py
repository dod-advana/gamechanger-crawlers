class EnvironmentArgument:
    """Environment argument for a Docker command.

    Attributes:
        flag (str): The flag used for this type of argument.

        formatted (str): The formatted argument to pass in a command.
    """

    def __init__(self, name, value):
        self.flag = "-e"
        self.formatted = self.format(name, value)

    def format(self, name, value):
        """Format the argument to be passed in a command.

        Args:
            name (str): Name of the environment variable to set.
            value (any): Value of the environment variable to set.

        Returns:
            str
        """
        return f"{self.flag} {name}={value}"
