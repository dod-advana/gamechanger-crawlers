from subprocess import check_output


class UserArgument:
    """User argument for the docker run command.

    Attributes:
        flag (str): The flag used for this type of argument.

        formatted (str): The formatted argument to pass in the docker run
            command.
    """

    def __init__(self):
        self.flag = "-u"
        self.formatted = self._format()

    def _format(self):
        """Format the argument to be passed in the docker run command.

        Returns:
            str
        """
        id_u = check_output(["id", "-u"]).decode("UTF-8").strip()
        id_g = check_output(["id", "-g"]).decode("UTF-8").strip()

        return f"{self.flag} {id_u}:{id_g}"
