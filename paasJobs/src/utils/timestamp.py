from datetime import datetime


class Timestamp:
    """Timestamp class.

    Args:
        str_format (str, optional): Formats the object's `str` according to
            this format. Default is "%Y%m%d_%H%M%S".

            For a complete list of formatting directives, see the
            "strftime and strptime Format Codes" section here:
            https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior

    Attributes:
        timestamp (datetime.datetime): Timestamp of the moment the object was
            created.

        iso_format (str): The object's `timestamp` attribute in ISO format.
            Example: `2022-08-02T08:27:02.663662`

        str (str): The object's `timestamp` attribute as a string, formatted
            according to the `str_format` argument passed in init.
    """

    def __init__(self, str_format="%Y%m%d_%H%M%S"):
        self.timestamp = datetime.now()
        self.iso_format = self.timestamp.isoformat()
        self.str = self.timestamp.strftime(str_format)
