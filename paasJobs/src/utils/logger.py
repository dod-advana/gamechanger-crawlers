from os import makedirs
from os.path import split
from logging import getLogger, StreamHandler, Formatter, FileHandler

# Logger defaults
DEFAULT_LEVEL = "INFO"
DEFAULT_MSG_FMT = "%(levelname)s - %(asctime)s - %(message)s"
DEFAULT_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def configure_logger(
    name=None,
    min_level=DEFAULT_LEVEL,
    filepath=None,
    msg_fmt=DEFAULT_MSG_FMT,
    date_fmt=DEFAULT_DATE_FMT,
):
    """Configure a logger object.

    Args:
        name (str or None, optional): If str, name of the logger to get/ create.
            If None, will get the root logger. Default is None.
        min_level (str or int, optional): The minimum level to log. See
            https://docs.python.org/3/library/logging.html#levels for options.
            Default is DEFAULT_LEVEL.
        filepath (str or None, optional): If str, path to a ".log" file to
            record log messages. If None, will not log to a file. Default is
            None.
        msg_fmt (str, optional): Log message formatting. Default is
            DEFAULT_MSG_FMT.
        date_fmt (str, optional): Date format for log messages. Default is
            DEFAULT_DATE_FMT.

    Returns:
        logging.Logger
    """
    logger = getLogger(name)
    logger.propagate = False
    logger.setLevel(min_level)

    has_stream = False
    has_file = False
    for handler in logger.handlers:
        handler_type = type(handler)
        if handler_type == StreamHandler:
            has_stream = True
        elif handler_type == FileHandler:
            has_file = True

    if not has_stream:
        add_handler(logger, StreamHandler(), min_level, msg_fmt, date_fmt)

    if not has_file and filepath is not None and filepath.endswith(".log"):
        makedirs(split(filepath)[0], exist_ok=True)
        add_handler(
            logger, FileHandler(filepath), min_level, msg_fmt, date_fmt
        )

    return logger


def add_handler(
    logger,
    handler,
    min_level=DEFAULT_LEVEL,
    msg_fmt=DEFAULT_MSG_FMT,
    date_fmt=DEFAULT_DATE_FMT,
):
    """Add a handler to a logger.

    Args:
        logger (logging.Logger): The logger to add a handler to.
        handler (logging.Handler): The handler to add to the logger.
        min_level (str or int, optional): Minimum level to log. Default is
            DEFAULT_LEVEL.
        msg_fmt (str, optional): Message formatting for the handler. Default is
            DEFAULT_MSG_FMT.
        date_fmt (str, optional): Date format for the handler. Default is
            DEFAULT_DATE_FMT.

    Returns:
        None
    """
    formatter = Formatter(msg_fmt, datefmt=date_fmt)
    handler.setFormatter(formatter)
    handler.setLevel(min_level)
    logger.addHandler(handler)


def get_file_handler_paths(logger):
    """Get paths of the logger's FileHandlers.

    Args:
        logger (logging.Logger)

    Returns:
        list of str: Paths
    """
    return [
        handle.baseFilename
        for handle in logger.handlers
        if type(handle) == FileHandler
    ]
