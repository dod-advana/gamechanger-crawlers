import io, traceback
from os.path import join as join_path
import logging

from logging.config import dictConfig as load_log_config_dict
import codecs

import yaml
from pathlib import Path

from dataPipelines import PACKAGE_PATH

DEFAULT_LOG_CFG_PATH = join_path(PACKAGE_PATH, "log_cfg.yml")


def setup_logging(log_cfg_path: str = DEFAULT_LOG_CFG_PATH, level: int = logging.INFO):
    if Path(log_cfg_path).exists():
        with codecs.open(log_cfg_path, "r", encoding="utf-8") as fd:
            config = yaml.safe_load(fd)
            str_level = logging.getLevelName(level)
            for _logger in config["loggers"]:
                config["loggers"][_logger]["level"] = str_level
            load_log_config_dict(config)
    elif level:
        logging.basicConfig(level=level)
    return logging.getLogger(__name__)


class FullStackFormatter(logging.Formatter):
    def formatException(self, ei):
        """
        Format and return the specified exception information as a string.

        This default implementation just uses
        traceback.print_exception()
        """
        sio = io.StringIO()
        tb = ei[2]

        # traceback.print_exception(ei[0], ei[1], tb, None, sio)
        traceback.print_stack(tb.tb_frame.f_back, file=sio)
        s = sio.getvalue()
        sio.close()
        if s[-1:] == "\n":
            s = s[:-1]
        return s
