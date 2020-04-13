import argparse
import logging
from typing import Dict


def init_log(baselevel: str = 'INFO') -> None:

    """

    :param baselevel: base level of log
    :return:
    """

    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        level=getattr(logging, baselevel.upper()))


def print_log(log_level: str,
              msg: str) -> None:
    """

    :param log_level: DEBUG, INFO, WARNING, ERROR
    :param msg: log message
    :return:
    """

    log = getattr(logging, log_level.lower())
    log(msg)


def sh_argparser() -> Dict[str, str]:

    """

    :return:
    """

    parser = argparse.ArgumentParser(description='This is the ETL running on spark')
    parser.add_argument('--mode',
                        dest='mode',
                        required=True,
                        help='job mode: test or full')
    args = vars(parser.parse_args())

    return args
