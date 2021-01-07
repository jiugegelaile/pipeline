# -*- coding: utf-8 -*- 
# @Time : 2020/12/2 10:13 
# @Author : Li Xiaopeng
# @File : log_util.py
# @Software: PyCharm
import logging
import os
import sys


# class LogUtil:
#
#     __version_tag = None
#
#     def setArgs(self, version_tag=None):
#         if version_tag is not None:
#             self.__version_tag = version_tag
#         return self
#
#     def get_logger(self, level="INFO", name="nb", console=True, format=None):
#         if format is None:
#             format = "%(asctime)s %(process)s %(filename)s:%(lineno)s %(name)s %(levelname)s %(message)s"
#         version_tag = None
#         if self.__version_tag is not None and len(self.__version_tag) > 0:
#             version_tag = base64.b64decode(self.__version_tag[0]).decode("utf8")
#         return get_logger(level, name, console, format, version_tag)


def get_logger(level="INFO", name="nb", console=True, format=None, version_tag=None):
    level_dict = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "WARN": logging.WARN,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    if sys.platform == "win32":
        parent_dir = "d:/logs"
    else:
        if os.getenv("NAVCLOUD_VERSION_SOURCE_INFO") is not None:
            parent_dir = "/opt/notebook"
        else:
            parent_dir = "/opt/pyspark"
    if format is None:
        format = "%(asctime)s %(process)s %(filename)s:%(lineno)s %(name)s %(levelname)s %(message)s"
    version_tag = "$$@@pyspark_version_tag@@$$"
    if "pyspark_version" in version_tag:
        version_tag = os.getpid()
    logging.basicConfig(format=format, filename="%s/%s.log" % (parent_dir, version_tag))
    logger = logging.getLogger(name)

    console_handler = None
    if console:
        handlers = logger.handlers
        if len(handlers) == 0:
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter(format)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

    if level in level_dict:
        log_level = level_dict[level]
    else:
        logger.error('请输入正确的日志等级，例如："DEBUG", "INFO", "WARNING", "WARN", "ERROR", "CRITICAL"')
        sys.exit(1)

    if console_handler is not None:
        console_handler.setLevel(log_level)
    logger.setLevel(log_level)

    return logger

