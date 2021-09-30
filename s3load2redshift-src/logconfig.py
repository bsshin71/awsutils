# -*- coding: utf-8 -*-
import logging
import logging.config

config = {
    "version": 1,
    "formatters": {
        "simple": {"format": "%(asctime)s %(threadName)s %(levelname)s  [%(name)s] %(message)s"},
        "complex": {
            "format": "%(asctime)s %(threadName)s %(levelname)s [%(name)s]   [%(filename)s:%(lineno)d] - %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "level": "DEBUG",
        },
        "file": {
            # "class": "logging.FileHandler",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "error.log",
            "formatter": "complex",
            "level": "DEBUG",
            "maxBytes": 1000000,
            "backupCount": 10,
        },
    },
    "root": {"handlers": ["console", "file"], "level": "DEBUG"},
    "loggers": {"parent": {"level": "INFO"}, "parent.child": {"level": "DEBUG"},},
}

def loadlogconfig():
    logging.config.dictConfig(config)
