# coding=utf-8
"""Logging functions

Soundforest debug and file logging facilities

"""

import os
import logging
import logging.handlers

from soundforest import SoundforestError

DEFAULT_LOGFORMAT = '%(module)s %(levelname)s %(message)s'
DEFAULT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_LOGFILEFORMAT = '%(asctime)s %(module)s.%(funcName)s %(message)s'
DEFAULT_LOGSIZE_LIMIT = 2**20
DEFAULT_LOG_BACKUPS = 10

class SoundforestLogger(object):
    """SoundforestLogger

    Class for common script logging tasks. Implemented as singleton to prevent
    errors in duplicate handler initialization.

    """
    __instances = {}
    def __init__(self, name=None):
        name = name is not None and name or self.__class__.__name__

        if not SoundforestLogger.__instances.has_key(name):
            SoundforestLogger.__instances[name] = SoundforestLogger.LoggerInstance(name)

        self.__dict__['_SoundforestLogger__instances'] = SoundforestLogger.__instances
        self.__dict__['name'] = name

    class LoggerInstance(dict):
        """LoggerInstance

        Singleton implementation of logging configuration for one program

        """
        def __init__(self, name):
            self.name = name
            self.loglevel = logging.Logger.root.level
            self.register_stream_handler('default_stream')

        @property
        def level(self):
            return self.loglevel

        def __getattr__(self, attr):
            if attr in self.keys():
                return self[attr]
            raise AttributeError('No such LoggerInstance attribute: %s' % attr)

        def __setattr__(self, attr, value):
            if attr in ('level', 'loglevel'):
                for logger in self.values():
                    logger.setLevel(value)
                self.__dict__['loglevel'] = value
            else:
                object.__setattr__(self, attr, value)

        def register_stream_handler(self, name, logformat=None, timeformat=None):
            """
            Register a common log stream handler
            """

            if name in self.keys():
                raise SoundforestScriptError('Handler already registered to %s: %s' % (self.name, name))

            if logformat is None:
                logformat = DEFAULT_LOGFORMAT
            if timeformat is None:
                timeformat = DEFAULT_TIME_FORMAT

            for logging_manager in logging.Logger.manager.loggerDict.values():
                if hasattr(logging_manager, 'name') and logging_manager.name==name:
                    self[name] = logging.getLogger(name)
                    return

            logger = logging.getLogger(name)
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(logformat, timeformat))
            logger.addHandler(handler)

            self[name] = logger
            return logger

        def register_file_handler(self, name, directory,
                         logformat=None, timeformat=None,
                         maxBytes=DEFAULT_LOGSIZE_LIMIT,
                         backupCount=DEFAULT_LOG_BACKUPS):
            """
            Register a common log file handler for rotating file based logs
            """
            if name in self.keys():
                raise SoundforestScriptError('Handler already registered to %s: %s' % (self.name, name))

            if logformat is None:
                logformat = DEFAULT_LOGFILEFORMAT
            if timeformat is None:
                timeformat = DEFAULT_TIME_FORMAT

            for logging_manager in logging.Logger.manager.loggerDict.values():
                if hasattr(logging_manager, 'name') and logging_manager.name==name:
                    self[name] = logging.getLogger(name)
                    return

            if not os.path.isdir(directory):
                try:
                    os.makedirs(directory)
                except OSError:
                    raise SoundforestScriptError('Error creating directory: %s' % directory)

            logger = logging.getLogger(name)
            logfile = os.path.join(directory, '%s.log' % name)
            handler = logging.handlers.RotatingFileHandler(
                filename=logfile,
                mode='a+',
                maxBytes=maxBytes,
                backupCount=backupCount
            )
            handler.setFormatter(logging.Formatter(logformat, timeformat))
            logger.addHandler(handler)
            logger.setLevel(self.loglevel)

            self[name] = logger
            return logger

        def set_level(self, value):
            if not hasattr(logging, value):
                raise SoundforestError('Invalid logging level: %s' % value)

            level = getattr(logging, value)
            if not isinstance(level, int):
                raise SoundforestError('Not integer value: %s (%s)' % (value, type(level)))

            self.loglevel = level

    def __getattr__(self, attr):
        return getattr(self.__instances[self.name], attr)

    def __setattr__(self, attr, value):
        setattr(self.__instances[self.name], attr, value)

    def __getitem__(self, item):
        return self.__instances[self.name][item]

    def __setitem__(self, item, value):
        self.__instances[self.name][item] = value
