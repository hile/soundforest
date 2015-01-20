# coding=utf-8
"""Soundforest audio tree management tools

Audio file tree processing classes

"""

import os
import sys
import unicodedata

from soundforest.defaults import SOUNDFOREST_USER_DIR


class SoundforestError(Exception):
    pass


class TreeError(Exception):
    pass


def normalized(path, normalization='NFC'):
    """
    Return given path value as normalized unicode string on OS/X,
    on other platform return the original string as unicode
    """
    if sys.platform != 'darwin':
        if not isinstance(path, unicode):
            return unicode(path, 'utf-8')

    if not isinstance(path, unicode):
        path = unicode(path, 'utf-8')

    return unicodedata.normalize(normalization, path)


class CommandPathCache(list):

    """
    Class to represent commands on user's search path.
    """
    def __init__(self):
        list.__init__(self)
        self.paths = None
        self.update()

    def update(self):
        """
        Updates the commands available on user's PATH
        """
        self.paths = []
        self.__delslice__(0, len(self))

        for path in os.getenv('PATH').split(os.pathsep):
            if not self.paths.count(path):
                self.paths.append(path)

        for path in self.paths:
            if not os.path.isdir(path):
                continue

            for cmd in [os.path.join(path, f) for f in os.listdir(path)]:
                if os.path.isdir(cmd) or not os.access(cmd, os.X_OK):
                    continue

                self.append(cmd)

    def versions(self, name):
        """
        Returns all commands with given name on path, ordered by PATH search
        order.
        """
        if not len(self):
            self.update()

        return [cmd for cmd in self if os.path.basename(cmd) == name]

    def which(self, name):
        """
        Return first matching path to command given with name, or None if
        command is not on path
        """
        versions = self.versions(name)
        if versions:
            return versions[0]
        else:
            return None
