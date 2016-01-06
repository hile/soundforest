#!/usr/bin/env python

import os
import codecs
import logging

from soundforest import normalized, path_string

class PlaylistError(Exception):
    pass


class Playlist(list):
    def __init__(self, name, unique=True):
        self.name = os.path.splitext(os.path.basename(name))[0]
        self.unique = unique
        self.modified = False
        self.path = None

    def __repr__(self):
        return self.path

    def __len__(self):
        if list.__len__(self) == 0:
            self.read()
        return list.__len__(self)

    @property
    def directory(self):
        return self.path.directory

    @property
    def filename(self):
        return self.path.filename

    @property
    def extension(self):
        return self.path.extension

    @property
    def exists(self):
        return os.path.isfile(self.path)

    def read(self):
        raise NotImplementedError('You must implement reading in subclass')

    def write(self):
        raise NotImplementedError('You must implement writing in subclass')

    def __insert(self, path, position=None):
        if self.unique and self.count(path) > 0:
            return

        self.modified = True
        if not position:
            super(Playlist, self).append(path)

        else:
            try:
                position = int(position)
                if position < 0:
                    raise ValueError

                if position > list.__len__(self):
                    position = list.__len__(self)

            except ValueError:
                raise PlaylistError('Invalid position: {0}'.format(position))

            self.insert(position, path)

    def append(self, path, position=None, recursive=False):
        path = normalized(os.path.realpath(path))
        if os.path.isfile(path):
            self.__insert(path, position)

        elif os.path.isdir(path):
            for f in ['{0}'.format(os.path.join(path, x)) for x in os.listdir(path)]:
                f = normalized(os.path.realpath(f))

                if not recursive and os.path.isdir(f):
                    continue

                self.append(f, position)

        else:
            raise PlaylistError('Not a file or directory: {0}'.format(path))

        self.modified = True

class m3uPlaylist(Playlist):
    def __init__(self, name, config=None, folder=None, unique=True):
        super(m3uPlaylist, self).__init__(name, unique)

        if os.path.isfile(name):
            path = os.path.realpath(name)

        else:
            if folder is not None:
                path = os.path.join(folder, '{0}.m3u'.format(self.name))
            else:
                path = os.path.join('{0}.m3u'.format(self.name))

        self.path = path_string(path)

    def read(self):
        if not self.exists:
            return

        try:
            with open(self.path, 'r') as lines:
                self.__delslice__(0, list.__len__(self))
                for l in lines:
                    l = l.strip()
                    if l.startswith('#'):
                        continue

                    filepath = normalized(os.path.realpath(l))
                    if not os.path.isfile(filepath):
                        continue

                    if self.unique and self.count(filepath)>0:
                        continue

                    self.append(filepath)
        except IOError, (ecode, emsg):
            raise PlaylistError('Error reading {0}: {1}'.format(self.path, emsg))

    def write(self):
        pl_dir = os.path.dirname(self.path)

        if not os.path.isdir(pl_dir):
            try:
                os.makedirs(pl_dir)

            except OSError, (ecode, emsg):
                raise PlaylistError('Error creating directory {0}: {1}'.format(pl_dir))

            except IOError, (ecode, emsg):
                raise PlaylistError('Error creating directory {0}: {1}'.format(pl_dir))

        if not self.modified:
            return

        try:
            fd = open(self.path, 'w')
            for filename in self:
                fd.write('{0}\n'.format(filename))
            fd.close()

        except IOError, (ecode, emsg):
            raise PlaylistError('Error writing playlist {0}: {1}'.format(self.path, emsg))

        except OSError, (ecode, emsg):
            raise PlaylistError('Error writing playlist {0}: {1}'.format(self.path, emsg))

    def remove(self):
        if not os.path.isfile(self.path):
            return

        try:
            os.unlink(self.path)

        except OSError, (ecode, emsg):
            raise PlaylistError('Error removing playlist {0}: {1}'.format(self.path, emsg))

        except oError, (ecode, emsg):
            raise PlaylistError('Error removing playlist {0}: {1}'.format(self.path, emsg))


class m3uPlaylistDirectory(list):
    def __init__(self, path=None):
        self.path = path_string(path)
        if not os.path.isdir(self.path):
            raise PlaylistError('No such directory: {0}'.format(self.path))

        for f in sorted(os.listdir(self.path)):
            f = os.path.join(self.path, f)

            if os.path.isdir(f):
                self.extend(m3uPlaylistDirectory(path=f))
                continue

            if os.path.splitext(f)[1][1:] != 'm3u':
                continue

            self.append(m3uPlaylist(f))

    def __getitem__(self, item):
        try:
            return self[int(item)]
        except IndexError:
            pass
        except ValueError:
            pass

        item = str(item)
        if str(item[-4:]) != '.m3u':
            item += '.m3u'

        try:
            name_lower = pl.filename.lower()
            return [item for item in self if item.lower() == name_lower][0]
        except IndexError:
            pass

        try:
            path = os.path.realpath(item)
            return [pl for pl in self if pl.path == path][0]
        except IndexError:
            pass

        raise IndexError('Invalid m3uPlaylistDirectory index {0}'.format(item))

    def relative_path(self, item):
        print self.path.relative_path(item)
        if hasattr(item, 'path'):
            return self.path.relative_path(item.path)
        else:
            return self.path.relative_path(item)

