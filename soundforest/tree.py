# coding=utf-8
"""Tree, album  and track

Abstraction of filesystem audio file trees, albums and tracks

"""

import hashlib
import os
import re
import shutil
import time


from soundforest import normalized, path_string, SoundforestError, TreeError
from soundforest.log import SoundforestLogger
from soundforest.formats import AudioFileFormat, match_codec, match_metadata
from soundforest.prefixes import TreePrefixes, PrefixError
from soundforest.metadata import CoverArt
from soundforest.tags import TagError
from soundforest.tags.albumart import AlbumArt, AlbumArtError
from soundforest.tags.tagparser import Tags


IGNORED_TREE_FOLDER_NAMES = (
    '.fseventsd',
    '.Spotlight-V100/',
    '.DocumentRevisions-V100/',
    '.Trashes',
    '.vol',
)


class IterableTrackFolder(object):
    """IterableTrackFolder model

    Abstract class for various iterable music items

    """

    def __init__(self, path, iterable):
        self.log = SoundforestLogger().default_stream
        self.__next = None
        self.__iterable = iterable

        if path in ['.', '']:
            path = os.path.realpath(path)

        self.path = path_string(path)
        self.prefixes = TreePrefixes()
        self.invalid_paths = []
        self.has_been_iterated = False

        setattr(self, iterable, [])

    def __getitem__(self, item):
        if not self.has_been_iterated:
            self.load()
        iterable = getattr(self, self.__iterable)
        return iterable[item]

    def __len__(self):
        iterable = getattr(self, self.__iterable)
        if len(iterable) == 0:
            self.load()
        if len(iterable) - len(self.invalid_paths) >= 0:
            return len(iterable) - len(self.invalid_paths)
        else:
            return 0

    def __iter__(self):
        return self

    def next(self):
        iterable = getattr(self, self.__iterable)
        if self.__next is None:
            self.__next = 0
            self.has_been_iterated = False
            if len(iterable) == 0:
                self.load()
        try:
            entry = iterable[self.__next]
            self.__next += 1
            path = os.path.join(entry[0], entry[1])
            try:
                return Track(path)
            except TreeError:
                if not self.invalid_paths.count(path):
                    self.invalid_paths.append(path)
                return self.next()
        except IndexError:
            self.__next = None
            self.has_been_iterated = True
            raise StopIteration

    def load(self):
        """Lazy loader

        Lazy loader of the iterable item
        """
        iterable = getattr(self, self.__iterable)
        iterable.__delslice__(0, len(iterable))
        self.invalid_paths.__delslice__(0, len(self.invalid_paths))

    def relative_path(self, item=None):
        """Item relative path

        Returns relative path of this iterable item
        """

        if item is not None:
            if isinstance(item, Track):
                return self.prefixes.relative_path(item.path)
            else:
                return self.prefixes.relative_path(item)

        else:
            return self.prefixes.relative_path(self.path)

    def remove_empty_path(self, empty):
        """Remove empty directory

        Remove empty directory and all empty parent directories
        """
        while True:
            if not os.path.isdir(empty):

                # Directory does not exist
                return

            if os.listdir(empty):
                # Directory is not empty
                return

            try:
                os.rmdir(empty)
            except OSError, (ecode, emsg):
                raise TreeError('Error removing empty directory {0}: {1}'.format(empty, emsg))

            # Try to remove parent empty directory
            empty = os.path.dirname(empty)


class Tree(IterableTrackFolder):
    """Tree

    Audio file tree

    """

    def __init__(self, path):
        super(Tree, self).__init__(path, 'files')
        self.paths = {}
        self.empty_dirs = []
        self.relative_dirs = []

    def __len__(self):
        """Tree track cound

        Tree must be loaded to figure out it's length
        """
        if not self.has_been_iterated:
            self.has_been_iterated = True
            while True:
                try:
                    self.next()
                except StopIteration:
                    break

        return super(Tree, self).__len__()

    def __cmp_file_path__(self, a, b):
        if a[0] != b[0]:
            return cmp(a[0], b[0])
        return cmp(a[1], b[1])

    def load(self):
        """Load the albums and songs in the tree"""

        if not os.path.isdir(self.path):
            raise TreeError('Not a directory: {0}'.format(self.path))

        self.log.debug('{0} load tree'.format(self.path))
        start = long(time.mktime(time.localtime()))

        super(Tree, self).load()
        self.paths = {}
        self.empty_dirs = []
        self.relative_dirs = []

        for (root, dirs, files) in os.walk(self.path, topdown=True):
            if os.path.basename(root) in IGNORED_TREE_FOLDER_NAMES:
                continue

            if files:
                self.files.extend((root, filename) for filename in files if filename != '')
                for filename in files:
                    self.paths[os.path.join(root, filename)] = True

            elif not dirs:
                self.empty_dirs.append(root)

        self.relative_dirs = set(self.relative_path(x[0]) for x in self.files)
        self.files.sort(lambda x, y: self.__cmp_file_path__(x, y))

        stop = long(time.mktime(time.localtime()))
        self.log.debug('loaded {0:d} files in {1:d} seconds'.format(len(self.files), (stop-start)))

    def filter_tracks(self, regexp=None, re_path=True, re_file=True, as_tracks=False):
        if not len(self.files):
            self.load()

        tracks = [track for track in self.files if match_codec(track[1])]

        if regexp is not None:
            if not re_file and not re_path:
                raise TreeError('No matches if both re_file and re_path are False')

            if isinstance(regexp, basestring):
                regexp = re.compile(regexp)

            tracks = [track for track in tracks if re_path and regexp.match(track[0]) or re_file and regexp.match(track[1])]

        if as_tracks:
            return [Track(os.path.join(t[0], t[1])) for t in tracks]
        else:
            return tracks

    @property
    def directories(self):
        return set(normalized(os.path.dirname(x)) for x in self.paths.keys())

    @property
    def realpaths(self):
        return dict((normalized(os.path.realpath(v)), True) for v in self.paths.keys())

    def relative_path(self, item):
        """Item relative path

        Returns relative path of item in tree
        """
        if hasattr(item, 'path'):
            return self.path.relative_path(item.path)
        else:
            return self.path.relative_path(item)

    def as_albums(self):
        if not self.has_been_iterated:
            self.load()
        return [Album(path) for path in sorted(set(d[0] for d in self.files)) if path not in (self.path, '')]

    def match(self, path):
        match_path = self.relative_path(path)
        if not os.path.dirname(match_path) in self.relative_dirs:
            return None

    def test(self, callback):
        errors = False
        for track in self:
            if track.test(callback) != 0:
                errors = True
        if errors:
            return 1
        else:
            return 0


class Album(IterableTrackFolder):

    def __init__(self, path):
        super(Album, self).__init__(path, 'files')
        self.metadata_files = []

    def __repr__(self):
        return 'album {0}'.format(self.path)

    def __getitem__(self, item):
        item = super(Album, self).__getitem__(item)
        return Track(os.path.join(*item))

    def load(self):
        super(Album, self).load()

        self.metadata_files = []
        for name in os.listdir(self.path):
            if match_codec(name) is not None:
                self.files.append((self.path, name))

            else:
                metadata = match_metadata(name)
                if metadata is not None:
                    self.metadata_files.append(MetaDataFile(os.path.join(self.path, name), metadata))

        self.files.sort()

    @property
    def mtime(self):
        return os.stat(self.path).st_mtime

    @property
    def ctime(self):
        return os.stat(self.path).st_ctime

    @property
    def atime(self):
        return os.stat(self.path).st_atime

    @property
    def metadata(self):
        if not self.has_been_iterated:
            self.load()
        return self.metadata_files

    @property
    def albumart(self):
        if not len(self.metadata):
            return None

        for m in self.metadata:
            if not hasattr(m, 'metadata'):
                raise TreeError('Invalid object types in self.metadata: {0}'.format(m))
            if isinstance(m.metadata, CoverArt):
                return AlbumArt(m.path)

        return None

    def copy_metadata(self, target):
        if isinstance(target, basestring):
            target = Album(target)

        if not os.path.isdir(target.path):
            try:
                os.makedirs(target.path)
            except OSError, (ecode, emsg):
                raise TreeError('Error creating directory {0}: {1}'.format(target.path, emsg))

        for m in self.metadata:
            dst_path = os.path.join(target.path, os.path.basename(m.path))

            if os.path.realpath(m.path) == os.path.realpath(dst_path):
                continue

            try:
                shutil.copyfile(m.path, dst_path)
            except OSError, (ecode, emsg):
                self.script.exit(1, 'Error writing file {0}: {1}'.format(dst_path, emsg))

        target.load()
        albumart = target.albumart
        if target.albumart:
            for track in target:
                tags = track.tags
                if tags is None:
                    continue

                if not tags.supports_albumart:
                    self.log.debug('albumart not supported: {0}'.format(track.path))
                    continue

                if tags.set_albumart(albumart):
                    self.log.debug('albumart: {0}'.format(track))
                    tags.save()


class MetaDataFile(object):
    """MetaDataFile

    Metadata files, like album artwork, booklets and vendor specific analysis files.
    """

    def __init__(self, path, metadata=None):
        if metadata is None:
            metadata = match_metadata(path)
            if metadata is None:
                raise TreeError('Not a metadata file: {0}'.format(path))

        self.path = path_string(path)

        self.extension = os.path.splitext(self.path)[1][1:].lower()
        if self.extension == '':
            self.extension = None

        self.metadata = metadata

    def __repr__(self):
        return '{0} {1}'.format(self.metadata.description, self.filename)

    @property
    def filename(self):
        return os.path.basename(self.path)


class Track(AudioFileFormat):
    """Track

    Audio file track

    """

    def __init__(self, path):
        super(Track, self).__init__(path)

        self.prefixes = TreePrefixes()
        if self.codec is None:
            raise TreeError('Not a music file: {0}'.format(self.path))

        self.tags_loaded = False
        self.file_tags = None

    @property
    def tags(self):
        if not self.tags_loaded:
            try:
                self.file_tags = Tags(self.path, fileformat=self)
                self.tags_loaded = True
            except TagError, emsg:
                raise TreeError('Error loading tags: {0}'.format(emsg))

        return self.file_tags

    def relative_path(self):
        return self.prefixes.relative_path(os.path.realpath(self.path))

    @property
    def filename_no_extension(self):
        return os.path.splitext(os.path.basename(self.path))[0]

    @property
    def extension(self):
        return os.path.splitext(self.path)[1][1:]

    @property
    def album(self):
        return Album(os.path.dirname(self.path))

    @property
    def tracknumber_and_title(self):
        filename = os.path.splitext(os.path.basename(self.path))[0]

        try:
            tracknumber, title = filename.split(None, 1)
            tracknumber = int(tracknumber)
        except ValueError:
            tracknumber = None
            title = filename

        return tracknumber, title

    @property
    def checksum(self):
        with open(self.path, 'rb') as fd:
            m = hashlib.md5()
            m.update(fd.read())
            return m.hexdigest()

    def get_album_tracks(self):
        path = os.path.dirname(self.path)
        extensions = CODECS[self.codec]['extensions']
        tracks = []

        for t in os.listdir(path):
            if os.path.splitext(t)[1][1:] not in extensions:
                continue
            tracks.append(Track(os.path.join(path, t)))

        return tracks

    def get_decoder_command(self, wav_path=None):
        if wav_path is None:
            wav_path = '{0}.wav'.format(os.path.splitext(self.path)[0])
        if wav_path == self.path:
            raise TreeError('Trying to encode to itself')

        try:
            decoder = self.get_available_decoders()[0]
        except IndexError:
            raise TreeError('No available decoders for {0}'.format(self.path))

        decoder = decoder.split()
        decoder[decoder.index('OUTFILE')] = wav_path
        decoder[decoder.index('FILE')] = self.path
        return decoder

    def get_encoder_command(self, wav_path=None):
        if wav_path is None:
            wav_path = '{0}.wav'.format(os.path.splitext(self.path)[0])
        if wav_path == self.path:
            raise TreeError('Trying to encode to itself')

        try:
            encoder = self.get_available_encoders()[0]
        except IndexError:
            raise TreeError('No available encoders for {0}'.format(self.path))

        encoder = encoder.split()
        encoder[encoder.index('OUTFILE')] = self.path
        encoder[encoder.index('FILE')] = wav_path
        return encoder

    def get_tester_command(self, tempfile_path):
        try:
            tester = self.get_available_testers()[0]
        except IndexError:
            raise TreeError('No available testers for {0}'.format(self.path))

        tester = tester.split()
        tester[tester.index('FILE')] = self.path
        if tester.count('OUTFILE') == 1:
            tester[tester.index('OUTFILE')] = tempfile_path

        return tester

    def test(self, callback):
        tempfile_path = self.get_temporary_file(prefix='test', suffix='.wav')
        try:
            cmd = self.get_tester_command(tempfile_path)
        except TreeError, emsg:
            callback(self, False, errors='No tester available for {0}'.format(self.extension))
            return

        rv, stdout, stderr = self.execute(cmd)
        if rv == 0:
            callback(self, True, stdout=stdout, stderr=stderr)
        else:
            callback(self, False, stdout=stdout, stderr=stderr)

        if os.path.isfile(tempfile_path):
            try:
                os.unlink(tempfile_path)
            except IOError, (ecode, emsg):
                raise TreeError('Error removing temporary file {0}: {1}'.format(tempfile_path, emsg))

        return rv
