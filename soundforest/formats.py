# coding=utf-8
"""Music file formats

Guessing of supported file formats and codecs based on extensions

"""

import os
import tempfile

from subprocess import Popen, PIPE

from soundforest import normalized, SoundforestError, CommandPathCache
from soundforest.config import ConfigDB
from soundforest.defaults import SOUNDFOREST_CACHE_DIR
from soundforest.log import SoundforestLogger
from soundforest.metadata import Metadata

logger = SoundforestLogger().default_stream

TAG_PARSERS = {
    'm4a':      'soundforest.tags.formats.aac.aac',
    'm4r':      'soundforest.tags.formats.aac.aac',
    'mp3':      'soundforest.tags.formats.mp3.mp3',
    'flac':     'soundforest.tags.formats.flac.flac',
    'vorbis':   'soundforest.tags.formats.vorbis.vorbis',
}

PATH_CACHE = CommandPathCache()
PATH_CACHE.update()

db = ConfigDB()

def filter_available_command_list(commands):
    available = []
    for cmd in commands:
        try:
            executable = cmd.command.split(' ', 1)[0]
        except IndexError:
            executable = cmd.command
            pass
        if PATH_CACHE.which(executable) is None:
            continue
        available.append(cmd.command)

    return available

def match_codec(path):
    ext = os.path.splitext(path)[1][1:]

    if ext == '':
        ext = path

    if ext in db.codecs.keys():
        return db.codecs[ext]

    for codec in db.codecs.values():
        if ext in [e.extension for e in codec.extensions]:
            return codec

    return None

def match_metadata(path):
    metadata = Metadata()
    m = metadata.match(path)
    if not m:
        return None

    return m

class path_string(unicode):
    def __init__(self, path):
        if isinstance(path, unicode):
            unicode.__init__(self, normalized(path).encode('utf-8'))
        else:
            unicode.__init__(self, normalized(path))

    @property
    def exists(self):
        if os.path.isdir(self) or os.path.isfile(self):
            return True
        return False

    @property
    def isdir(self):
        return os.path.isdir(self)

    @property
    def isfile(self):
        return os.path.isfile(self)

    @property
    def no_ext(self):
        return os.path.splitext(self)[0]

    @property
    def directory(self):
        return os.path.dirname(self)

    @property
    def filename(self):
        return os.path.basename(self)

    @property
    def extension(self):
        return os.path.splitext(self)[1][1:]


class AudioFileFormat(object):
    """AudioFileFormat

    Common file format wrapper for various codecs

    """

    def __init__(self, path):
        self.log =  SoundforestLogger().default_stream
        self.path = path_string(path)
        self.codec = None
        self.description = None
        self.is_metadata = False

        self.codec = match_codec(path)
        if self.codec is not None:
            self.description = self.codec.description.lower()

        else:
            m = match_metadata(path)
            if m:
                self.is_metadata = True
                self.description = m.description.lower()

            elif os.path.isdir(path):
                self.description = 'unknown directory'

            else:
                self.description = 'unknown file format'

    def __repr__(self):
        return '{0} {1}'.format(self.codec, self.path)

    @property
    def directory(self):
        return os.path.dirname(self.path)

    @property
    def filename(self):
        return os.path.basename(self.path)

    @property
    def extension(self):
        return os.path.splitext(self.path)[1][1:]

    @property
    def size(self):
        if not self.path.isfile:
            return None
        return os.stat(self.path).st_size

    @property
    def ctime(self):
        if not self.path.isfile:
            return None
        return os.stat(self.path).st_ctime

    @property
    def mtime(self):
        if not self.path.isfile:
            return None
        return os.stat(self.path).st_mtime

    def get_temporary_file(self, dir=SOUNDFOREST_CACHE_DIR, prefix='tmp', suffix=''):
        if not os.path.isdir(dir):
            try:
                os.makedirs(dir)
            except IOError, (ecode, emsg):
                raise SoundforestError('Error creating directory {0}: {1}'.format(SOUNDFOREST_CACHE_DIR, emsg))
            except OSError, (ecode, emsg):
                raise SoundforestError('Error creating directory {0}: {1}'.format(SOUNDFOREST_CACHE_DIR, emsg))

        return tempfile.mktemp(dir=dir, prefix=prefix, suffix=suffix)

    def get_tag_parser(self):
        if self.codec is None or self.codec.name not in TAG_PARSERS.keys():
            return None

        try:
            classpath = TAG_PARSERS[self.codec.name]
            module_path = '.'.join(classpath.split('.')[:-1])
            class_name = classpath.split('.')[-1]
            m = __import__(module_path, globals(), fromlist=[class_name])

        except KeyError, emsg:
            return None

        return getattr(m, class_name)

    def get_available_encoders(self):
        if self.codec is None or not self.codec.encoders:
            return []

        return filter_available_command_list(self.codec.encoders)

    def get_available_decoders(self):
        if self.codec is None or not self.codec.decoders:
            return []

        return filter_available_command_list(self.codec.decoders)

    def get_available_testers(self):
        if self.codec is None or not self.codec.testers:
            return []

        return filter_available_command_list(self.codec.testers)

    def execute(self, args):
        self.log.debug('running: {0}'.format(' '.join(args)))
        p = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = p.communicate()

        if stdout:
            self.log.debug('output:\n{0}'.format(stdout))
        if stderr:
            self.log.debug('errors:\n{0}'.format(stderr))

        return p.returncode, stdout, stderr
