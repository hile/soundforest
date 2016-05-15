# coding=utf-8
"""Music file formats

Guessing of supported file formats and codecs based on extensions

"""

import os
import tempfile

from subprocess import Popen, PIPE

from soundforest import path_string, SoundforestError, CommandPathCache
from soundforest.database import ConfigDB
from soundforest.defaults import SOUNDFOREST_CACHE_DIR
from soundforest.log import SoundforestLogger
from soundforest.metadata import Metadata

logger = SoundforestLogger().default_stream

TAG_PARSERS = {
    'm4a':      'soundforest.tags.formats.aac.aac',
    'mp3':      'soundforest.tags.formats.mp3.mp3',
    'flac':     'soundforest.tags.formats.flac.flac',
    'vorbis':   'soundforest.tags.formats.vorbis.vorbis',
}
TAG_EXTENSION_ALIASES = {
    'm4r':      'm4a',
    'ogg':      'vorbis',
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

    if ext in db.codec_configuration.keys():
        return db.codec_configuration[ext]

    for codec in db.codec_configuration.values():
        if ext in [value.extension for value in codec.extensions]:
            return codec

    return None

def match_metadata(path):
    metadata = Metadata()
    m = metadata.match(path)
    if not m:
        return None

    return m


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
        if self.codec is None:
            return None

        if self.codec.name in TAG_PARSERS.keys():
            classpath = TAG_PARSERS[self.codec.name]
        else:
            try:
                classpath = TAG_PARSERS[TAG_EXTENSION_ALIASES[self.codec.name]]
            except KeyError:
                return None

        module_path = '.'.join(classpath.split('.')[:-1])
        class_name = classpath.split('.')[-1]
        m = __import__(module_path, globals(), fromlist=[class_name])

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
