"""
Matroska container metadata parser and audio track exporter.
"""

import os
import re
import sys

from soundforest.converters import ConverterError
from subprocess import Popen, PIPE

# Parsers for various known mkvinfo section header lines
RE_MATROSKA_SECTION_HEADER = re.compile('^\+ (?P<name>[^\s].*)$')
RE_SEGMENT_SECTION = re.compile('^Segment, size (?P<size>\d+)$')
RE_SEEK_HEAD_SECTION = re.compile('^Seek head .*$')

# Map audio formats to extensions known to soundforest
AUDIO_FORMAT_MAP = {
    'A_AAC': 'm4a',
    'A_FLAC': 'flac',
    'A_MP3': 'mp3',
    'A_OPUS': 'opus',
    'A_VORBIS': 'ogg',
}

# Rename strings in mkvinfo track details
KEY_NAME_MAP = {
    'Name': 'name',
    'String': 'string',
    'Channels': 'channels',
    'Sampling frequency': 'samplerate',
    'Codec ID': 'codec',
    'Codec delay': 'codec_delay_ms',
    'Seek pre-roll': 'seek_preroll_ms',
    'Default duration': 'default_duration_ms',
    'Lacing flag': 'lacing_flag',
    'Track UID': 'uid',
    'TrackUID': 'uid',
    'Track type': 'type',
    'Track number': 'tracknumber',
    'Language': 'language',
    'Display height': 'display_height',
    'Display width': 'display_width',
    'Pixel height': 'pixel_height',
    'Pixel width': 'pixel_width',
}

# Convert strings in track details to float milliseconds for these fields
TRACK_MILLISECOND_FIELDS = (
    'codec_delay_ms',
    'default_duration_ms',
    'seek_preroll_ms',
)


class EBMLValue(object):
    """Value in EBML

    Value from foo: bar lines in EBML
    """
    def __init__(self, section, key, value):
        self.section = section
        self.key = key
        self.value = value

    def __repr__(self):
        return '{0} {1}={2}'.format(self.section, self.key, self.value)


class EBMLSection(dict):
    """Generic EBML section

    Common container for EBML sections
    """
    def __init__(self, parent, name, prefix=0):
        self.parent = parent
        self.name = name
        self.prefix = prefix
        self.children = []

        if parent is not None:
            parent.children.append(self)

    def __repr__(self):
        return self.name

    def __setitem__(self, key, value):
        key = self.lookup_key(key)
        return super(EBMLSection, self).__setitem__(key, value)

    def lookup_key(self, key):
        try:
            return KEY_NAME_MAP[key]
        except KeyError:
            return key


class HeadSection(EBMLSection):
    """EBML head

    """
    pass


class SegmentSection(EBMLSection):
    """Segment

    """
    def __init__(self, parent, size, prefix):
        super(SegmentSection, self).__init__(parent, 'Segment', prefix)
        self.size = int(size)
        self.information = None

    def __repr__(self):
        return 'Segment {0} bytes'.format(self.size)


class SeekHeadSection(EBMLSection):
    """Segment seek head

    """
    def __init__(self, parent, prefix):
        super(SeekHeadSection, self).__init__(parent, 'Seekhead', prefix)


class SegmentInformationSection(EBMLSection):
    """Segment information

    """
    def __init__(self, parent, prefix):
        super(SegmentInformationSection, self).__init__(parent, 'SegmentInformation', prefix)
        parent.information = self


class TrackListSection(EBMLSection):
    """Segment Track list

    """
    def __init__(self, parent, prefix):
        super(TrackListSection, self).__init__(parent, 'Tracklist', prefix)

class TrackSection(EBMLSection):
    """Segment Track

    """
    def __init__(self, parent, prefix):
        super(TrackSection, self).__init__(parent, 'Track', prefix)
        self.details = None
        self.parent.parent.parent.tracks.append(self)

    def __setitem__(self, key, value):
        key = self.lookup_key(key)

        if key == 'tracknumber':
            value = int(value.split(None, 1)[0])

        if key == 'language' and value == 'und':
            value = None

        if key in TRACK_MILLISECOND_FIELDS:
            value = float(value.split(None, 1)[0].replace('ms', ''))

        super(TrackSection, self).__setitem__(key, value)


class TrackCodecDetailsSection(EBMLSection):
    def __init__(self, parent, prefix):
        super(TrackCodecDetailsSection, self).__init__(parent, 'CodecDetails', prefix)
        parent.details = self


class TagsSection(EBMLSection):
    """Segment tags list

    """
    def __init__(self, parent, prefix):
        super(TagsSection, self).__init__(parent, 'Tags', prefix)


class TagSection(EBMLSection):
    """Segment tag

    """
    def __init__(self, parent, prefix):
        super(TagSection, self).__init__(parent, 'Tag', prefix)
        self.targets = []
        self.parent.parent.parent.tags.append(self)


class TagTargetsSection(EBMLSection):
    """Segment tag targets

    """
    def __init__(self, parent, prefix):
        super(TagTargetsSection, self).__init__(parent, 'TagTargets', prefix)


class SimpleTagSection(EBMLSection):
    """Segment simple tag

    """
    def __init__(self, parent, prefix):
        super(SimpleTagSection, self).__init__(parent, 'SimpleTag', prefix)



class Track(object):
    """Common track object

    Arguments:
    - matroska: Matroska object
    - section: configuration section in mkvinfo details
    """
    def __init__(self, matroska, section):
        self.matroska = matroska
        self.section = section

        for key in ( 'type', 'tracknumber', ):
            if key not in self.section:
                raise ValueError('Missing key {0}: {1}'.format(key, self.section.items()))

    def __repr__(self):
        return '{0} {1} {2}'.format(self.matroska.path, self.section['tracknumber'], self.codec)

    @property
    def id(self):
        """Track ID

        Track ID for mkvextract command arguments, based on tracknumber
        """
        return self.section['tracknumber'] - 1

    @property
    def codec(self):
        """Codec details

        Codec string from mkvinfo section
        """
        return self.section['codec']


class VideoTrack(Track):
    """Video track

    TODO - do something sensible with this.
    """
    pass


class AudioTrack(Track):
    """Video track

    Converter for audio track based on mkvinfo track details
    """

    @property
    def codec(self):
        """Codec extension

        Return codec extension matching mkvinfo details.
        """
        try:
            return AUDIO_FORMAT_MAP[self.section['codec']]
        except KeyError:
            raise ValueError('Unknown audio format: {0}'.format(self.section['codec']))

    def extract(self, directory):
        """Extract audio

        Extract audio file to given directory. Filename is determined from
        input file name, replacing extension with audio codec extension.
        """
        outputfile = os.path.join(
            directory,
            '{0}.{1}'.format(os.path.basename(os.path.splitext(self.matroska.path)[0]), self.codec),
        )

        if os.path.isfile(outputfile):
            return

        cmd = (
            'mkvextract',
            'tracks',
            self.matroska.path,
            '{0:d}:{1}'.format(self.id, outputfile),
        )
        p = Popen(cmd, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr)
        p.wait()


class MkvInfo(object):
    """Parse mkvinfo

    Parser for mkvinfo output for .mkv files
    """
    def __init__(self, matroska):
        self.matroska = matroska
        self.children = []
        self.tracks = []
        self.tags = []
        self.load()

    def load(self):
        """Load mkvinfo output

        Note: not all fields are imported. We are mainly interested about
        audio tracks here.
        """
        def parse_section(parent, name, prefix=0):
            if name == 'EBML head':
                return HeadSection(self, name, prefix)

            if isinstance(parent, SegmentSection) and name == 'Segment information':
                return SegmentInformationSection(parent, prefix)

            if isinstance(parent, SegmentSection) and name == 'Segment tracks':
                return TrackListSection(parent, prefix)

            if isinstance(parent, TrackListSection) and name == 'A track':
                return TrackSection(parent, prefix)

            if isinstance(parent, TrackSection) and name in ('Video track', 'Audio track', ):
                return TrackCodecDetailsSection(parent, prefix)

            if isinstance(parent, SegmentSection) and name == 'Tags':
                return TagsSection(parent, prefix)

            if isinstance(parent, TagsSection) and name == 'Tag':
                return TagSection(parent, prefix)

            if isinstance(parent, TagSection) and name == 'Targets':
                return TagTargetsSection(parent, prefix)

            if isinstance(parent, TagSection) and name == 'Simple':
                return SimpleTagSection(parent, prefix)

            m = RE_SEGMENT_SECTION.match(name)
            if m:
                return SegmentSection(parent, prefix=prefix, **m.groupdict())

            m = RE_SEEK_HEAD_SECTION.match(name)
            if m:
                return SeekHeadSection(parent, prefix=prefix)

            return EBMLSection(parent, name, prefix)

        p = Popen(('mkvinfo', self.matroska.path), stdin=PIPE, stdout=PIPE, stderr=PIPE)
        stdout, stderr = p.communicate()

        if p.returncode != 0:
            raise ConverterError('{0}\n{1}'.format(stdout.rstrip(), stderr.rstrip()))

        section = None
        for line in stdout.splitlines():
            m = RE_MATROSKA_SECTION_HEADER.match(line)
            if m:
                section = parse_section(self, **m.groupdict())
                self.children.append(section)

            elif line.startswith('|'):
                prefix = len(line[1:line.index('+')]) + 1
                entry = line[line.index('+')+1:].lstrip()

                try:
                    key, value = (x.strip() for x in entry.split(':', 1))
                    if key.count('('):
                        raise ValueError
                    section[key] = value

                except ValueError:
                    parent = section
                    if prefix <= parent.prefix:
                        parent = parent.parent
                        while prefix <= parent.prefix:
                            parent = parent.parent
                            if parent is None:
                                break
                    section = parse_section(parent, entry, prefix)

class Matroska(object):
    """Matroska container

    Matroska (.mkv) container files
    """
    def __init__(self, path):
        self.path = path
        self.info = MkvInfo(self)

    @property
    def tags(self):
        return self.info.tags

    @property
    def audiotracks(self):
        """Audio tracks

        Return audio tracks in matroska container
        """
        return [AudioTrack(self, track) for track in self.info.tracks if track['type'] == 'audio']

    @property
    def videotracks(self):
        """Container videotracks

        Return video tracks in matroska container
        """
        return [VideoTrack(self, track) for track in self.info.tracks if track['type'] == 'video']
