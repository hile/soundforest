# coding=utf-8
"""vorbis tags

Vorbis file tag parser

"""

from mutagen.oggvorbis import OggVorbis, OggVorbisHeaderError

from soundforest.tags import TagError
from soundforest.tags.constants import OGG_MULTIPLE_VALUES_TAGS
from soundforest.tags.tagparser import TagParser, TrackNumberingTag, TrackAlbumart
from soundforest.tags.albumart import AlbumArt, AlbumArtError

VORBIS_ALBUMART_TAG = 'METADATA_BLOCK_PICTURE'

VORBIS_STANDARD_TAGS = {
    'album_artist':         ['ALBUM_ARTIST'],
    'artist':               ['ARTIST'],
    'arranger':             ['ARRANGER'],
    'author':               ['AUTHOR'],
    'composer':             ['COMPOSER'],
    'conductor':            ['CONDUCTOR'],
    'ensemble':             ['ENSEMBLE'],
    'orchestra':            ['ORCHESTRA'],
    'performer':            ['PERFORMER'],
    'publisher':            ['PUBLISHER'],
    'lyricist':             ['LYRICIST'],
    'album':                ['ALBUM'],
    'title':                ['TITLE'],
    'partnumber':           ['PARTNUMBER'],
    'tracknumber':          ['TRACKNUMBER'],
    'disknumber':           ['DISKNUMBER'],
    'genre':                ['GENRE'],
    'comment':              ['COMMENT'],
    'note':                 ['NOTE'],
    'description':          ['DESCRIPTION'],
    'location':             ['LOCATION'],
    'year':                 ['DATE'],
    'bpm':                  ['BPM'],
    'rating':               ['RATING'],
    'label':                ['LABEL'],
    'labelno':              ['LABELNO'],
    'opus':                 ['OPUS'],
    'isrc':                 ['ISRC'],
    'ean':                  ['EAN/UPN'],
    'lyrics':               ['LYRICS'],
    'website':              ['WEBSITE'],
    'copyright':            ['COPYRIGHT'],
    'version':              ['VERSION'],
    'sourcemedia':          ['SOURCEMEDIA'],
    'encoding':             ['ENCODING'],
    'encoded_by':           ['ENCODED-BY'],
    'sort_album_artist':    ['SORT_ALBUM_ARTIST'],
    'sort_artist':          ['SORT_ARTIST'],
    'sort_composer':        ['SORT_COMPOSER'],
    'sort_performer':       ['SORT_PERFORMER'],
    'sort_show':            ['SORT_SHOW'],
    'sort_album':           ['SORT_ALBUM'],
    'sort_title':           ['SORT_TITLE'],
}

VORBIS_REPLAYGAIN_TAGS = {
    'album_gain':           ['REPLAYGAIN_ALBUM_GAIN'],
    'album_peak':           ['REPLAYGAIN_ALBUM_PEAK'],
    'track_gain':           ['REPLAYGAIN_TRACK_GAIN'],
    'track_peak':           ['REPLAYGAIN_TRACK_PEAK'],
}

VORBIS_TAG_FORMATTERS = {

}

VORBIS_EXTRA_TAGS = {
}

class VorbisNumberingTag(TrackNumberingTag):
    """
    Vorbis tags for storing track or disk numbers.
    The tag can be either a single number or two numbers separated by /
    If total is given, the value must be integer.
    """
    def __init__(self, track, tag):
        super(VorbisNumberingTag, self).__init__(track, tag)

        if not isinstance(track, vorbis):
            raise TagError('Track is not instance of vorbis')

        if not self.track.entry.has_key(self.tag):
            return

        value = self.track.entry[self.tag]
        try:
            value, total = value[0].split('/', 1)
        except ValueError:
            total = None
        self.value = value
        self.total = total

    def save_tag(self):
        """
        Set new numbering information to vorbis tags, marking file
        dirty to require saving but not saving tags.
        """
        value = self.__repr__()
        if value is not None:
            self.track.entry[self.tag] = '{0}'.format(value)
            self.track.modified = True

class vorbis(TagParser):
    """
    Class for processing Ogg Vorbis file tags
    """
    def __init__(self, codec, path):
        super(vorbis, self).__init__(codec, path, tag_map=VORBIS_STANDARD_TAGS)

        try:
            self.entry = OggVorbis(path)
        except IOError, emsg:
            raise TagError('Error opening {0}: {1}'.format(path, str(emsg)))
        except OggVorbisHeaderError, emsg:
            raise TagError('Error opening {0}: {1}'.format(path, str(emsg)))

        self.albumart_obj = None
        self.track_numbering = VorbisNumberingTag(self, 'TRACKNUMBER')
        self.disk_numbering = VorbisNumberingTag(self, 'DISKNUMBER')

    def __getitem__(self, item):
        if item == 'tracknumber':
            return [unicode('{0:d}'.format(self.track_numbering.value))]
        if item == 'totaltracks':
            return [unicode('{0:d}'.format(self.track_numbering.total))]
        if item == 'disknumber':
            return [unicode('{0:d}'.format(self.disk_numbering.value))]
        if item == 'totaldisks':
            return [unicode('{0:d}'.format(self.disk_numbering.total))]

        return super(vorbis, self).__getitem__(item)

    def __delitem__(self, item):
        try:
            item, value = item.split('=', 1)
        except ValueError:
            value = None

        fields = self.__tag2fields__(item)
        for tag in fields:
            tag = self.__field2tag__(tag)
            if not self.has_key(tag):
                continue

            if value is None:
                del self.entry[tag]
                self.modified = True

            elif value in self.entry[tag]:
                self.entry[tag] = [x for x in self.entry[tag] if x != value]

                if not self.entry[tag]:
                    del self.entry[tag]

                self.modified = True

    def __field2tag__(self, field):
        return super(vorbis, self).__field2tag__(field.upper())

    def keys(self):
        """
        Return tag names sorted with self.sort_keys()
        """

        keys = super(vorbis, self).keys()

        if 'TOTALTRACKS' in keys:
            keys.remove('TOTALTRACKS')

        if 'TOTALDISKS' in keys:
            keys.remove('TOTALDISKS')

        if 'TRACKNUMBER' in [x.upper() for x in keys]:
            if self.track_numbering.total is not None:
                keys.append('totaltracks')

        if 'DISKNUMBER' in [x.upper() for x in keys]:
            if self.disk_numbering.total is not None:
                keys.append('totaldisks')

        if VORBIS_ALBUMART_TAG in [x.upper() for x in keys]:
            keys.remove(VORBIS_ALBUMART_TAG)

        for replaygain_tag_fields in VORBIS_REPLAYGAIN_TAGS.values():
            for tag in replaygain_tag_fields:
                if tag in keys:
                    keys.remove(tag)

        return [x for x in self.sort_keys(keys)]

    def items(self):
        return [(key, self[key]) for key in self.keys()]

    def has_key(self,  tag):
        if tag is None:
            return False
        return tag.lower() in self.keys()

    def set_tag(self, item, value):
        """
        All vorbis tags are unicode strings, and there can be multiple
        tags with same name.

        We do special precessing for track and disk numbering.
        """
        if item == 'tracknumber':
            self.track_numbering.value = value
            self.modified = True
            return

        if item == 'totaltracks':
            self.track_numbering.total = value
            self.modified = True
            return

        if item == 'disknumber':
            self.disk_numbering.value = value
            self.modified = True
            return

        if item == 'totaldisks':
            self.disk_numbering.total = value
            self.modified = True
            return

        if not isinstance(value, list):
            value = [value]

        value = [x for x in set(value)]

        tags = self.__tag2fields__(item)
        item = tags[0]

        for tag in tags:
            if self.entry.has_key(tag):
                if tag.lower() in OGG_MULTIPLE_VALUES_TAGS:
                    value = set(self.entry[tag] + value)

                del self.entry[tag]

        entries =[]
        for v in value:
            if VORBIS_TAG_FORMATTERS.has_key(item):
                entries.append(VORBIS_TAG_FORMATTERS[item](v))

            else:
                if not isinstance(v, unicode):
                    v = unicode(v, 'utf-8')
                entries.append(v)

        self.entry[item] = entries
        self.modified = True
