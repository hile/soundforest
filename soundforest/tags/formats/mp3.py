# coding=utf-8
"""mp3 tags

mp3 file tag parser

"""

from mutagen.mp3 import MP3
from mutagen.id3 import ID3, APIC, ID3NoHeaderError
from mutagen.id3 import error as ID3Error

from soundforest.tags import TagError, format_unicode_string_value
from soundforest.tags.tagparser import TagParser, TrackNumberingTag, TrackAlbumart
from soundforest.tags.albumart import AlbumArt, AlbumArtError

MP3_ALBUMART_TAG = ''
MP3_ALBUMART_PIL_FORMAT_MAP = {
    # TODO - check these values
    'JPEG':     'image/jpeg',
    'PNG':      'image/png'
}

MP3_STANDARD_TAGS = {
    'album_artist':         ['TIT1'],
    'artist':               ['TPE1'],
    'composer':             ['TCOM'],
    'conductor':            ['TPE3'],
    'orchestra':            ['TPE2'],
    'performers':           ['TMCL'],
    'album':                ['TALB'],
    'title':                ['TIT2'],
    'genre':                ['TCON'],
    'comment':              ["COMM::'eng'"],
    'note':                 ['TXXX'],
    'description':          ['TIT3'],
    'year':                 ['TDRC'],
    'bpm':                  ['TBPM'],
    'label':                ['TPUB'],
    'copyright':            ['WCOP'],
    'license':              ['TOWN'],
    'sort_artist':          ['TSOP'],
    'sort_album':           ['TSOA'],
    'sort_title':           ['TSOT'],
}

MP3_TAG_FORMATTERS = {

}


def encode_frame(tag, value):
    """
    Return a mp3 frame object matching tag
    """
    try:
        m = __import__('mutagen.id3', globals(), {}, tag)
        tagclass = getattr(m, tag)
        if tagclass is None:
            raise AttributeError
    except AttributeError as e:
        raise TagError('Error importing ID3 frame {}: {}'.format(tag, e))
    if not isinstance(value, list):
        value = [value]
    return tagclass(encoding=3, text=value)


class MP3AlbumArt(TrackAlbumart):
    """
    Encoding of mp3 albumart to APIC tags
    """
    def __init__(self, track):
        if not isinstance(track, mp3):
            raise TagError('Track is not instance of mp3')

        super(MP3AlbumArt, self).__init__(track)

        try:
            self.tag = [k for k in self.track.entry.keys() if k[:5] == 'APIC:'][0]
        except IndexError:
            self.tag = None
            return

        try:
            albumart = AlbumArt()
            albumart.import_data(self.track.entry[self.tag].data)
        except AlbumArtError as e:
            raise TagError('Error reading mp3 albumart tag: {}'.format(e))
        self.albumart = albumart

    def import_albumart(self, albumart):
        """
        Imports albumart object to the file tags.

        Sets self.track.modified to True
        """
        super(MP3AlbumArt, self).import_albumart(albumart)
        frame = APIC(0, albumart.mimetype, 0, '', albumart.dump())
        self.track.entry.tags.add(frame)
        self.track.modified = True


class MP3NumberingTag(TrackNumberingTag):
    """
    mp3 track numbering field handling
    """
    def __init__(self, track, tag):
        super(MP3NumberingTag, self).__init__(track, tag)

        if self.tag not in self.track.entry:
            return

        try:
            self.value, self.total = self.track.entry[self.tag].text[0].split('/')
        except ValueError:
            self.value = self.track.entry[self.tag].text[0]
            self.total = self.value

        try:
            self.value = int(self.value)
            self.total = int(self.total)
        except ValueError:
            raise TagError('Unsupported tag value for {}: {}'.format(
                self.tag,
                self.track.entry[self.tag].text[0]),
            )

    def save_tag(self):
        """
        Export this numbering information back to mp3 tags
        """
        if self.f_value is None:
            return
        value = self.value
        total = self.total
        if total is None:
            total = value
        if total < value:
            raise ValueError('Total is smaller than number')

        value = '{:d}/{:d}'.format(value, total)
        if self.tag in self.track.entry.tags:
            old_value = self.track.entry.tags[self.tag]
            if value == old_value:
                return
            del(self.track.entry.tags[self.tag])

        frame = encode_frame(self.tag, value)
        self.track.entry.tags.add(frame)
        self.track.modified = True


class mp3(TagParser):
    """
    Class for processing mp3 tags
    """
    def __init__(self, codec, path):
        super(mp3, self).__init__(codec, path, tag_map=MP3_STANDARD_TAGS)

        try:
            self.entry = MP3(self.path, ID3=ID3)
        except IOError:
            raise TagError('No ID3 header in {}'.format(self.path))
        except ID3NoHeaderError:
            raise TagError('No ID3 header in {}'.format(self.path))
        except RuntimeError:
            raise TagError('Runtime error loading {}'.format(self.path))

        try:
            self.entry.add_tags()
        except ID3Error:
            pass

        self.albumart_obj = MP3AlbumArt(self)
        self.track_numbering = MP3NumberingTag(self, 'TRCK')
        self.disk_numbering = MP3NumberingTag(self, 'TPOS')

    def __getitem__(self, item):
        """
        Return tags formatted to str, decimal.Decimal or
        other supported types.
        Does not include albumart images, which are accessed
        by self.__getattr__('albumart')
        """
        if item == 'tracknumber':

            return [format_unicode_string_value('{:d}'.format(self.track_numbering.value))]

        if item == 'totaltracks':
            return [format_unicode_string_value('{:d}'.format(self.track_numbering.total))]

        if item == 'disknumber':
            return [format_unicode_string_value('{:d}'.format(self.disk_numbering.value))]

        if item == 'totaldisks':
            return [format_unicode_string_value('{:d}'.format(self.disk_numbering.total))]

        if item[:5] == 'APIC:':
            return self.albumart_obj

        fields = self.__tag2fields__(item)

        for field in fields:
            if field not in self.entry:
                continue

            tag = self.entry[field]
            if not isinstance(tag, list):
                tag = [tag]

            values = []
            for value in tag:
                matched = False

                for field in ['text', 'url']:
                    if hasattr(value, field):
                        value = getattr(value, field)
                        if isinstance(value, list):
                            value = value[0]

                        matched = True
                        break

                if value is None:
                    continue

                if not matched:
                    raise TagError('Error parsing {}: {}'.format(tag, dir(value)))

                if not isinstance(value, str):
                    try:
                        value = '{:d}'.format(int(format_unicode_string_value(value)))
                    except ValueError:
                        pass

                    try:
                        value = format_unicode_string_value(value)
                    except UnicodeDecodeError as e:
                        raise TagError('Error decoding {} tag {}: {}'.format(self.path, field, e))

                values.append(value)

            return values

        raise KeyError('No such tag: {}'.format(fields))

    def __delitem__(self, item):
        tags = self.__tag2fields__(item)
        item = tags[0]
        for t in tags:
            if t in self.entry.tags:
                del(self.entry.tags[t])
                self.modified = True

    def keys(self):
        """
        Return tag names sorted with self.sort_keys()

        Itunes internal tags are ignored from results
        """
        keys = super(mp3, self).keys()
        if 'TRCK' in keys:
            keys.extend(['tracknumber', 'totaltracks'])
            keys.remove('TRCK')
        if 'TPOS' in keys:
            keys.extend(['disknumber', 'totaldisks'])
            keys.remove('TPOS')
        for k in keys:
            if k[:5] == 'APIC:':
                keys.remove(k)
        return self.sort_keys(keys)

    def set_tag(self, item, value):
        """
        Set given tag value to mp3
        """

        if item == 'tracknumber':
            self.track_numbering.value = value
            return

        if item == 'totaltracks':
            self.track_numbering.total = value
            return

        if item == 'disknumber':
            self.disk_numbering.value = value
            return

        if item == 'totaldisks':
            self.disk_numbering.total = value
            return

        if isinstance(value, list):
            value = value[0]

        tags = self.__tag2fields__(item)
        item = tags[0]
        for tag in tags[1:]:
            if tag in self.entry.tags:
                del(self.entry.tags[tag])

        if item in MP3_TAG_FORMATTERS:
            value = MP3_TAG_FORMATTERS[item](value)
        else:
            value = format_unicode_string_value(value)

        if item in self.entry:
            old_value = self.entry[item]
            if value == old_value:
                return
            del(self.entry.tags[item])

        frame = encode_frame(item, value)
        self.entry.tags.add(frame)
        self.modified = True
