# coding=utf-8
"""
AAC file tag parser
"""

import base64,struct

from mutagen.mp4 import MP4,MP4Cover,MP4StreamInfoError,MP4MetadataValueError

from soundforest.tags import TagParser
from soundforest.tags.albumart import AlbumArt,AlbumArtError
from soundforest.tags.db import base64_tag,TagError

# Albumart filed processing
AAC_ALBUMART_TAG = 'covr'
AAC_ALBUMART_PIL_FORMAT_MAP = {
    'JPEG':     MP4Cover.FORMAT_JPEG,
    'PNG':      MP4Cover.FORMAT_PNG
}

AAC_STANDARD_TAGS = {
    'album_artist':         ['aART'],
    'artist':               ['\xa9ART'],
    'composer':             ['\xa9wrt'],
    'conductor':            ['cond'],
    'orchestra':            ['orch'],
    'performers':           ['ense'],
    'album':                ['\xa9alb'],
    'title':                ['\xa9nam'],
    'genre':                ['\xa9gen'],
    'comment':              ['\xa9cmt'],
    'note':                 ['note'],
    'description':          ['desc'],
    'location':             ['loca'],
    'year':                 ['\xa9day'],
    'bpm':                  ['tmpo'],
    'rating':               ['rati'],
    'label':                ['labe'],
    'copyright':            ['cprt'],
    'license':              ['lice'],
    'sort_album_artist':    ['soaa'],
    'sort_artist':          ['soar'],
    'sort_composer':        ['soco'],
    'sort_performers':      ['sopr'],
    'sort_show':            ['sosn'],
    'sort_album':           ['soal'],
    'sort_title':           ['sonm'],
}

# Internal program tags for itunes. Ignored by current code
ITUNES_TAG_MAP = {
    # Indicates the encoder command used to encode track
    'encoder':              ['\xa9too','enco'],
    # Boolean flag indicating if track is part of compilation
    'compilation':          ['cpil'],
    # iTunes grouping flag
    'grouping':             ['\xa9grp'],
    # iTunes encoder and normalization data
    'itunes_encoder':       ['----:com.apple.iTunes:cdec'],
    'itunes_normalization': ['----:com.apple.iTunes:iTunNORM'],
    # NO idea what this is
    'itunes_smbp':          ['----:com.apple.iTunes:iTunSMPB'],
    # iTunes store purchase details
    'purchase_date':        ['purd'],
    'purchaser_email':      ['apID'],
    # Tags for video shows
    'video_show':           ['tvsh'],
    'video_episode':        ['tven'],
    # XID is internal itunes metadata reference
    'xid':                  ['xid'],
}

AAC_UNOFFICIAL_TAGS = {
    # String containing exact command used to encode the file
    'encoder_command':      ['encoder_command'],
    # Musicbrainz ID reference
    'musicbrainz_id':       ['musi'],
}

AAC_INTEGER_TUPLE_TYPES = [ 'trkn', 'disk' ]

# Placeholder to write lambda functions to process specific tags if needed
AAC_TAG_FORMATTERS = {

}

class AACAlbumArt(object):
    """
    Thin wrapper to process AAC object albumart files

    Technically supports setting albumart to other tags than
    standard AAC_ALBUMART_TAG. Don't do this, not tested.
    """
    def __init__(self,track,tag=AAC_ALBUMART_TAG):
        if not isinstance(track,AAC):
            raise TagError('Entry is not instance of AAC')
        self.track = track
        self.tag = tag
        self.modified = False
        self.albumart = None

        if not self.track.entry.has_key(self.tag):
            return
        try:
            albumart = AlbumArt()
            albumart.import_data(self.track.entry[self.tag][0])
        except AlbumArtError,emsg:
            raise TagError('Error reading AAC albumart tag: %s' % emsg)
        self.albumart = albumart

    def as_base64_tag(self):
        """
        Return albumart image data as base64_tag tag
        """
        if self.albumart is None:
            raise TagError('Albumart is not loaded')
        return base64_tag(base64.b64encode(self.albumart.dump()))

    def defined(self):
        """
        Returns True if albumart is defined, False otherwise
        """
        if self.albumart is None:
            return False
        return True

    def import_albumart(self,albumart):
        """
        Imports albumart object to the file tags.

        Sets self.track.modified to True
        """
        if not isinstance(albumart,AlbumArt):
            raise TagError('Albumart must be AlbumArt instance')
        self.albumart = albumart

        try:
            img_format = AAC_ALBUMART_PIL_FORMAT_MAP[self.albumart.out_format]
        except KeyError:
            raise TagError(
                'Unsupported albumart format %s' % self.albumart.out_format
            )
        try:
            tag = MP4Cover(data=self.albumart.dump(),imageformat=img_format)
        except MP4MetadataValueError,emsg:
            raise TagError('Error encoding albumart: %s' % emsg)

        # Remove existing albumart entry
        if self.track.entry.has_key(self.tag):
            del self.track.entry[self.tag]
        # Store new albumart tag entry
        self.track.entry[self.tag] = [tag]
        self.track.modified = True

class AACIntegerTuple(object):
    """
    AAC field for ('item','total items') type items in tags.
    Used for track and disk numbering
    """
    def __init__(self,track,tag):
        self.track = track
        self.tag = tag
        self.__value = None
        self.__total = None

        if not self.track.entry.has_key(self.tag):
            return
        self.__value,self.__total = self.track.entry[self.tag][0]

    def __getattr__(self,attr):
        if attr == 'value':
            return self.__value
        if attr == 'total':
            return self.__total
        raise AttributeError('No such AACIntegerTuple attribute: %s' % attr)

    def __setattr__(self,attr,value):
        if attr in ['value','total']:
            try:
                value = int(value)
            except ValueError:
                raise TagError('AACIntegerTuple values must be integers')
            if attr == 'value':
                self.__value = value
            if attr == 'total':
                self.__total = value
            self.save_tag()
        else:
            object.__setattr__(self,attr,value)

    def save_tag(self):
        """
        Export this numbering information back to AAC tags.

        If value is None, ignore both values without setting tag.
        If total is None but value is set, set total==value.
        """
        if self.__value is None:
            return
        value = self.__value
        total = self.__total
        if total is None:
            total = value
        if total < value:
            raise ValueError('Total is smaller than number')
        self.track.entry[self.tag] = [(value,total)]

class AAC(TagParser):
    """
    Class for processing AAC file tags
    """
    def __init__(self,codec,path):
        TagParser.__init__(self,codec,path,tag_map=AAC_STANDARD_TAGS)

        try:
            self.entry = MP4(self.path)
        except IOError,e:
            raise TagError('Error opening %s: %s' % (path,str(e)))
        except MP4StreamInfoError,e:
            raise TagError('Error opening %s: %s' % (path,str(e)))
        except struct.error:
            raise TagError('Invalid tags in %s' % path)
        except RuntimeError,e:
            raise TagError('Error opening %s: %s' % (path,str(e)))

        self.albumart = AACAlbumArt(self)
        self.track_numbering = AACIntegerTuple(self,'trkn')
        self.disk_numbering = AACIntegerTuple(self,'disk')

    def __getitem__(self,item):
        if item == 'tracknumber':
            return self.track_numbering.value
        if item == 'totaltracks':
            return self.track_numbering.total
        if item == 'disknumber':
            return self.disk_numbering.value
        if item == 'totaldisks':
            return self.disk_numbering.total
        return TagParser.__getitem__(self,item)

    def keys(self):
        """
        Return tag names sorted with self.sort_keys()

        Itunes internal tags are ignored from results
        """
        keys = TagParser.keys(self)
        if 'trkn' in keys:
            keys.extend(['tracknumber','totaltracks'])
            keys.remove('trkn')
        if 'disk' in keys:
            keys.extend(['disknumber','totaldisks'])
            keys.remove('disk')
        if 'covr' in keys:
            keys.remove('covr')
        for itunes_tags in ITUNES_TAG_MAP.values():
            for tag in itunes_tags:
                if tag in keys:
                    keys.remove(tag)
        return self.sort_keys(keys)

    def set_tag(self,item,value):
        """
        Set given tag to correct type of value in tags.

        Normal tag values in AAC tags are always a list
        and you can pass this function a list to set all values.

        Tracknumber, totaltracks, disknumber and totaldisks
        attributes must be integers.

        Existing tag value list is replaced.
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

        if not isinstance(value,list):
            value = [value]

        item = self.__tag2fields__(item)
        entries =[]
        for v in value:
            if AAC_TAG_FORMATTERS.has_key(item):
                entries.append(AAC_TAG_FORMATTERS[item](v))
            else:
                if not isinstance(v,unicode):
                    v = unicode(v,'utf-8')
                entries.append(v)
        self.entry[item] = entries
        self.modified = True

    def save(self):
        """
        Save AAC tags to the file
        """
        TagParser.save(self)
