"""
Tag metadata reader and writer classes
"""

import os

from systematic.shell import normalized
from soundforest.codecs import CodecDB,CodecError
from soundforest.tags import TagError
from soundforest.tags.db import FileTags,TagsDB
from soundforest.tags.albumart import AlbumArt,AlbumArtError
from soundforest.tags.constants import STANDARD_TAG_ORDER

__all__ = ['aac']

TAG_FORMATTERS = {
    'aac':      'soundforest.tags.formats.aac.AAC',
    'mp3':      'soundforest.tags.formats.mp3.MP3',
    'flac':     'soundforest.tags.formats.flac.FLAC',
}

class TagParser(dict):
    """
    Parent class for tag parser implementations
    """
    def __init__(self,codec,path,tag_map=None):
        dict.__init__(self)
        self.codec = codec
        self.name = codec.name
        self.path = normalized(os.path.realpath(path))
        self.tag_map = tag_map is not None and tag_map or {}
        self.supports_albumart = False
        self.entry = None
        self.modified = False

    def __getattr__(self,attr):
        """
        Extend attributes to access tags
        """
        if attr == 'unknown_tags':
            # Must be implemented in child if needed: return empty list here
            return []
        try:
            return self[attr]
        except KeyError:
            pass

        raise AttributeError('No such TagParser attribute: %s' % attr)

    def __getitem__(self,item):
        """
        Return tags formatted to unicode, decimal.Decimal or
        other supported types.
        Does not include albumart images, which are accessed
        by self.__getattr__('albumart')
        """
        fields = self.__tag2fields__(item)
        for field in fields:
            if not self.entry.has_key(field):
                continue
            tag = self.entry[field]
            if not isinstance(tag,list):
                tag = [tag]
            values = []
            for value in tag:
                if not isinstance(value,unicode):
                    if isinstance(value,int):
                        value = unicode('%d'%value)
                    else:
                        try:
                            value = unicode(value,'utf-8')
                        except UnicodeDecodeError,emsg:
                            raise TagError('Error decoding %s tag %s: %s' % (self.path,field,emsg))
                values.append(value)
            return values
        raise KeyError('No such tag: %s' % fields)

    def __setitem__(self,item,value):
        if isinstance(item,AlbumArt):
            try:
                self.albumart.import_albumart(value)
            except AlbumArtError,emsg:
                raise TagError('Error setting albumart: %s' % emsg)
        self.set_tag(item,value)

    def __delitem__(self,item):
        fields = self.__tag2fields__(item)
        for tag in fields:
            if tag not in self.entry.keys():
                continue
            print 'REMOVING %s tag "%s"' % (self.path,tag)
            del self.entry[tag]
            self.modified = True

    def __tag2fields__(self,tag):
        """
        Resolve tag name to internal parser field
        """
        for name,tags in self.tag_map.items():
            if tag == name:
                return tags
        return [tag]

    def __field2tag__(self,field):
        """
        Resolve internal parser field to tag name
        """
        for name,tags in self.tag_map.items():
            # Can happen if name is internal reference: ignore here
            if tags is None:
                continue
            if field in tags:
                return name
        return field

    def __repr__(self):
        return '%s: %s' % (self.codec.description,self.path)

    #noinspection PyUnusedLocal
    def set_tag(self,item,value):
        """
        Sets the tag item to given value.
        Must be implemented in child class, this raises
        NotImplementedError
        """
        raise NotImplementedError('Must implement set_tag in child')

    def sort_keys(self,keys):
        """
        Sort keys with STANDARD_TAG_ORDER list
        """
        values = []
        for k in STANDARD_TAG_ORDER:
            if k in keys: values.append(k)
        for k in keys:
            if k not in STANDARD_TAG_ORDER: values.append(k)
        return values

    def has_key(self,key):
        """
        Test if given key is in tags
        """
        keys = self.__tag2fields__(key)
        for k in keys:
            if k in self.entry.keys():
                return True
        return False

    def keys(self):
        """
        Return file tag keys mapped with tag_map.
        """
        return self.sort_keys(
            [self.__field2tag__(k) for k in self.entry.keys()]
        )

    def items(self):
        """
        Return tag,value pairs using tag_map keys.
        """
        return [(self.__field2tag__(k),self[k]) for k in self.keys()]

    def values(self):
        """
        Return tag values from entry
        """
        return [self[k] for k,v in self.keys()]

    def remove_unknown_tags(self):
        """
        Remove any tags which we don't know about.
        """
        for tag in self.unknown_tags:
            del self[tag]
        if self.modified:
            self.save()

    def save(self):
        """
        Save tags to file.
        """
        if not self.modified:
            return
        # TODO - replace with copying of file to new inode
        self.entry.save()

class Tags(dict):
    """
    Loader for file metadata tags. Tag reading and writing for various
    file formats is implemented by tag formatter classes in module
    soundforest.tags.formats, initialized automatically by this class.
    """
    def __init__(self,path,tags_db=None,codec_db=None):
        dict.__init__(self)
        if not os.path.isfile(path):
            raise TagError('No such file: %s' % path)
        self.path = normalized(os.path.realpath(path))

        if codec_db is None:
            try:
                codec_db = CodecDB()
            except CodecError,emsg:
                raise TagError(
                    'Error initializing codec database: %s' % emsg
                )
        self.codec_db = codec_db

        if tags_db is None:
            try:
                tags_db = TagsDB()
            except TagError,emsg:
                raise TagError(
                    'Error initializing tags database: %s' % emsg
                )
        self.tags_db = tags_db

    def __getattr__(self,attr):
        if attr == 'db_tags':
            return FileTags(self.tags_db,self.path)
        if attr == 'mtime':
            return os.stat(self.path).st_mtime
        if attr == 'file_tags':
            codec = self.codec_db.match(self.path)
            if codec is None:
                raise TagError('No codec configured for %s' % self.path)
            try:
                classpath = TAG_FORMATTERS[codec.name]
                module_path = '.'.join(classpath.split('.')[:-1])
                class_name = classpath.split('.')[-1]
                m = __import__(module_path,globals(),fromlist=[class_name])
                return getattr(m,class_name)(codec,self.path)
            except KeyError:
                raise TagError('No tag parser configured for %s' % self.path)
        raise AttributeError('No such Tags attribute: %s' % attr)

    def remove(self,tags):
        """
        Try to remove given tag from file tags
        """
        file_tags = self.file_tags
        for tag in tags:
            if not file_tags.has_key(tag):
                continue
            del file_tags[tag]
            file_tags.save()

    def update_tags(self,tags=None,append=False):
        """
        Update file and database tags from tags given, and sync file
        tags to database.
        Updated tags replaces all existing tags, unless append is True
        """
        db_tags = self.db_tags
        if tags is None:
            tags = {}

            # No new tags: check if we need to do anything
            if db_tags.mtime>=self.mtime:
                return

        if not isinstance(tags,dict):
            raise TagError('Updated tags must be instance of dict')

        all_tags = dict(self.file_tags.items())
        for tag,values in all_tags.items():
            if not tags.has_key(tag):
                continue
            if append:
                all_tags[tag].extend(values)
            else:
                all_tags[tag] = values
        self.db_tags.update_tags(all_tags,mtime=self.mtime)
