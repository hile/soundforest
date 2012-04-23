"""
Tagging utilities for file formats in soundforest.

Includes
"""

__all__ = ['albumart','db','constants','formats']

import os

from systematic.shell import normalized
from soundforest.tags.db import TagsDB,TagError
from soundforest.codecs import CodecDB,CodecError
from soundforest.tags.albumart import AlbumArt,AlbumArtError
from soundforest.tags.constants import STANDARD_TAG_ORDER

TAG_FORMATTERS = {
    'aac':      'soundforest.tags.formats.aac.AAC',
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
                        value = unicode(value,'utf-8')
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
        item = self.__tag2fields__(item)
        if not self.entry.has_key(item):
            return
        del self.entry[item]
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
        self.path = path

        if tags_db is None:
            try:
                tags_db = TagsDB()
            except TagError,emsg:
                raise TagError(
                    'Error initializing tags database: %s' % emsg
                )
        self.tags_db = tags_db

        if codec_db is None:
            try:
                codec_db = CodecDB()
            except CodecError,emsg:
                raise TagError(
                    'Error initializing codec database: %s' % emsg
                )
        self.codec_db = codec_db

    def update(self,tags=None,append=False):
        """
        Update file and database tags from tags given, and sync file
        tags to database.
        Updated tags replaces all existing tags, unless append is True
        """
        if tags is None:
            tags = {}
        if not isinstance(tags,dict):
            raise TagError('Updated tags must be instance of dict')
        codec = self.codec_db.match(self.path)
        if codec is None:
            raise TagError('No codec configured for %s' % self.path)
        try:
            classpath = TAG_FORMATTERS[codec.name]
            module_path = '.'.join(classpath.split('.')[:-1])
            class_name = classpath.split('.')[-1]
            m = __import__(module_path,globals(),fromlist=[class_name])
            file_tags = getattr(m,class_name)(codec,self.path)
        except KeyError:
            raise TagError('No tag parser configured for %s' % self.path)

        all_tags = dict(**file_tags.items())
        for tag,values in all_tags.items():
            if not tags.has_key(tag):
                continue

        # TODO - do the actual update...


if __name__ == '__main__':
    import sys
    for f in sys.argv[1:]:
        try:
            t = Tags(f)
            for k,v in t.items():
                print '%14s %s' % (k,v)
        except TagError,emsg:
            print emsg
