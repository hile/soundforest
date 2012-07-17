"""
Tag metadata reader and writer classes
"""

import os,base64

from systematic.shell import normalized
from soundforest.database.models import SoundForestDB
from soundforest.tags import TagError
from soundforest.tags.db import base64_tag,FileTags,TagsDB
from soundforest.tags.albumart import AlbumArt,AlbumArtError
from soundforest.tags.constants import STANDARD_TAG_ORDER

__all__ = ['aac','flac','mp3','vorbis']

TAG_FORMATTERS = {
    'aac':      'soundforest.tags.formats.aac.aac',
    'mp3':      'soundforest.tags.formats.mp3.mp3',
    'flac':     'soundforest.tags.formats.flac.flac',
    'vorbis':   'soundforest.tags.formats.vorbis.vorbis',
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
        return [(k,self[k]) for k in self.keys()]

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

class TrackAlbumart(object):
    """
    Parent class for common albumart operations
    """
    def __init__(self,track):
        self.track  = track
        self.modified = False
        self.albumart = None

    def __repr__(self):
        return self.albumart.__repr__()

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
        Parent method to set albumart tag. Child class must
        implement actual embedding of the tag to file.
        """
        if not isinstance(albumart,AlbumArt):
            raise TagError('Albumart must be AlbumArt instance')
        if not albumart.is_loaded:
            raise TagError('Albumart to import is not loaded with image.')
        self.albumart = albumart

class TrackNumberingTag(object):
    """
    Parent class for processing track numbering info, including track and
    disk numbers and total counts.

    Fields should be set and read from attributes 'value' and 'total'
    """
    def __init__(self,track,tag):
        self.track = track
        self.tag = tag
        self.f_value = None
        self.f_total = None

    def __repr__(self):
        if self.total is not None:
            return '%d/%d' % (self.value,self.total)
        else:
            return '%d' % (self.value)

    def __getattr__(self,attr):
        if attr == 'value':
            return self.f_value
        if attr == 'total':
            return self.f_total
        raise AttributeError('No such TrackNumberingTag attribute: %s' % attr)

    def __setattr__(self,attr,value):
        if attr in ['value','total']:
            if isinstance(value,list):
                value = value[0]
            try:
                if value is not None:
                    value = int(value)
            except ValueError:
                raise TagError('TrackNumberingTag values must be integers')
            except TypeError:
                raise TagError('TrackNumberingTag values must be integers')
            if attr == 'value':
                self.f_value = value
            if attr == 'total':
                self.f_total = value
            self.save_tag()
        else:
            object.__setattr__(self,attr,value)

    def save_tag(self):
        """
        Export this numbering information back to file tags.

        If value is None, ignore both values without setting tag.
        If total is None but value is set, set total==value.
        """
        raise NotImplementedError('save_tag must be implemented in child class')

class Tags(dict):
    """
    Loader for file metadata tags. Tag reading and writing for various
    file formats is implemented by tag formatter classes in module
    soundforest.tags.formats, initialized automatically by this class.
    """
    def __init__(self,path,db=None):
        dict.__init__(self)
        if not os.path.isfile(path):
            raise TagError('No such file: %s' % path)
        self.path = normalized(os.path.realpath(path))

        if db is None:
            db = SoundForestDB() 
        if not isinstance(db,SoundForestDB):
            raise SoundForestError('Not a soundforest database: %s' % db)
        self.db = db

        self.codec = self.db.match_codec(self.path)
        if self.codec is None:
            raise TagError('No codec configured for %s' % self.path)

    def __getattr__(self,attr):
        if attr == 'db_tags':
            return FileTags(self.db,self.path)
        if attr == 'mtime':
            return os.stat(self.path).st_mtime
        if attr == 'albumart':
            return self.file_tags.albumart
        if attr == 'file_tags':
            try:
                classpath = TAG_FORMATTERS[self.codec.name]
                module_path = '.'.join(classpath.split('.')[:-1])
                class_name = classpath.split('.')[-1]
                m = __import__(module_path,globals(),fromlist=[class_name])
                return getattr(m,class_name)(self.codec,self.path)
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
                pass
                #return

        if not isinstance(tags,dict):
            raise TagError('Updated tags must be instance of dict')

        file_tags = self.file_tags
        all_tags = dict(file_tags.copy().items())
        for tag,values in tags.items():
            if not isinstance(values,list):
                values = [values]
            if append:
                all_tags[tag].extend(values)
            elif all_tags.has_key(tag) and all_tags[tag] == tags[tag]:
                continue
            else:
                all_tags[tag] = values

        for tag,values in all_tags.items():
            if file_tags.has_key(tag) and file_tags[tag] == all_tags[tag]:
                continue
            file_tags[tag] = values
        if file_tags.modified:
            file_tags.save()

        self.db_tags.update_tags(all_tags,mtime=self.mtime)

    def keys(self):
        return self.file_tags.keys()

    def items(self):
        return self.file_tags.items()

    def values(self):
        return self.file_tags.values()

if __name__ == '__main__':
    import sys
    for f in sys.argv[1:]:
        print 'Processing: %s' % f
        t = Tags(f)
        t.update_tags(tags={
            'artist': 'Subsistence','album': 'Psytrance', 'title': 'Space Sphere',
            'totaltracks': 12,
        })
        t.file_tags.save()
        print t.albumart
        for k,v in t.file_tags.items():
            print '%16s %s' % (k,'.'.join(v))

