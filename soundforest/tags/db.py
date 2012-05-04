"""
Tag database for soundforest file tags

This is kept separate from trees to keep things clean. Database implementation
may of course use same backend if they wish.

Table labels:
describes tag labels and ordering, ordered by 'position' column.

Table file:
Maps source,path entries to tags database. Source is by default 'filesystem',
but you can set it to any string, for example 'traktor' or 'itunes'.

Table tag:
Maps tags for a single file to the 'file' table instance. Tags are updated
via 'FileTags' objects, and by default only single tag is stored: however,
it is possible to store multiple same tags for a file if you wish.
"""
import os,sqlite3,base64,decimal,time,logging

from systematic.shell import normalized
from systematic.sqlite import SQLiteDatabase,SQLiteError

from soundforest.tags import TagError
from soundforest.tags.constants import STANDARD_TAG_MAP,STANDARD_TAG_ORDER

TAGS_DB_PATH = os.path.expanduser('~/.soundforest/tags.sqlite')

DB_TABLES = [
"""
CREATE TABLE IF NOT EXISTS labels (
    id          INTEGER PRIMARY KEY,
    position    INTEGER,
    tag         TEXT UNIQUE,
    label       TEXT,
    description TEXT
);
""",
"""
CREATE UNIQUE INDEX IF NOT EXISTS unique_labels ON labels (tag,label);
""",
"""
CREATE TABLE IF NOT EXISTS file (
    id          INTEGER PRIMARY KEY,
    source      TEXT,
    path        TEXT,
    mtime       INTEGER
);
""",
"""
CREATE UNIQUE INDEX IF NOT EXISTS sourcefiles ON file (source,path);
""",
"""
CREATE TABLE IF NOT EXISTS tag (
    id          INTEGER PRIMARY KEY,
    file        INTEGER,
    tag         TEXT,
    value       TEXT,
    base64      BOOLEAN DEFAULT 0,
    FOREIGN KEY(file) REFERENCES file(id) ON DELETE CASCADE
);
""",
"""
CREATE UNIQUE INDEX IF NOT EXISTS filetags ON tag (file,tag,value);
""",
]

class TagsDB(object):
    """
    Database of file metadata tags, using singleton instances for different
    sqlite paths
    """
    __instance = None
    def __init__(self,db_path=TAGS_DB_PATH):
        if TagsDB.__instance is None:
            TagsDB.__instance = TagsDB.TagsDBInstance(db_path)
        self.__dict__['_TagsDB.__instance'] = TagsDB.__instance
        self.install_default_tag_labels()

    class TagsDBInstance(SQLiteDatabase):
        """
        Singleton instance of Tags database for given sqlite path
        """
        def __init__(self,db_path):
            try:
                SQLiteDatabase.__init__(self,db_path,tables_sql=DB_TABLES)
            except SQLiteError,emsg:
                raise TagError('Error intializing database %s: %s' %
                    (db_path,emsg)
                )

        def __getattr__(self,attr):
            try:
                return SQLiteDatabase.__getattr__(self,attr)
            except AttributeError:
                raise AttributeError('No such AudioTreeDB attribute: %s' % attr)

    def __getattr__(self,attr):
        if attr == 'standard_tag_order':
            c = self.cursor
            c.execute('SELECT tag FROM labels ORDER BY position')
            results = [r[0] for r in c.fetchall()]
            del c
            return results
        return getattr(self.__instance,attr)

    def install_default_tag_labels(self,overwrite=False):
        """
        Install default tags and descriptions to database.
        If overwrite is specified, existing entries are replaced.
        """
        for tag,details in STANDARD_TAG_MAP.items():
            try:
                position = STANDARD_TAG_ORDER.index(tag)
            except IndexError:
                raise TagError(
                    'Standard tag %s not in STANDARD_TAG_ORDER' % tag
                )
            try:
                label = details['label']
                description = details['description']
            except KeyError:
                raise TagError(
                    'Invalid tag default specification for %s' % tag
                )
            try:
                self.get_taglabel(tag)
            except TagError:
                self.define_taglabel(
                    tag,position,label,description,replace=overwrite
                )

    def get_taglabel(self,tag):
        """
        Get a tag label and description from database
        Returns label,description
        """
        c = self.cursor
        try:
            c.execute(
                'SELECT label,description FROM labels WHERE tag=?',
                (tag,)
            )
            result = c.fetchone()
            if result is None:
                raise TagError('No label for tag: %s' % tag )
            return result[0],result[1]
        except sqlite3.DatabaseError,emsg:
            raise TagError('Error querying database: %s' % emsg)

    def define_taglabel(self,tag,position,label,description=None,replace=False):
        """
        Define a tag label for user interfaces
        """
        c = self.cursor
        if replace:
            c.execute('DELETE FROM labels WHERE tag=?',(tag,))
            self.commit()
        try:
            c.execute(
                'INSERT INTO labels (tag,position,label,description) VALUES (?,?,?,?)',
                (tag,position,label,description,)
            )
            self.commit()
        except sqlite3.IntegrityError:
            raise TagError(
                'Tag label already defined: %s %s' % (tag,label)
            )
        del c


    def get_file(self,path,source='filesystem'):
        """
        Return FileTags object for given file from database. Object is created
        to database if it didn't exist.
        """
        return FileTags(self,path,source)

    def remove_file(self,path,source='filesystem'):
        """
        Remove file and associated tags from database
        """
        path = normalized(os.path.realpath(path))
        c = self.cursor
        c.execute(
            'DELETE FROM file WHERE source=? AND path=?',
            (source,path,)
        )
        self.commit()
        del c

    def source_file_mtimes(self,source='filesystem'):
        """
        Return mtime of all paths for a source in batch result
        """
        c = self.cursor
        c.execute('SELECT path,mtime FROM file WHERE source=? ORDER BY path',(source,))
        results = dict([(r[0],r[1]) for r in c.fetchall()])
        del c
        return results

    def source_files(self,source='filesystem'):
        """
        Return filetags entries for given source
        """
        c = self.cursor
        c.execute('SELECT * FROM file WHERE source=? ORDER BY path',(source,))
        results = [FileTags(self,**self.__result2dict__(c,r)) for r in c.fetchall()]
        del c
        return results

class base64_tag(object):
    """
    Wrapper to store a base64 formatted value to tags cleanly.
    """
    def __init__(self,value):
        if not isinstance(value,basestring):
            raise TagError('base64_tag value must be a string')
        try:
            base64.b64decode(value)
        except TypeError:
            raise TagError('base64_tag value is not valid base64 string')
        self.value = unicode(value)

    def __repr__(self):
        return self.value

    def __unicode__(self):
        return self.value

class FileTags(dict):
    """
    Tags database file entry, used to access tags for particular file.
    It is recommended to use this via soundforest.tags.formats.Tags class
    attribute db_tags, not directly.

    If id is given, id and mtime values are expected to be valid database
    references (from TagsDB.source_files() etc.)

    File modification time is initialized to 0: it is updated when the tags
    are read for the file.
    """
    def __init__(self,db,path,source='filesystem',id=None,mtime=0):
        dict.__init__(self)
        self.db = db
        self.log = logging.getLogger('modules')
        self.source = source
        self.path = normalized(os.path.realpath(path))

        if not os.path.isfile(self.path):
            raise TagError('No such file: %s' % self.path)

        try:
            mtime = int(mtime)
        except ValueError:
            raise TagError('Invalid FileTags mtime value: %s' % mtime)

        if id is not None:
            # From database results, trust the values!
            self.id = id
            self.mtime = mtime
            return

        c = self.db.cursor
        c.execute(
            'SELECT id,mtime FROM file WHERE source=? AND path=?',
            (self.source,self.path,)
        )
        result = c.fetchone()

        if result is None:
            c.execute(
                'INSERT INTO file (source,path,mtime) VALUES (?,?,?)',
                (self.source,self.path,mtime,)
            )
            self.db.commit()
            c.execute(
                'SELECT id,mtime FROM file WHERE source=? AND path=?',
                (self.source,self.path,)
            )
            result = c.fetchone()

        self.id = result[0]
        self.mtime = result[1]

    def __getitem__(self,item):
        """
        Return tag items from database
        """
        if not len(dict.keys(self)):
            self.load_tags()
        return dict.__getitem__(self,item)

    def keys(self):
        """
        Return tag names sorted by TagsDB.standard_tag_order list
        """
        if not len(dict.keys(self)):
            self.load_tags()
        ordered_keys = self.db.standard_tag_order
        keys = dict.keys(self)
        values = []
        for k in ordered_keys:
            if k in keys: values.append(k)
        for k in sorted(keys):
            if k not in ordered_keys: values.append(k)
        return values

    def items(self):
        """
        Return tag,value pairs keyed with self.dict()
        """
        return [(k,self[k]) for k  in self.keys()]

    def values(self):
        """
        Return values keyed with self.dict()
        """
        return [self[k] for k  in self.keys()]

    def load_tags(self,merge_string=None):
        """
        Load tags for this file from database
        """
        tags = {}
        c = self.db.cursor
        c.execute('SELECT tag,value,base64 FROM tag WHERE file=?',(self.id,))
        for entry in [self.db.__result2dict__(c,r) for r in c.fetchall()]:
            tag = entry['tag']
            value = entry['value']
            base64 = entry['base64'] and True or False
            if base64:
                value = base64_tag(value)
            if tags.has_key(tag):
                if merge_string is not None:
                    if not base64:
                        tags[tag] += '%s%s' % (merge_string,value)
                    else:
                        self.log.debug("Found multiple base64 tags, using first")
                else:
                    tags[tag] = [tags[tag]]
                    tags[tag].append(value)
            else:
                tags[tag] = value
        del c
        self.clear()
        dict.update(self,tags)

    def update_tags(self,tags,mtime=None,replace=True):
        """
        Update tags for this file from dictionary tags. Tags are
        treated as strings.

        If mtime is not given, current wall clock time is used.
        If replace is True, duplicate tags are removed: otherwise,
        new tag with given value is added to database.
        """
        if type(tags) is not dict:
            raise TagError('Tags parameter must be a dictionary')
        if not len(tags.keys()):
            raise TagError('No tags to update')

        if mtime is None:
            mtime = int(time.mktime(time.localtime()))
        try:
            mtime = int(mtime)
        except ValueError:
            raise TagError('Invalid mtime integer value: %s' % mtime)

        c = self.db.cursor

        # Preprocess tag data
        entries = []
        for tag,value in tags.items():
            if type(value) is list:
                # Multiple values to set
                for v in value:
                    if type(value) is base64_tag:
                        entries.append({'tag':tag,'value':unicode(v),'base64':True})
                    else:
                        entries.append({'tag':tag,'value':v,'base64':False})
            else:
                # Single value
                if type(value) is base64_tag:
                    entries.append({'tag':tag,'value':unicode(value),'base64':True})
                else:
                    entries.append({'tag':tag,'value':value,'base64':False})

            if replace:
                try:
                    c.execute(
                        'DELETE FROM tag WHERE file=? AND tag=?',
                        (self.id,tag)
                    )
                except sqlite3.OperationalError,emsg:
                    raise TagError('Error removing tags for %s:\n%s' % (self.path,emsg))

        # Insert actual tag data
        for entry in entries:
            tag = entry['tag']
            value = entry['value']
            base64 = entry['base64']
            if isinstance(value,int):
                value = unicode('%d' % value,'utf-8')
            if isinstance(value,float) or isinstance(value,decimal.Decimal):
                value = unicode('%0.4f' % value,'utf-8')
            if not isinstance(value,unicode):
                try:
                    value = unicode(value,'utf-8')
                except UnicodeEncodeError:
                    raise TagError('Error encoding tag value to unicode')
                except UnicodeDecodeError:
                    raise TagError('Error encoding tag value to unicode')


            try:
                c.execute(
                    'INSERT INTO tag (file,tag,value,base64) VALUES (?,?,?,?)',
                    (self.id,tag,value,base64)
                )
            except sqlite3.IntegrityError:
                # Tag was already defined
                pass
            except sqlite3.OperationalError,emsg:
                raise TagError(
                    'Error adding tag %s value %s to database:\n%s' %
                    (tag,value,emsg)
                )
        c.execute(
            'UPDATE file SET mtime=? WHERE source=? and path=?',
            (mtime,self.source,self.path,)
        )
        self.db.commit()
        del c

