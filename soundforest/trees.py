"""
Module to detect and store known audio file library paths
"""

import os,sqlite3,time,hashlib

from systematic.shell import normalized 
from systematic.sqlite import SQLiteDatabase

from soundforest.codecs import CodecDB
from soundforest.metadata import MetaData

DB_PATH = os.path.join(os.getenv('HOME'),'.soundforest','trees.sqlite')

# Ignored special filenames causing problems
IGNORED_FILES = [ 'Icon\r']

DEFAULT_TREE_TYPES = {
    'Songs':        'Complete song files',
    'Recordings':   'Live performance recordings',
    'Playlists':    'Playlist files',
    'Loops':        'Audio loops',
    'Samples':      'Audio samples',
}

DB_TABLES = [
"""
CREATE TABLE IF NOT EXISTS treetypes (
    id          INTEGER PRIMARY KEY,
    type        TEXT UNIQUE,
    description TEXT 
);
""",
"""
CREATE TABLE IF NOT EXISTS tree (
    id          INTEGER PRIMARY KEY,
    treetype    INTEGER,
    path        TEXT UNIQUE,
    FOREIGN KEY(treetype) REFERENCES treetypes(id) ON DELETE CASCADE
);
""",
"""
CREATE TABLE IF NOT EXISTS aliases (
    id      INTEGER PRIMARY KEY,
    tree    INTEGER,
    alias   TEXT UNIQUE,
    FOREIGN KEY(tree) REFERENCES tree(id) ON DELETE CASCADE
);
""",
"""
CREATE UNIQUE INDEX IF NOT EXISTS tree_aliases ON aliases (tree,alias)
""",
"""
CREATE TABLE IF NOT EXISTS files (
    id          INTEGER PRIMARY KEY,
    tree        INTEGER,
    path        TEXT UNIQUE,
    mtime       INTEGER,
    shasum      TEXT,
    deleted     BOOLEAN DEFAULT FALSE,
    FOREIGN KEY(tree) REFERENCES tree(id) ON DELETE CASCADE
);
""",
"""CREATE UNIQUE INDEX IF NOT EXISTS file_paths ON files (tree,path)""",
"""CREATE UNIQUE INDEX IF NOT EXISTS file_mtimes ON files (path,mtime)""",
"""
CREATE TABLE IF NOT EXISTS filechanges (
    id          INTEGER PRIMARY KEY,
    file        INTEGER,
    mtime       INTEGER,
    event       INTEGER,
    FOREIGN KEY(file) REFERENCES files(id) ON DELETE CASCADE
);
""",
]

# Event types in filechanges table
FILE_ADDED = 1
FILE_DELETED = 2
FILE_MODIFIED = 3

VALID_FILE_EVENTS = [FILE_ADDED,FILE_DELETED,FILE_MODIFIED]

class AudioTreeDB(object):
    """
    Database of audio tree paths and their aliases, including the tree
    type. Valid standard tree types are:
    Songs       Music songs
    Recordings  Music performance recordings
    Samples     Audio sample files
    Loops       Audio loop files
    Playlists   Playlist files
    """
    __instance = None

    def __init__(self,db_path=DB_PATH,metadata_lookup=None):
        if AudioTreeDB.__instance is None:
            AudioTreeDB.__instance = AudioTreeDB.AudioTreeDBInstance(db_path,metadata_lookup)
        self.__dict__['_AudioTreeDB.__instance'] = AudioTreeDB.__instance

    def __getattr__(self,attr):
        return getattr(self.__instance,attr)

    def __setattr__(self,attr,value):
        return setattr(self.__instance,attr,value)

    class AudioTreeDBInstance(SQLiteDatabase):
        """
        Singleton instance of one AudioTree sqlite database.
        """
        def __init__(self,db_path=DB_PATH,metadata_lookup=None):
            SQLiteDatabase.__init__(self,db_path,tables_sql=DB_TABLES)
            for ttype,description in DEFAULT_TREE_TYPES.items():
                self.register_tree_type(ttype,description,ignore_duplicate=True)

            self.metadata_lookup = metadata_lookup is not None and metadata_lookup or MetaData()
            self.codec_db = CodecDB()

        def __getattr__(self,attr):
            if attr == 'tree_types':
                results = self.conn.execute(
                    'SELECT type,description from treetypes ORDER BY type'
                )
                return [(r[0],r[1]) for r in results]
            if attr == 'trees':
                return [self.get_tree(r[0]) for r in \
                    self.conn.execute('SELECT path from tree')
                ]
            try:
                return SQLiteDatabase.__getattr__(self,attr)
            except AttributeError:
                raise AttributeError('No such AudioTreeDB attribute: %s' % attr)

        def get_tree(self,path):
            """
            Retrieve AudioTree matching give path from database.

            Raises ValueError if path was not found.
            """
            path = normalized(os.path.realpath(path))
            c = self.cursor
            c.execute('SELECT t.id,tt.type,t.path ' +
                'FROM treetypes AS tt, tree AS t ' +
                'WHERE tt.id=t.treetype AND t.path=?',
                (path,)
            )
            result = c.fetchone()
            if result is None:
                raise ValueError('Path not in database: %s' % path)
            tree = AudioTree(*result[1:])
            c.execute('SELECT alias FROM aliases where tree=?',(result[0],))
            for r in c.fetchall():
                tree.aliases.append(r[0])
            return tree

        def unregister_tree_type(self,ttype):
            """
            Unregister audio tree type from database
            """
            try:
                with self.conn:
                    self.conn.execute(
                        'DELETE FROM treetypes WHERE type=?',(ttype,)
                    )
            except sqlite3.IntegrityError,emsg:
                raise ValueError('Error unregistering tree type %s: %s' % (ttype,emsg))

        def register_tree_type(self,ttype,description,ignore_duplicate=False):
            """
            Register audio tree type to database
            ttype       Name of tree type ('Music','Samples' etc.)
            description Description of the usage of the files
            """
            try:
                with self.conn:
                    self.conn.execute(
                        'INSERT INTO treetypes (type,description) VALUES (?,?)',
                        (ttype,description)
                    )
            except sqlite3.IntegrityError:
                if ignore_duplicate:
                    return
                raise ValueError('Tree type %s already registered' % ttype)

        def unregister(self,tree):
            """
            Remove audio file tree from database. Will remove fill references to the tree.
            """
            if type(tree) != AudioTree:
                raise TypeError('Tree must be AudioTree object')
            try:
                with self.conn:
                    self.conn.execute(
                        'DELETE FROM tree where path=?',(tree.path,)
                    )
            except sqlite3.IntegrityError,emsg:
                raise ValueError(emsg)

        def register(self,tree,ignore_duplicate=True):
            """
            Register a audio file tree to database
            """

            if type(tree) != AudioTree:
                raise TypeError('Tree must be AudioTree object')

            c = self.cursor
            c.execute('SELECT id FROM treetypes WHERE type=?',(tree.tree_type,))
            result = c.fetchone()
            if result is None:
                raise ValueError('Tree type not found: %s' % tree.tree_type)
            tt_id = result[0]

            try:
                c.execute(
                    'INSERT INTO tree (treetype,path) VALUES (?,?)',
                    (tt_id,tree.path,)
                )
                self.commit()
            except sqlite3.IntegrityError:
                if not ignore_duplicate:
                    raise ValueError('Already registered: %s' % tree.path)

            c.execute('SELECT id FROM tree where path=?',(tree.path,))
            tree_id = c.fetchone()[0]

            for alias in tree.aliases:
                try:
                    with self.conn:
                        self.conn.execute(
                            'INSERT INTO aliases (tree,alias) VALUES (?,?)',
                            (tree_id,alias)
                        )
                except sqlite3.IntegrityError:
                    # Alias already in database
                    pass

        def filter_type(self,tree_type):
            """
            Filter audio trees from database based on the tree type
            """
            c = self.cursor
            c.execute(
                'SELECT t.path FROM treetypes as tt, tree as t ' +
                'WHERE tt.id=t.treetype and tt.type=?',
                (tree_type,)
            )
            return [self.get_tree(r[0]) for r in c.fetchall()]

class AudioTree(object):
    """
    Representation of one audio tree object in database

    Attributes:
    adb         Instance of AudioTreeDB()
    path        Normalized unicode real path to tree root
    aliases     Symlink paths for this tree
    tree_type   Tree type (reference to treetypes table)

    """
    def __init__(self,tree_type,path):
        self.__cached_attrs = {}
        self.__next = None
        self.__iterfiles = None

        self.adb = AudioTreeDB()
        self.aliases = []
        self.tree_type = tree_type

        self.path = normalized(os.path.realpath(path))
        path = normalized(path)
        if path != self.path and path not in self.aliases:
            self.aliases.append(path)

    def __getattr__(self,attr):
        """
        Optional attributes:

        id              Tree database ID
        is_available    Boolean indicating if this tree is readable (mounted)
        files           Returns list of relative paths for tree files
        """
        if attr == 'is_available':
            return os.access(self.path,os.X_OK)
        if attr in ['id']:
            if not self.__cached_attrs:
                self.__update_cached_attrs()
            return self.__cached_attrs[attr]

        if attr == 'files':
            # Return relative path names for tree from database
            c = self.adb.cursor
            c.execute('SELECT path FROM files where tree=? ORDER BY path',(self.id,))
            files = [r[0] for r in c.fetchall()]
            del c
            return files
        raise AttributeError('No such Tree attribute: %s' % attr)

    def __update_cached_attrs(self):
        """
        Update cached attributes from database.

        Attributes are retrieved with __getattr__
        """
        c = self.adb.cursor
        c.execute('SELECT * FROM tree WHERE path=?',(self.path,))
        self.__cached_attrs.update(self.adb.__result2dict__(c,c.fetchone()))

    def __getitem__(self,item):
        try:
            return TreeFile(self,item)
        except ValueError:
            pass
        raise KeyError('No such track in library: %s' % item)

    def __repr__(self):
        return '%s %s' % (self.tree_type,self.path)

    def __iter__(self):
        return self

    def next(self):
        """
        Return TreeFile items for paths in tree
        """
        if self.__next is None:
            self.__next = 0
            self.__iterfiles = self.files
        try:
            while True:
                entry = TreeFile(self,self.__iterfiles[self.__next])
                self.__next += 1
                if not entry.deleted:
                    return entry
        except IndexError:
            self.__next = None
            self.__iterfiles = None
            raise StopIteration

    def file_events(self,time_start=None):
        """
        Return list of change events for a file path in given library.

        If time_start is given, it is used as EPOC timestamp to filter out
        events older than given value
        """
        c = self.adb.cursor
        try:
            if time_start is  None:
                c.execute(
                    'SELECT f.path,fc.mtime,fc.event FROM filechanges AS fc, files AS f ' +\
                    'WHERE file in (SELECT f.id FROM files AS f, tree AS t WHERE f.tree=t.id AND t.id=?) ' +\
                    'ORDER BY fc.mtime',
                    (self.id,)
                )
            else:
                try:
                    time_start = int(time_start)
                    if time_start<0:
                        raise ValueError
                except ValueError:
                    raise ValueError('Invalid time_start timestamp: %s' % time_start)
                c.execute(
                    'SELECT f.path,fc.mtime,fc.event FROM filechanges AS fc, files AS f ' +\
                    'WHERE fc.mtime>=? AND file in (SELECT f.id FROM files AS f, tree AS t WHERE f.tree=t.id AND t.id=?) ' +\
                    'ORDER BY fc.mtime',
                    (time_start,self.id,)
                )
            results = [self.adb.__result2dict__(c,r) for r in c.fetchall()]
        except sqlite3.DatabaseError,emsg:
            raise ValueError('Error querying file events: %s' % emsg)
        del c
        return results

    def file_event(self,path,event):
        """
        Create a new filechanges event for given file ID
        Event must be from one of FILE_ADDED, FILE_DELETED, FILE_MODIFIED
        """
        try:
            event = int(event)
            if event not in VALID_FILE_EVENTS:
                raise ValueError
        except ValueError:
            raise ValueError('Invalid file event: %s' % event)
        mtime = time.mktime(time.localtime())

        try:
            c = self.adb.cursor
            c.execute(
                'SELECT id FROM files WHERE tree=? AND path=?',
                (self.id,path,)
            )
            file_id = c.fetchone()[0]
            c.execute(
                'INSERT INTO filechanges (file,mtime,event) VALUES (?,?,?)',
                (file_id,mtime,event)
            )
            self.adb.commit()
        except AttributeError:
            raise ValueError('File not in library: %s' % path)
        except sqlite3.DatabaseError,emsg:
            raise ValueError('Error creating file event: %s' % emsg)

    def update_checksums(self,force_update=False):
        """
        Update SHA1 checksum for files in tree with TreeFile.update_checksum()
        If force_update is True, all file checksums are updated.
        """
        for entry in self:
            entry.update_checksum(force_update)

    def cleanup(self):
        """
        Remove database entries for deleted files (with deleted flag)
        """
        c = self.adb.cursor
        c.execute('DELETE FROM files WHERE tree=? and deleted=1',(self.path,))
        self.adb.commit()
        del c

    def update(self,update_checksums=False):
        """
        Update files for this tree in DB, adding filechanges events.
        Returns a dictionary containing paths to added, deleted and modified
        files in library.
        """
        if not self.is_available:
            self.adb.log.debug('Tree not available, skipping update: %s' % self.path)
            return

        changes = {'added':[],'deleted':[],'modified':[]}

        c = self.adb.cursor
        c.execute('SELECT path,mtime,deleted FROM files WHERE tree=?',(self.id,))
        tree_songs = dict([(r[0],{'mtime': r[1],'deleted': r[2]}) for r in c.fetchall()])

        for (root,dirs,files) in os.walk(self.path):
            for f in files:
                # Some special filenames cause errors
                if f in IGNORED_FILES:
                    continue

                is_modified = False
                f = normalized(os.path.realpath(os.path.join(root,f)))
                db_path = f[len(self.path):].lstrip(os.sep)
                mtime = long(os.stat(f).st_mtime)

                try:
                    db_mtime = tree_songs[db_path]['mtime']
                    if db_mtime != mtime:
                        try:
                            c.execute(
                                'UPDATE files SET mtime=? WHERE path=?',
                                (mtime,db_path,)
                            )
                            is_modified = True
                        except sqlite3.IntegrityError,emsg:
                            self.log.debug('Error updating mtime: %s' % emsg)
                        self.file_event(db_path,FILE_MODIFIED)
                        if update_checksums:
                            TreeFile(self,db_path).update_checksum(force_update=True)
                        else:
                            self.adb.log.debug('Modified: %s' % db_path)

                    is_deleted = tree_songs[db_path]['deleted']
                    if is_deleted:
                        try:
                            c.execute(
                                'UPDATE files SET deleted=0 WHERE path=?',
                                (db_path,)
                            )
                            is_modified = True
                        except sqlite3.DatabaseError,emsg:
                            raise ValueError('Error updating deleted flag: %s' % emsg)
                        if update_checksums:
                            TreeFile(self,db_path).update_checksum(force_update=True)

                except KeyError:
                    try:
                        c.execute(
                            'INSERT INTO files (tree,mtime,path) VALUES (?,?,?)',
                            (self.id,mtime,db_path,)
                        )
                        is_modified = True
                    except sqlite3.IntegrityError,emsg:
                        raise ValueError('%s: %s' % (f,emsg))
                    self.file_event(db_path,FILE_ADDED)
                    tree_songs[f] = {'mtime': mtime, 'deleted': False}
                    changes['added'].append(db_path)
                    if update_checksums:
                        TreeFile(self,db_path).update_checksum(force_update=True)
                    else:
                        self.adb.log.debug('Added: %s' % db_path)

                if is_modified:
                    self.adb.commit()

        for db_path,flags in tree_songs.items():
            is_deleted = flags['deleted']
            if is_deleted:
                continue
            f = os.path.join(self.path,db_path)
            if not os.path.isfile(f):
                try:
                    c.execute(
                        'UPDATE files SET deleted=1 WHERE tree=? and path=?',
                        (self.id,db_path,)
                    )
                except sqlite3.IntegrityError:
                    raise ValueError('Error marking file deleted: %s' % db_path)
                self.file_event(db_path,FILE_DELETED)
                changes['deleted'].append(db_path)
        del c
        return changes

class TreeFile(object):
    """
    Class for one file in given sound forest tree.
    Note the file format is not processed yet, this file can be jpg, pdf
    or anything else when initialized.
    """
    def __init__(self,tree,path):
        self.adb = AudioTreeDB()
        self.__cached_attrs = {}
        self.tree = tree
        self.__filetype = None
        self.__fileformat = None

        if path.startswith(os.sep):
            self.path = normalized(os.path.realpath(path))
        else:
            self.path = normalized(path)

        if self.path[:len(self.tree.path)] == self.tree.path:
            self.path = self.path[len(self.tree.path):].lstrip(os.sep)
        self.realpath = os.path.join(self.tree.path,self.path)

    def __getattr__(self,attr):
        if attr in ['mtime','shasum','deleted']:
            if not self.__cached_attrs:
                self.__update_cached_attrs()
            try:
                return self.__cached_attrs[attr]
            except KeyError:
                raise AttributeError('No database details for %s' % self.path)
        if attr == 'filetype':
            if self.__filetype is None:
                self.__match_fileformat()
            return self.__filetype
        if attr == 'format':
            if self.__filetype is None:
                self.__match_fileformat()
            return self.__fileformat

        raise AttributeError('No such TreeFile attribute: %s' % attr)

    def __match_fileformat(self):
        """
        Match file format to registered metadata and codecs
        """
        m = self.adb.metadata_lookup.match(self.realpath)
        if m:
            self.__filetype = 'metadata'
            self.__fileformat = m
            return
        m = self.adb.codec_db.match(self.realpath)
        if m:
            self.__filetype = 'audio'
            self.__fileformat = m
            return
        else:
            self.__filetype = 'unknown'
            self.__fileformat = None

    def __update_cached_attrs(self):
        c = self.adb.cursor
        c.execute(
            'SELECT * FROM files WHERE tree=? AND path=?',
            (self.tree.id,self.path,)
        )
        result = c.fetchone()
        if result is None:
            # File many not be in database
            return
        self.__cached_attrs.update(self.adb.__result2dict__(c,result))
        del c

    def __repr__(self):
        return '%s%s' % (self.path,self.deleted and ' (DELETED)' or '')

    def update_checksum(self,force_update=False):
        """
        Update SHA1 checksum stored for file into database
        """
        if not os.path.isfile(self.realpath):
            self.adb.log.debug('Not updating sha1, file missing: %s' % self.realpath)
            return

        mtime = long(os.stat(self.realpath).st_mtime)
        if self.mtime==mtime and self.shasum is not None and not force_update:
            # Do not update existing mtime if file is not modified
            return

        self.adb.log.debug('Updating SHA1 for %s' % self.realpath.encode('utf-8'))
        shasum = hashlib.sha1()
        shasum.update(open(self.realpath,'r').read())
        c = self.adb.cursor
        c.execute(
            'UPDATE files set shasum=? WHERE tree=? and path=?',
            (shasum.hexdigest(),self.tree.id,self.path,)
        )
        self.adb.commit()
        del c
        return shasum.hexdigest()
