"""
Module to detect and store known audio file library paths
"""

import os,logging,sqlite3,time,hashlib

from systematic.shell import normalized
from systematic.filesystems import MountPoints
from systematic.sqlite import SQLiteDatabase,SQLiteError

from soundforest.codecs import CodecDB,CodecError
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
    source      TEXT,
    path        TEXT,
    FOREIGN KEY(treetype) REFERENCES treetypes(id) ON DELETE CASCADE
);
""",
"""
CREATE UNIQUE INDEX IF NOT EXISTS source_files ON tree (source,path);
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
    directory   TEXT,
    filename    TEXT,
    mtime       INTEGER,
    shasum      TEXT,
    deleted     BOOLEAN DEFAULT FALSE,
    FOREIGN KEY(tree) REFERENCES tree(id) ON DELETE CASCADE
);
""",
"""CREATE UNIQUE INDEX IF NOT EXISTS file_paths ON files (tree,directory,filename)""",
"""CREATE UNIQUE INDEX IF NOT EXISTS file_mtimes ON files (tree,directory,filename,mtime)""",
"""
CREATE TABLE IF NOT EXISTS filechanges (
    id          INTEGER PRIMARY KEY,
    file        INTEGER,
    mtime       INTEGER,
    event       INTEGER,
    FOREIGN KEY(file) REFERENCES files(id) ON DELETE CASCADE
);
""",
"""
CREATE TABLE IF NOT EXISTS removablemedia (
    id          INTEGER PRIMARY KEY,
    name        TEXT UNIQUE,
    updated     INTEGER
);
""",
"""
CREATE TABLE IF NOT EXISTS removablefiles (
    id          INTEGER PRIMARY KEY,
    source      INTEGER,
    path        TEXT UNIQUE,
    mtime       INTEGER,
    FOREIGN KEY(source) REFERENCES files(id) ON DELETE CASCADE
);
""",
"""CREATE UNIQUE INDEX IF NOT EXISTS removabblefilesources ON removablefiles (source,path)""",
]

# Event types in filechanges table
FILE_ADDED = 1
FILE_DELETED = 2
FILE_MODIFIED = 3

VALID_FILE_EVENTS = [FILE_ADDED,FILE_DELETED,FILE_MODIFIED]

class AudioTreeError(Exception):
    """
    Exceptions raised by audio tree processing
    """
    def __str__(self):
        return self.args[0]

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
        for ttype,description in DEFAULT_TREE_TYPES.items():
            self.register_tree_type(ttype,description,ignore_duplicate=True)

    class AudioTreeDBInstance(SQLiteDatabase):
        """
        Singleton instance of one AudioTree sqlite database.
        """
        def __init__(self,db_path=DB_PATH,codec_db=None,metadata_lookup=None):
            try:
                SQLiteDatabase.__init__(self,db_path,tables_sql=DB_TABLES)
            except SQLiteError,emsg:
                raise AudioTreeError(
                    'Error initializing database %s: %s' % (db_path,emsg)
                )

            if codec_db is None:
                try:
                    codec_db = CodecDB()
                except CodecError,emsg:
                    raise AudioTreeError(
                       'Error initializing codec database: %s' % emsg
                    )
            if not isinstance(codec_db,CodecDB):
                raise AudioTreeError('Not a codec database: %s' % type(codec_db))
            self.codec_db = codec_db

            if metadata_lookup is None:
                metadata_lookup = MetaData()
            self.metadata_lookup = metadata_lookup

        def __getattr__(self,attr):
            try:
                return SQLiteDatabase.__getattr__(self,attr)
            except AttributeError:
                raise AttributeError('No such AudioTreeDB attribute: %s' % attr)

    def __getattr__(self,attr):
        if attr == 'tree_types':
            c = self.cursor
            c.execute(
                'SELECT type,description FROM treetypes ORDER BY type'
            )
            results = [(r[0],r[1]) for r in c.fetchall()]
            del c
            return results
        if attr == 'trees':
            c = self.cursor
            c.execute('SELECT path,source FROM tree')
            results = [self.get_tree(r[0],r[1]) for r in c.fetchall()]
            del c
            return results
        return getattr(self.__instance,attr)

    def __setattr__(self,attr,value):
        return setattr(self.__instance,attr,value)

    def get_tree(self,path,source='filesystem'):
        """
        Retrieve AudioTree matching give path from database.

        Raises ValueError if path was not found.
        """
        path = normalized(os.path.realpath(path))
        c = self.cursor
        c.execute('SELECT t.id,tt.type,t.path,t.source ' +
            'FROM treetypes AS tt, tree AS t ' +
            'WHERE tt.id=t.treetype AND t.source=? AND t.path=?',
            (source,path,)
        )
        result = c.fetchone()
        if result is None:
            raise ValueError('Path not in database: %s' % path)
        tree = AudioTree(*result[1:],audio_db=self)
        c.execute('SELECT alias FROM aliases WHERE tree=?',(result[0],))
        for r in c.fetchall():
            tree.aliases.append(r[0])
        return tree

    def unregister_tree_type(self,ttype):
        """
        Unregister audio tree type from database
        """
        try:
            c = self.cursor
            c.execute(
                'DELETE FROM treetypes WHERE type=?',
                (ttype,)
            )
            self.commit()
            del c
        except sqlite3.IntegrityError,emsg:
            raise ValueError('Error unregistering tree type %s: %s' % (ttype,emsg))

    def register_tree_type(self,ttype,description,ignore_duplicate=False):
        """
        Register audio tree type to database
        ttype       Name of tree type ('Music','Samples' etc.)
        description Description of the usage of the files
        """
        try:
            c = self.cursor
            c.execute(
                'INSERT INTO treetypes (type,description) VALUES (?,?)',
                (ttype,description)
            )
            self.commit()
            del c
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
            c = self.cursor
            c.execute(
                'DELETE FROM tree where source=? AND path=?',
                (tree.source,tree.path,)
            )
            self.commit()
            del c
        except sqlite3.IntegrityError,emsg:
            raise ValueError(emsg)

    def register(self,tree,ignore_duplicate=True):
        """
        Register a audio file tree to database
        """

        if not isinstance(tree,AudioTree):
            raise TypeError('Tree must be AudioTree object')

        c = self.cursor

        c.execute('SELECT id FROM treetypes WHERE type=?',(tree.tree_type,))
        result = c.fetchone()
        if result is None:
            raise ValueError('Tree type not found: %s' % tree.tree_type)
        tt_id = result[0]

        try:
            c.execute(
                'INSERT INTO tree (treetype,source,path) VALUES (?,?,?)',
                (tt_id,tree.source,tree.path,)
            )
            self.commit()
        except sqlite3.IntegrityError:
            if not ignore_duplicate:
                raise ValueError('Already registered: %s %s' % (tree.source,tree.path))

        c.execute('SELECT id FROM tree where source=? AND path=?',(tree.source,tree.path,))
        tree_id = c.fetchone()[0]

        for alias in tree.aliases:
            try:
                c.execute(
                    'INSERT INTO aliases (tree,alias) VALUES (?,?)',
                    (tree_id,alias)
                )
            except sqlite3.IntegrityError:
                # Alias already in database
                pass
            self.commit()

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

    def register_removable(self,removable_media,ignore_duplicate=False):
        """
        Register removable media device by name
        """
        if not isinstance(removable_media,RemovableMedia):
            raise TypeError('Removable media must be RemovableMedia instance')
        c = self.cursor
        c.execute('')
        try:
            c.execute(
                'INSERT INTO removablemedia (name,updated) VALUES (?,?)',
                (removable_media.name,0)
            )
            self.commit()
        except sqlite3.IntegrityError:
            if not ignore_duplicate:
                raise ValueError('Already registered: %s %s' % removable_media.name)
        del c

class AudioTree(object):
    """
    Representation of one audio tree object in database

    Attributes:
    adb         Instance of AudioTreeDB()
    path        Normalized unicode real path to tree root
    aliases     Symlink paths for this tree
    tree_type   Tree type (reference to treetypes table)
    source      Source reference, by default 'filesystem'

    """
    def __init__(self,tree_type,path,source='filesystem',audio_db=None):
        self.__cached_attrs = {}
        self.__next = None
        self.__iterfiles = None

        if audio_db is None:
            audio_db = AudioTreeDB()
        if not isinstance(audio_db,AudioTreeDB):
            raise AudioTreeError('Not a audio tree database: %s' % type(audio_db))
        self.adb = audio_db

        self.log = logging.getLogger('modules')
        self.aliases = []
        self.tree_type = tree_type
        self.source = source

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
            c.execute('SELECT directory,filename FROM files WHERE tree=? ORDER BY directory,filename',(self.id,))
            files = [os.path.join(r[0],r[1]) for r in c.fetchall()]
            del c
            return files

        if attr == 'directories':
            # Return relative path names for unique directories (albums) which contain files
            c = self.adb.cursor
            c.execute('SELECT distinct directory FROM files WHERE tree=? ORDER BY directory',(self.id,))
            directories = [r[0] for r in c.fetchall()]
            del c
            return directories

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

    def directory_files(self,directory):
        """
        Return files from database matching tree and directory
        """
        c = self.adb.cursor
        c.execute(
            'SELECT filename FROM files WHERE tree=? AND directory=?',
            (self.id,directory,)
        )
        return [r[0] for r in c.fetchall()]

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
                    'SELECT f.directory,f.filename,fc.mtime,fc.event FROM filechanges AS fc, files AS f ' +\
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
                    'SELECT f.directory,f.filename,fc.mtime,fc.event FROM filechanges AS fc, files AS f ' +\
                    'WHERE fc.mtime>=? AND file in (SELECT f.id FROM files AS f, tree AS t WHERE f.tree=t.id AND t.id=?) ' +\
                    'ORDER BY fc.mtime',
                    (time_start,self.id,)
                )
            results = []
            for r in [self.adb.__result2dict__(c,r) for r in c.fetchall()]:
                r['path'] = os.path.join(r['directory'],r['filename'])
                del r['directory']
                del r['filename']
                results.append(r)
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

        directory = os.path.dirname(path)
        filename = os.path.basename(path)
        try:
            c = self.adb.cursor
            c.execute(
                'SELECT id FROM files WHERE tree=? AND directory=? AND filename=?',
                (self.id,directory,filename)
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
        c.execute('SELECT directory,filename,mtime,deleted FROM files WHERE tree=?',(self.id,))
        tree_songs = dict([(os.path.join(r[0],r[1]),{'mtime': r[2],'deleted': r[3]}) for r in c.fetchall()])

        for (root,dirs,files) in os.walk(self.path,):

            # Ignore files inside .itlp directories
            is_itlp = filter(lambda x:
                os.path.splitext(x)[1]=='.itlp',
                root.split(os.sep)
            )
            if is_itlp:
                continue

            for f in files:
                # Some special filenames cause errors
                if f in IGNORED_FILES:
                    continue

                is_modified = False
                f = normalized(os.path.realpath(os.path.join(root,f)))
                db_path = f[len(self.path):].lstrip(os.sep)
                db_directory = os.path.dirname(db_path)
                db_filename = os.path.basename(db_path)
                mtime = long(os.stat(f).st_mtime)

                try:
                    db_mtime = tree_songs[db_path]['mtime']
                    if db_mtime != mtime:
                        try:
                            c.execute(
                                'UPDATE files SET mtime=? WHERE directory=? AND filename=?',
                                (mtime,db_directory,db_filename,)
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
                                'UPDATE files SET deleted=0 WHERE directory=? AND filename=?',
                                (db_directory,db_filename,)
                            )
                            is_modified = True
                        except sqlite3.DatabaseError,emsg:
                            raise ValueError('Error updating deleted flag: %s' % emsg)
                        if update_checksums:
                            TreeFile(self,db_path).update_checksum(force_update=True)

                except KeyError:
                    try:
                        c.execute(
                            'INSERT INTO files (tree,mtime,directory,filename) VALUES (?,?,?,?)',
                            (self.id,mtime,db_directory,db_filename)
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
                db_directory = os.path.dirname(db_path)
                db_filename = os.path.basename(db_path)
                try:
                    c.execute(
                        'UPDATE files SET deleted=1 WHERE tree=? AND directory=? AND filename=?',
                        (self.id,db_directory,db_filename,)
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
        self.log = logging.getLogger('modules')
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
        self.directory = os.path.dirname(self.path)
        self.filename = os.path.basename(self.path)

    def __getattr__(self,attr):
        if attr in ['mtime','shasum','deleted']:
            if not self.__cached_attrs:
                self.__update_cached_attrs()
            try:
                return self.__cached_attrs[attr]
            except KeyError:
                raise AttributeError('No database details for %s' % self.path)
        if attr == 'path_noext':
            return os.path.splitext(self.path)[0]
        if attr == 'realpath_noext':
            return os.path.splitext(self.realpath)[0]
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
            'SELECT * FROM files WHERE tree=? AND directory=? AND filename=?',
            (self.tree.id,self.directory,self.filename,)
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
            'UPDATE files set shasum=? WHERE tree=? AND directory=? AND filename=?',
            (shasum.hexdigest(),self.tree.id,self.directory,self.filename,)
        )
        self.adb.commit()
        del c
        return shasum.hexdigest()

class RemovableMedia(object):
    """
    Removable media device accessed by mountpoint name
    """
    def __init__(self,adb,name):
        self.adb = adb
        self.name = name

    def __getattr__(self,attr):
        if attr == 'id':
            c = self.adb.cursor
            c.execute('SELECT id FROM removablemedia WHERE name=?',(self.name,))
            result = c.fetchone()
            del c
            if result is None:
                raise AudioTreeError('Removable media not registered: %s' % self.name)
            return result[0]
        if attr == 'is_available':
            mp = MountPoints()
            matches = filter(lambda x: x.name==self.name, mp)
            if len(matches)>1:
                raise AudioTreeError('Multiple removable devices with name %s' % self.name)
            elif matches:
                return True
            return False
        raise AttributeError('No such RemovableMedia attribute: %s' % attr)

if __name__ == '__main__':
    adb = AudioTreeDB()
    r = RemovableMedia(adb,'ELENTRA')
    print r.is_available