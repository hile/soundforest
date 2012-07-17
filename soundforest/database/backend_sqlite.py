"""
Soundforest sqlite database backend implementation
"""

import os,sqlite3,time

from systematic.sqlite import SQLiteDatabase,SQLiteError

from soundforest.database import SoundForestDBError

DB_PATH = os.path.join(os.getenv('HOME'),'.soundforest','default.sqlite')

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
    extension   TEXT,
    mtime       INTEGER,
    shasum      TEXT,
    deleted     BOOLEAN DEFAULT 0,
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
    name        TEXT,
    format      TEXT,
    updated     INTEGER
);
""",
"""CREATE UNIQUE INDEX IF NOT EXISTS removablemedias ON removablemedia (name,format)""",
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
"""
CREATE TABLE IF NOT EXISTS codec (
    id          INTEGER PRIMARY KEY,
    name        TEXT UNIQUE,
    description TEXT
);
""",
"""
CREATE TABLE IF NOT EXISTS extensions (
    id          INTEGER PRIMARY KEY,
    codec       INTEGER,
    extension   TEXT UNIQUE,
    FOREIGN KEY(codec) REFERENCES codec(id) ON DELETE CASCADE
);
""",
"""
CREATE TABLE IF NOT EXISTS decoder (
    id          INTEGER PRIMARY KEY,
    priority    INTEGER,
    codec       INTEGER,
    command     TEXT,
    FOREIGN KEY(codec) REFERENCES codec(id) ON DELETE CASCADE
);
""",
"""
CREATE TABLE IF NOT EXISTS encoder (
    id          INTEGER PRIMARY KEY,
    priority    INTEGER,
    codec       INTEGER,
    command     TEXT,
    FOREIGN KEY(codec) REFERENCES codec(id) ON DELETE CASCADE
);
""",
"""
CREATE TABLE IF NOT EXISTS playlistsources (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    path        TEXT
);
""",
"""
CREATE UNIQUE INDEX IF NOT EXISTS source_paths ON playlistsources (name,path);
""",
"""
CREATE TABLE IF NOT EXISTS playlist (
    id          INTEGER PRIMARY KEY,
    source      INTEGER,
    updated     INTEGER,
    folder      TEXT,
    name        TEXT,
    description TEXT,
    FOREIGN KEY(source) REFERENCES playlistsources(id) ON DELETE CASCADE
);
""",
"""
CREATE UNIQUE INDEX IF NOT EXISTS playlist_paths ON playlist (source,folder,name);
""",
"""
CREATE TABLE IF NOT EXISTS playlistfile (
    id          INTEGER PRIMARY KEY,
    playlist    INTEGER,
    position    INTEGER,
    path        TEXT,
    FOREIGN KEY(playlist) REFERENCES playlist(id) ON DELETE CASCADE
);
""",
"""
CREATE UNIQUE INDEX IF NOT EXISTS playlistindex ON playlistfile (playlist,position,path);
""",
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
CREATE TABLE IF NOT EXISTS tags (
    id          INTEGER PRIMARY KEY,
    file        INTEGER,
    tag         TEXT,
    value       TEXT,
    base64      BOOLEAN DEFAULT 0,
    FOREIGN KEY(file) REFERENCES files(id) ON DELETE CASCADE
);
""",
"""
CREATE UNIQUE INDEX IF NOT EXISTS file_tag ON tags (file,tag,value);
""",
]

class SQliteBackend(object):
    """
    Sqlite backend for soundforest databases, implementing one singleton
    instance per unique sqlite database file
    """
    __instances = {}
    def __init__(self,db_path=DB_PATH):
        if not SQliteBackend.__instances.has_key(db_path):
            SQliteBackend.__instances[db_path] = SQliteBackend.SQliteBackendInstance(db_path)
        self.__dict__['SQliteBackend.__instances'] = SQliteBackend.__instances
        self.__dict__['path'] = db_path

    class SQliteBackendInstance(SQLiteDatabase):
        """
        Singleton instance to access given sqlite soundforest database file.
        """
        def __init__(self,db_path=DB_PATH):
            try:
                SQLiteDatabase.__init__(self,db_path,tables_sql=DB_TABLES)
            except SQLiteError,emsg:
                raise SoundForestDBError(
                    'Error initializing database %s: %s' % (db_path,emsg)
                )

        def __getattr__(self,attr):
            try:
                return SQLiteDatabase.__getattr__(self,attr)
            except AttributeError:
                raise SoundForestDBError(
                    'No such SQliteBackend attribute: %s' % attr
                )

    def __getattr__(self,attr):
        return getattr(self.__instances[self.path],attr)

    def __setattr__(self,attr,value):
        return setattr(self.__instances[self.path],attr,value)

    def register_tree_type(self,ttype,description,ignore_duplicate=False):
        """
        Sqlite backend implementation of register_tree_type
        """
        c = self.cursor
        try:
            c.execute(
                'INSERT INTO treetypes (type,description) VALUES (?,?)',
                (ttype,description,),
            )
            self.commit()
        except sqlite3.IntegrityError:
            if ignore_duplicate:
                return
            raise SoundForestDBError(
                'Tree type %s already registered' % ttype
            )
        c.execute(
            'SELECT id FROM treetypes ' +
            'WHERE type=? AND description=?',
            (ttype,description,),
        )
        result = c.fetchone()
        del c
        return result[0]

    def register_tree(self,tree,ignore_duplicate=True):
        """
        Sqlite backend implementation of register_tree

        Returns ID of registered tree.
        """
        c = self.cursor
        c.execute(
            'SELECT id FROM treetypes WHERE type=?',
            (tree.tree_type,),
        )
        result = c.fetchone()
        if result is None:
            raise SoundForestDBError(
                'Tree type not registered: %s' % tree.tree_type
            )
        tt_id = result[0]

        try:
            c.execute(
                'INSERT INTO tree (treetype,source,path) VALUES (?,?,?)',
                (tt_id,tree.source,tree.path,)
            )
            self.commit()
        except sqlite3.IntegrityError:
            if not ignore_duplicate:
                raise SoundForestDBError(
                    'Already registered: %s %s' % (tree.source,tree.path)
                )

        c.execute(
            'SELECT id FROM tree where source=? AND path=?',
            (tree.source,tree.path,)
        )
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

        return tree_id

    def register_removable_media(tree,media,ignore_duplicate=False):
        """
        Sqlite backend method to register removable media
        """
        c = self.cursor
        try:
            c.execute(
                'INSERT INTO removablemedia (name,format,updated) VALUES (?,?,?)',
                (media.name,media.format,0)
            )
            self.commit()

        except sqlite3.IntegrityError:
            if not ignore_duplicate:
                raise SoundForestDBError('Already registered: %s %s' % (
                    media.name,media.format
                    ))
        c.execute(
            'SELECT id FROM removablemedia ' +
            'WHERE name=? AND format=?',
            (media.name,media.format,),
        )
        result = c.fetchone()
        del c
        return result[0]

    def register_playlist_source(self,name,path):
        """
        Register a playlist source name and path
        """
        c = self.cursor
        try:
            c.execute(
                'INSERT INTO playlistsources (name,path) VALUES (?,?)',
                (name,path,)
            )
            self.commit()

        except sqlite3.IntegrityError:
            self.log.debug(
                'ERROR: playlist source already registered: %s' % name
            )
        c.execute(
            'SELECT id FROM playlistsources' +
            'WHERE name=? AND path=?',
            (name,path,),
        )
        result = c.fetchone()
        del c
        return result()

    def register_playlist(self,source_id,folder,name,description,updated):
        """
        Register a playlist to database
        """
        playlist_id = None
        c = self.cursor
        try:
            c.execute(
                'INSERT INTO ' +
                'playlist (source,updated,folder,name,description) ' +
                'VALUES (?,?,?,?,?)',
                (source_id,updated,folder,name,description)
            )
            self.commit()
        except sqlite3.IntegrityError:
            self.log.debug('ERROR: playlist source already registered: %s' % name)
        c.execute(
            'SELECT id FROM playlist ' +
            'WHERE source=? AND folder=? AND name=?',
            (source_id,folder,name,),
        )
        result = c.fetchone()
        del c
        return result[0]

    def register_codec(self,name,description=''):
        """
        Register a codec to database. Returns a Codec object, to add codec
        encoders or decoders use the methods in returned object.

        Codec name must unique in database. Description is just a textual
        description for the codec.
        """
        c = self.cursor
        try:
            c.execute(
                'INSERT INTO codec (name,description) VALUES (?,?)',
                (name,description,)
            )
            self.commit()
        except sqlite3.IntegrityError:
            self.log.debug('ERROR: codec already registered: %s' % name)
        c.execute(
            'SELECT id FROM codec WHERE name=? AND description=?',
            (name,description,)
        )
        result = c.fetchone()
        del c
        return result[0]

    def register_codec_extension(self,codec_id,extension):
        """
        Register given file extension for this codec ID
        """
        c = self.cursor
        try:
            c.execute(
                'INSERT INTO extensions (codec,extension) VALUES (?,?)',
                (codec_id,extension,)
            )
            self.commit()
        except sqlite3.IntegrityError,emsg:
            self.log.debug('Error adding extension %s: %s' % (extension,emsg))
        c.execute(
            'SELECT id FROM extensions WHERE codec=? AND extension=?',
            (codec_id,extension,),
        )
        result = c.fetchone()
        del c
        return result[0]

    def register_codec_encoder(self,codec_id,command,priority=0):
        """
        Register given encoder command for this codec ID
        """
        c = self.cursor
        try:
            c.execute(
                'INSERT INTO encoder (codec,command,priority) VALUES (?,?,?)',
                (codec_id,command,priority,)
            )
            self.commit()
        except sqlite3.IntegrityError,emsg:
            self.log.debug('Error adding encoder %s: %s' % (command,emsg))
        c.execute(
            'SELECT id FROM encoder WHERE codec=? AND command=?',
            (codec_id,command,),
        )
        result = c.fetchone()
        del c
        return result[0]

    def register_codec_decoder(self,codec_id,command,priority=0):
        """
        Register given encoder command for this codec ID
        """
        c = self.cursor
        try:
            c.execute(
                'INSERT INTO decoder (codec,command,priority) VALUES (?,?,?)',
                (codec_id,command,priority,)
            )
            self.commit()
        except sqlite3.IntegrityError,emsg:
            self.log.debug('Error adding decoder %s: %s' % (command,emsg))
        c.execute(
            'SELECT id FROM decoder WHERE codec=? AND command=?',
            (codec_id,command,),
        )
        result = c.fetchone()
        del c
        return result[0]

    def unregister_tree_type(self,ttype):
        """
        Sqlite backend implementation of unregister_tree_type
        """
        try:
            c = self.cursor
            c.execute(
                'DELETE FROM treetypes WHERE type=?',
                (ttype,)
            )
            self.commit()
            del c
        except sqlite3.DatabaseError,emsg:
            raise SoundForestDBError(
                'Error removing tree type: %s' % emsg
            )

    def unregister_tree(self,tree):
        """
        Sqlite backend implementation of unregister_tree
        """
        try:
            c = self.cursor
            c.execute(
                'DELETE FROM tree where source=? AND path=?',
                (tree.source,tree.path,)
            )
            self.commit()
            del c
        except sqlite3.DatabaseError,emsg:
            raise SoundForestDBError(
                'Error removing tree: %s' % emsg
            )

    def unregister_removable_media(self,media):
        """
        Unregister given RemovableMedia instance from database.
        """
        c = self.cursor
        c.execute(
            'DELETE FROM removablemedia ' +
            'WHERE name=? AND format=?',
            (media.name,media.format,),
        )
        self.commit()

    def unregister_playlist_source(self,name,path):
        """
        Remove a playlist source from database
        """
        try:
            c = self.cursor
            c.execute(
                'DELETE from playlistsources WHERE name=? AND path=?',
                (name,path,)
            )
            self.commit()
            del c
        except sqlite3.DatabaseError,emsg:
            raise SoundForestDBError(
                'Error removing playlist source: %s' % emsg
            )

    def unregister_playlist(self,playlist_id):
        """
        Remove a playlist source from database
        """
        try:
            c = self.cursor
            c.execute(
                'DELETE from playlist WHERE id=?',
                (playlist_id,)
            )
            self.commit()
            del c
        except sqlite3.DatabaseError,emsg:
            raise SoundForestDBError(
                'Error removing playlist: %s' % emsg
            )

    def unregister_codec(self,name):
        """
        Remove references to a registered codec from database
        """
        try:
            c = self.cursor
            c.execute('DELETE FROM codec WHERE name=?',(name,))
            self.commit()
            del c
        except sqlite3.DatabaseError,emsg:
            raise SoundForestDBError(
                'Error removing codec: %s' % emsg
            )

    def unregister_codec_extension(self,codec_id,extension):
        """
        Unregister given codec file extension from database.
        """
        c = self.cursor
        c.execute(
            'DELETE FROM extensions WHERE codec=? AND extension=?',
            (codec_id,extension,)
        )
        self.commit()

    def unregister_codec_encoder(self,codec_id,command):
        """
        Unregister given codec encoder command from database.
        """
        c = self.cursor
        c.execute(
            'DELETE FROM encoder WHERE codec=? AND command=?',
            (codec_id,command,)
        )
        self.commit()

    def unregister_codec_decoder(self,codec_id,command):
        """
        Unregister given codec decoder command from database.
        """
        c = self.cursor
        c.execute(
            'DELETE FROM decoder WHERE codec=? AND command=?',
            (codec_id,command,)
        )
        self.commit()

    def get_tree_types(self):
        """
        Return audio tree types registered to database
        """
        c = self.cursor
        c.execute(
            'SELECT type,description FROM treetypes ORDER BY type'
        )
        results = [(r[0],r[1]) for r in c.fetchall()]
        del c
        return results

    def get_trees(self,source=None):
        """
        Sqlite backend to get all audio trees
        """
        c = self.cursor
        if source is not None:
            c.execute(
                'SELECT path,source FROM tree WHERE source=?',
                (source,)
            )
        else:
            c.execute(
                'SELECT path,source FROM tree'
            )
        results = [(r[0],r[1]) for r in c.fetchall()]
        del c
        return results

    def get_tree_aliases(self,source=None):
        """
        Sqlite backend to get all audio trees
        """
        c = self.cursor
        if source is None:
            c.execute(
                'SELECT a.alias,t.path FROM aliases AS a, tree as t ' +
                'WHERE a.tree=t.id'
            )
        else:
            c.execute(
                'SELECT a.alias,t.path FROM aliases AS a, tree as t ' +
                'WHERE a.tree=t.id AND t.source=?',
                (source,)
            )
        results = [(r[0],r[1]) for r in c.fetchall()]
        del c
        return results

    def get_tree(self,path,source):
        """
        Retrieve AudioTree matching give path from database.

        Raises ValueError if path was not found.
        """
        c = self.cursor
        c.execute('SELECT t.id,tt.type,t.path,t.source ' +
                  'FROM treetypes AS tt, tree AS t ' +
                  'WHERE tt.id=t.treetype AND t.source=? AND t.path=?',
            (source,path,)
        )
        result = c.fetchone()
        if result is None:
            # Compare path prefix to tree paths
            mpath = path.split(os.sep)
            for tree,source in self.get_trees(source=source):
                tpath = tree.split(os.sep)
                if (tpath[:len(mpath)] != mpath):
                    continue
                result = c.execute(
                    'SELECT t.id,tt.type,t.path.t.source ' +
                    'FROM FROM treetypes AS tt, tree AS t '
                    'WHERE tt.id=t.treetype AND t.source=? AND t.path=?',
                    (source,tree,)
                )
                break
        if result is None:
            raise ValueError('Path not in database: %s' % path)
        details = self.__result2dict__(c,result)
        c.execute('SELECT alias FROM aliases WHERE tree=?',(result[0],))
        aliases = [r[0] for r in c.fetchall()]
        return details,aliases

    def match_tree(self,path,source):
        """
        Retrieve AudioTree matching given path prefix from database.

        Raises ValueError if path was not found.
        """
        c = self.cursor
        c.execute('SELECT t.id,tt.type,t.path,t.source ' +
                  'FROM treetypes AS tt, tree AS t ' +
                  'WHERE tt.id=t.treetype AND t.source=? AND t.path like ?',
            (source,'%s%%' % path,)
        )
        result = c.fetchone()
        if result is not None:
            details = self.__result2dict__(c,result)
            c.execute('SELECT alias FROM aliases WHERE tree=?',(result[0],))
            aliases = [r[0] for r in c.fetchall()]
            return details,aliases
        else:
            # Compare path prefix to alias paths
            mpath = path.strip('os.sep').split(os.sep)
            for tree_path,source in self.get_trees(source=source):
                tpath = tree_path.split(os.sep)
                if (mpath[:len(tpath)] != tpath):
                    continue
                return self.get_tree(tree_path,source)
        raise ValueError('No path match found: %s' % path)

    def match_tree_aliases(self,path,source):
        """
        Retrieve AudioTree matching given path aliases from database.

        Raises ValueError if path was not found.
        """
        tree_id = None
        c = self.cursor
        c.execute(
            'SELECT a.alias,t.path ' +
            'FROM treetypes AS tt, tree AS t, aliases AS a ' +
            'WHERE tt.id=t.treetype AND t.source=? ' +
            'AND a.tree=t.id AND a.alias like ?',
            (source,'%s%%' % path,)
        )
        result = c.fetchone()
        if result is not None:
            return self.get_tree(result[1],source)
        else:
            # Compare path prefix to alias paths
            mpath = path.strip('os.sep').split(os.sep)
            for alias,tree_path in self.get_aliases(source=source):
                apath = alias.split(os.sep)
                if (mpath[:len(apath)] != apath):
                    continue
                return self.get_tree(tree_path,source)
                break
        raise ValueError('No alias matches %s' % path)

    def match_tree_extension(self,tree_id,path):
        """
        Return files matching given extension from audio tree
        """
        try:
            c = self.cursor
            c.execute(
                'SELECT directory,filename FROM files ' +
                'WHERE tree=? AND extension=?',
                (tree_id,'%s%%' % extension,),
            )
            results = c.fetchall()
        except sqlite3.DatabaseError,emsg:
            raise SoundForestDBError('Error matching extension: %s' % emsg)
        paths = []
        for directory,filename in results:
            paths.append(os.path.join(directory,filename))
        return paths

    def match_tree_path(self,tree_id,path):
        """
        Return file paths matching given path from audio tree
        """
        try:
            c = self.cursor
            c.execute(
                'SELECT directory,filename FROM files ' +
                'WHERE tree=? and directory like ?',
                (tree_id,'%s%%' % path,),
            )
            results = c.fetchall()
        except sqlite3.DatabaseError,emsg:
            raise SoundForestDBError('Error matching path: %s' % emsg)
        paths = []
        for directory,filename in results:
            paths.append(os.path.join(directory,filename))
        return paths

    def match_filename(self,tree_id,name):
        """
        Return file paths matching given name from audio tree
        """
        try:
            c = self.cursor
            c.execute(
                'SELECT directory,filename FROM files ' +
                'WHERE tree=? and filename like ?',
                (tree_id,'%%%s%%' % name,),
            )
            results = c.fetchall()
        except sqlite3.DatabaseError,emsg:
            raise SoundForestDBError('Error matching name: %s' % emsg)
        paths = []
        for directory,filename in results:
            paths.append(os.path.join(directory,filename))
        return paths

    def cleanup_tree(self,tree_id):
        """
        Sqlite backend implementation of cleanup_tree
        """
        c = self.cursor
        c.execute(
            'DELETE FROM files WHERE tree=? and deleted=1',
            (tree_id,)
        )
        self.commit()
        del c

    def tree_add_file(self,tree_id,directory,filename,mtime):
        """
        Sqlite backend implemenatation of tree_append_file
        """
        extension = os.path.splitext(filename)[1][1:]
        try:
            c = self.cursor
            c.execute(
                'INSERT INTO files (tree,mtime,directory,filename,extension) ' +
                ' VALUES (?,?,?,?,?)',
                (tree_id,mtime,directory,filename,extension)
            )
        except sqlite3.IntegrityError,emsg:
            raise SoundForestDBError('Error adding song: %s' % emsg)

    def tree_remove_file(self,tree_id,directory,filename,mtime):
        """
        Sqlite backend implemenatation of tree_remove_file
        """
        extension = os.path.splitext(filename)[1][1:]
        try:
            c = self.cursor
            c.execute(
                'DELETE FROM files WHERE tree=? AND directory=? AND filename=?',
                (tree_id,directory,filename,extension)
            )
            self.commit()
        except sqlite3.IntegrityError,emsg:
            raise SoundForestDBError('Error deleting song: %s' % emsg)

    def update_file_shasum(self,file_id,shasum):
        """
        Sqlite backend implementation of update_tree_file_shasum
        """
        c = self.db.cursor
        c.execute(
            'UPDATE files set shasum=? WHERE id=?',
            (shasum,file_id,)
        )
        self.db.commit()
        del c

    def update_file_mtime(self,file_id,mtime):
        """
        Sqlite backend implementation of update_tree_file_mtime
        """
        try:
            c = self.cursor
            c.execute(
                'UPDATE files SET mtime=? WHERE id=?',
                (mtime,file_id),
            )
            self.commit()
        except sqlite3.IntegrityError,emsg:
            raise SoundForestDBError('Error updating mtime: %s' % emsg)

    def set_file_deleted_flag(self,directory,filename,value=1):
        """
        Sqlite backend implementation of tree_file_deleted_flag
        """
        try:
            c = self.cursor
            c.execute(
                'UPDATE files SET deleted=? WHERE directory=? AND filename=?',
                (value,directory,filename,)
            )
            self.commit()
            del c
        except sqlite3.IntegrityError,emsg:
            raise SoundForestDBError('Error updating deleted flag: %s' % emsg)

    def get_directories(self,tree_id):
        """
        Sqlite backend implementation to return audio tree directories
        """
        c = self.cursor
        c.execute(
            'SELECT distinct directory FROM files WHERE tree=? AND deleted=0 ' +
            'ORDER BY directory',
            (tree_id,)
        )
        directories = [r[0] for r in c.fetchall()]
        del c
        return directories

    def get_files(self,tree_id,directory=None):
        """
        Sqlite backend method to return files matching tree ID
        """
        c = self.cursor
        if directory is not None:
            c.execute(
                'SELECT directory,filename ' +
                'FROM files ' +
                'WHERE tree=? AND deleted==0 AND directory=?' +
                'ORDER BY directory,filename',
                (tree_id,directory,)
            )
        else:
            c.execute(
                'SELECT directory,filename ' +
                'FROM files WHERE tree=? AND deleted==0 ' +
                'ORDER BY directory,filename',
                (tree_id,)
            )
        files = [os.path.join(r[0],r[1]) for r in c.fetchall()]
        del c
        return files

    def get_file_details(self,tree_id,directory,filename):
        """
        Sqlite backend implementation of get_tree_File_details
        """
        c = self.cursor
        c.execute(
            'SELECT * FROM files WHERE tree=? AND directory=? AND filename=?',
            (tree_id,directory,filename,)
        )
        result = c.fetchone()
        if result is None:
            return {}
        details = self.__result2dict__(c,result)
        del c
        return details

    def file_events(self,tree_id,time_start=None):
        """
        Return list of change events for a file path in given library.

        If time_start is given, it is used as EPOC timestamp to filter out
        events older than given value
        """
        try:
            c = self.cursor
            if time_start is  None:
                c.execute(
                    'SELECT f.directory,f.filename,fc.mtime,fc.event FROM filechanges AS fc, files AS f ' +\
                    'WHERE file in (SELECT f.id FROM files AS f, tree AS t WHERE f.tree=t.id AND t.id=?) ' +\
                    'ORDER BY fc.mtime',
                    (tree_id,)
                )
            else:
                try:
                    time_start = int(time_start)
                    if time_start<0:
                        raise ValueError
                except ValueError:
                    raise SoundForestDBError(
                        'Invalid time_start timestamp: %s' % time_start
                    )
                c.execute(
                    'SELECT f.directory,f.filename,fc.mtime,fc.event FROM filechanges AS fc, files AS f ' +\
                    'WHERE fc.mtime>=? AND file in (SELECT f.id FROM files AS f, tree AS t WHERE f.tree=t.id AND t.id=?) ' +\
                    'ORDER BY fc.mtime',
                    (time_start,tree_id,)
                )
            results = []
            for r in [self.adb.__result2dict__(c,r) for r in c.fetchall()]:
                r['path'] = os.path.join(r['directory'],r['filename'])
                del r['directory']
                del r['filename']
                results.append(r)
            del c
        except sqlite3.DatabaseError,emsg:
            raise SoundForestDBError(
                'Error querying file events: %s' % emsg
            )
        return results

    def create_file_event(self,tree_id,path,event):
        """
        Sqlite backend implementation for create_file_event
        """
        mtime = time.mktime(time.localtime())
        directory = os.path.dirname(path)
        filename = os.path.basename(path)
        try:
            c = self.cursor
            c.execute(
                'SELECT id FROM files WHERE tree=? AND directory=? AND filename=?',
                (tree_id,directory,filename)
            )
            file_id = c.fetchone()[0]
            c.execute(
                'INSERT INTO filechanges (file,mtime,event) VALUES (?,?,?)',
                (file_id,mtime,event)
            )
            self.commit()
            del c
        except AttributeError:
            raise SoundForestDBError('File not in library: %s' % path)
        except sqlite3.DatabaseError,emsg:
            raise SoundForestDBError('Error creating file event: %s' % emsg)

    def get_playlist_sources(self):
        """
        Return all playlist sources
        """
        c = self.cursor
        c.execute('SELECT id,name,path FROM playlistsources');
        result = c.fetchall()
        return [{'source_id':r[0],'name':r[1],'path':r[2]} for r in result]

    def get_playlist_source_id(self,name,path):
        """
        Return playlist source ID matching name and path
        """
        source_id = None
        try:
            c = self.cursor
            c.execute(
                'SELECT id FROM playlistsources WHERE name=? AND path=?',
                (name,path,)
            )
            result = c.fetchone()
            if result is not None:
                source_id = result[0]
        except sqlite3.DatabaseError,emsg:
            raise SoundForestDBError(
                'Error querying playlist source ID: %s' % emsg
            )
        return source_id

    def get_playlist_source_playlists(self,source_id):
        """
        Return playlists matching given playlist source ID
        """
        playlists = []
        c = self.cursor
        c.execute(
            'SELECT id,folder,name FROM playlist WHERE source=?',
            (source_id,)
        )
        result = c.fetchall()
        del c
        return [{'playlist_id':r[0],'folder':r[1],'name':r[2]} for r in result]

    def get_playlist_id(self,source_id,name,path):
        """
        Return playlist ID matching source, name and path
        """
        c = self.cursor
        c.execute(
            'SELECT id FROM playlist WHERE source=? AND folder=? AND name=?',
            (source_id,folder,name,)
        )
        result = c.fetchone()
        if result is None:
            return None
        return result[0]

    def get_playlist(self,source_id,folder,name):
        """
        Return playlist details matching source, name and path
        """
        c = self.cursor
        c.execute(
            'SELECT id,source,folder,name,description,updated ' +
            'FROM playlist WHERE source=? AND folder=? AND name=?',
            (source_id,folder,name,)
        )
        result = c.fetchone()
        if result is None:
            return None
        value = self.__result2dict__(c,result)
        del c
        return value

    def get_playlist_tracks(self,playlist_id):
        """
        Return playlist track paths for given playlist ID
        """
        c = self.cursor
        c.execute(
            'SELECT path from playlistfile '
            'WHERE playlist=? ORDER BY position',
            (playlist_id,)
        )
        return [r[0] for r in c.fetchall()]

    def clear_playlist(self,playlist_id):
        """
        Clear tracks from given playlist
        """
        try:
            c = self.cursor
            c.execute(
                'DELETE FROM playlistfile WHERE playlist=?',
                (playlist_id,)
            )
        except sqlite3.DatabaseError,emsg:
            raise SoundForestDBError(
                'Error replacing playlist tracks: %s' % emsg
            )

    def replace_playlist_tracks(self,playlist_id,tracks):
        """
        Replace playlist tracks with given track paths
        """
        self.clear_playlist(playlist_id)
        for index,path in enumerate(tracks):
            try:
                c = self.cursor
                c.execute(
                    'INSERT INTO playlistfile (playlist,path,position)' +
                    'VALUES (?,?,?)',
                    (playlist_id,path,index+1,)
                )
                self.commit()
            except sqlite3.DatabaseError,emsg:
                raise SoundForestDBError(
                    'Error adding track %s to playlist: %s' % (path,emsg)
                )

    def get_registered_codecs(self):
        """
        Returns list of Codec objects matching codecs registered to database
        """
        c = self.cursor
        c.execute('SELECT id,name,description from codec')
        results = [(r[0],r[1],r[2]) for r in c.fetchall()]
        del c
        return results

    def get_codec(self,name):
        """
        Return Codec instance for given codec name.
        Raises CodecError if codec name is not configured
        """
        c = self.cdb.cursor
        c.execute('SELECT id,description FROM codec where name=?',(name,))
        result = c.fetchone()
        if result is None:
            return None
        codec_id = result[0]
        description = result[1]
        del c
        return codec_id,name,description

    def get_codec_extensions(self,codec_id):
        """
        Return file extensions associated to given codec db ID.
        """
        c = self.cursor
        c.execute(
            'SELECT extension FROM extensions WHERE codec=?',
            (codec_id,)
        )
        results = c.fetchall()
        if results:
            extensions = [r[0] for r in results]
        else:
            extensions = []
        del c
        return extensions

    def get_codec_encoders(self,codec_id):
        """
        Return encoder commands associated to given codec ID
        """
        c = self.cursor
        c.execute(
            'SELECT command FROM encoder WHERE codec=? ORDER BY priority DESC',
            (codec_id,)
        )
        encoders = [r[0] for r in c.fetchall()]
        del c
        return encoders

    def get_codec_decoders(self,codec_id):
        """
        Return decoder commands associated to given codec ID
        """
        c = self.cursor
        c.execute(
            'SELECT command FROM decoder WHERE codec=? ORDER BY priority DESC',
            (codec_id,)
        )
        decoders = [r[0] for r in c.fetchall()]
        del c
        return decoders

    def set_labels(self,labels,source):
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
            self.set_taglabel(
                tag,position,label,description,replace=overwrite
            )

    def get_labels(self,source):
        """
        Return standard tag labels, ordered by position
        """
        c = self.cursor
        c.execute(
            'SELECT tag FROM labels ORDER BY position'
        )
        return [r[0] for r in c.fetchall()]

    def set_label(self,tag,position,label,description,source):
        """
        Define a tag label for user interfaces
        """
        c = self.cursor
        if replace:
            c.execute('DELETE FROM labels WHERE tag=?',(tag,))
            self.commit()
        try:
            c.execute(
                'INSERT INTO labels (tag,position,label,description) ' +
                'VALUES (?,?,?,?)',
                (tag,position,label,description,)
            )
            self.commit()
        except sqlite3.IntegrityError:
            raise TagError(
                'Tag label already defined: %s %s' % (tag,label)
            )
        del c

    def get_label(self,tag,source):
        """
        Return info for a tag label
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

    def set_tags(self,path,tags,source):
        """
        Set given tags for given filename and data source
        """
        directory = os.path.dirname(path)
        filename = os.path.basename(path)
        c = self.cursor
        c.execute(
            'SELECT f.id FROM files AS f, tree AS t ' +
            'WHERE f.directory=? AND f.filename=? ' +
            'AND f.tree=t.id AND t.source=?',
            (directory,filename,source,),
        )
        result = c.fetchone()
        if result is None:
            raise ValueError('File matching source not found')
        file_id = result[0]
        for tag in tags:
            if not isinstance(tag,dict):
                raise ValueError('Tag to set is not dictionary')
            try:
                key = tag['tag']
                value = tag['value']
                base64 = tag['base64'] and 1 or 0
            except KeyError:
                raise ValueError('Invalid tag to set')
            c.execute(
                'DELETE FROM tags ' +
                'WHERE file=? AND tag=?',
                (file_id,key,)
            )
            c.execute(
                'INSERT INTO tags (file,tag,value,base64) VALUES (?,?,?,?)',
                (file_id,key,value,base64,),
            )
        self.commit()

    def get_tags(self,path,source):
        """
        Return tags for given path and data source
        """
        directory = os.path.dirname(path)
        filename = os.path.basename(path)
        c = self.cursor
        c.execute(
            'SELECT tag,value,base64 FROM tags WHERE file=' +
            '(SELECT f.id FROM files AS f, tree AS t ' +
            'WHERE f.directory=? AND f.filename=? ' +
            'AND f.tree=t.id AND t.source=?)',
            (directory,filename,source,),
        )
        return [self.__result2dict__(c,r) for r in c.fetchall()]

