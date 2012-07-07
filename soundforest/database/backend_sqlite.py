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
            raise SoundForestDBError(
                'Tree type %s already registered' % ttype
            )

    def register_tree(self,tree,ignore_duplicate=True):
        """
        Sqlite backend implementation of register_tree
        """
        c = self.cursor
        c.execute('SELECT id FROM treetypes WHERE type=?',(tree.tree_type,))
        result = c.fetchone()
        if result is None:
            raise SoundForestDBError('Tree type not registered: %s' % tree.tree_type)
        tt_id = result[0]

        try:
            c.execute(
                'INSERT INTO tree (treetype,source,path) VALUES (?,?,?)',
                (tt_id,tree.source,tree.path,)
            )
            self.commit()
        except sqlite3.IntegrityError:
            if not ignore_duplicate:
                raise SoundForestDBError('Already registered: %s %s' % (tree.source,tree.path))

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

        return tree_id

    def register_removable_media(tree,removable_media,ignore_duplicate):
        """
        Sqlite backend method to register removable media
        """
        c = self.cursor
        c.execute('')
        try:
            c.execute(
                'INSERT INTO removablemedia (name,format,updated) VALUES (?,?,?)',
                (removable_media.name,removable_media.format,0)
            )
            self.commit()
        except sqlite3.IntegrityError:
            if not ignore_duplicate:
                raise SoundForestDBError('Already registered: %s %s' % (
                    removable_media.name,removable_media.format
                    ))
        del c

    def register_playlist_source(self,name,path):
        """
        Register a playlist source name and path
        """
        try:
            c = self.cursor
            c.execute(
                'INSERT INTO playlistsources (name,path) VALUES (?,?)',
                (name,path,)
            )
            self.commit()
            del c
        except sqlite3.IntegrityError:
            self.log.debug('ERROR: playlist source already registered: %s' % name)
        return self.get_playlist_source_id(name,path)

    def register_playlist(self,source_id,folder,name,description,updated):
        """
        Register a playlist to database
        """
        playlist_id = None
        try:
            c = self.cursor
            c.execute(
                'INSERT INTO playlist (source,updated,folder,name,description) VALUES (?,?,?,?,?)',
                (source_id,updated,folder,name,description)
            )
            self.commit()
            del c
        except sqlite3.IntegrityError:
            self.log.debug('ERROR: playlist source already registered: %s' % name)

    def clear_playlist(self,playlist_id):
        """
        Clear all songs from given playlist
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
        Register given track to playlist with given list position
        """
        self.clear_playlist(playlist_id)
        for index,path in enumerate(tracks):
            try:
                c = self.cursor
                c.execute(
                    'INSERT INTO playlistfile (playlist,path,position) VALUES (?,?,?)',
                    (playlist_id,path,index+1,)
                )
                self.commit()
            except sqlite3.DatabaseError,emsg:
                raise SoundForestDBError(
                    'Error adding track %s to playlist: %s' % (path,emsg)
                )

    def register_codec(self,name,description=''):
        """
        Register a codec to database. Returns a Codec object, to add codec
        encoders or decoders use the methods in returned object.

        Codec name must unique in database. Description is just a textual
        description for the codec.
        """
        codec_id = None
        try:
            c = self.cursor
            c.execute(
                'INSERT INTO codec (name,description) VALUES (?,?)',
                (name,description,)
            )
            self.commit()
            c.execute(
                'SELECT id FROM codec WHERE name=? AND description=?',
                (name,description,)
            )
            codec_id = c.fetchone()[0]
            del c
        except sqlite3.IntegrityError:
            self.log.debug('ERROR: codec already registered: %s' % name)
        return codec_id

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

    def unregister_playlist(self,source_id,name,path):
        """
        Remove a playlist source from database
        """
        try:
            c = self.cursor
            c.execute(
                'DELETE from playlistsources WHERE source=? AND name=? AND path=?',
                (source_id,name,path,)
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

    def get_trees(self):
        """
        Sqlite backend to get all audio trees
        """
        c = self.cursor
        c.execute('SELECT path,source FROM tree')
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
            raise SoundForestDBError('Path not in database: %s' % path)
        details = self.__result2dict__(c,result)
        c.execute('SELECT alias FROM aliases WHERE tree=?',(result[0],))
        aliases = [r[0] for r in c.fetchall()]
        return details,aliases

    def get_tree_directories(self,tree_id):
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
            'SELECT id,source,folder,name,description,updated FROM playlist WHERE source=? AND folder=? AND name=?',
            (source_id,folder,name,)
        )
        result = c.fetchone()
        if result is None:
            return None
        value = self.__result2dict__(c,result)
        del c
        return value

    def get_registered_codecs(self):
        """
        Returns list of Codec objects matching codecs registered to database
        """
        c = self.cursor
        c.execute('SELECT id,name,description from codec')
        results = [(r[0],r[1],r[2]) for r in c.fetchall()]
        del c
        return results

    def get_playlist_tracks(self,playlist_id):
        c = self.cursor
        c.execute(
            'SELECT path from playlistfile WHERE playlist=? ORDER BY position',
            (playlist_id,)
        )
        return [r[0] for r in c.fetchall()]

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

    def register_codec_extension(self,codec_id,extension):
        """
        Register given file extension for this codec ID
        """
        try:
            c = self.cursor
            c.execute(
                'INSERT INTO extensions (codec,extension) VALUES (?,?)',
                (codec_id,extension,)
            )
            self.commit()
            del c
        except sqlite3.IntegrityError,emsg:
            self.log.debug('Error adding extension %s: %s' % (extension,emsg))

    def register_codec_encoder(self,codec_id,command,priority=0):
        """
        Register given encoder command for this codec ID
        """
        try:
            c = self.cursor
            c.execute(
                'INSERT INTO encoder (codec,command,priority) VALUES (?,?,?)',
                (codec_id,command,priority,)
            )
            self.commit()
            del c
        except sqlite3.IntegrityError,emsg:
            self.log.debug('Error adding encoder %s: %s' % (command,emsg))

    def register_codec_decoder(self,codec_id,command,priority=0):
        """
        Register given encoder command for this codec ID
        """
        try:
            c = self.cursor
            c.execute(
                'INSERT INTO decoder (codec,command,priority) VALUES (?,?,?)',
                (codec_id,command,priority,)
            )
            self.commit()
            del c
        except sqlite3.IntegrityError,emsg:
            self.log.debug('Error adding decoder %s: %s' % (command,emsg))

    def directory_files(self,tree_id,directory):
        """
        Sqlite backend implementation of directory_files
        """
        c = self.cursor
        c.execute(
            'SELECT filename FROM files WHERE tree=? AND directory=?',
            (tree_id,directory,)
        )
        del c
        return [r[0] for r in c.fetchall()]

    def get_tree_files(self,tree_id):
        """
        Sqlite backend method to return files matching tree ID
        """
        c = self.cursor
        c.execute(
            'SELECT directory,filename FROM files WHERE tree=? AND deleted==0 ' + \
            'ORDER BY directory,filename',
            (tree_id,)
        )
        files = [os.path.join(r[0],r[1]) for r in c.fetchall()]
        del c
        return files

    def get_tree_file_details(self,tree_id,directory,filename):
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

    def get_tree_fields(self,tree_id,fields):
        """
        Sqlite backend implementation of get_tree_files_details
        """
        c = self.cursor
        c.execute(
            'SELECT %s FROM files WHERE tree=? ORDER BY directory,filename' % ','.join(fields),
            (tree_id,)
        )
        details = [self.__result2dict__(c,r) for r in c.fetchall()]
        del c
        return details

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

    def tree_append_file(self,tree_id,directory,filename,mtime):
        """
        Sqlite backend implemenatation of tree_append_file
        """
        try:
            c = self.cursor
            c.execute(
                'INSERT INTO files (tree,mtime,directory,filename) VALUES (?,?,?,?)',
                (tree_id,mtime,directory,filename)
            )
        except sqlite3.IntegrityError,emsg:
            raise SoundForestDBError('Error adding song: %s' % emsg)

    def update_tree_file_shasum(self,file,shasum):
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

    def update_tree_file_mtime(self,file_id,mtime):
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

    def tree_file_deleted_flag(self,directory,filename,value=1):
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

    def tree_file_events(self,tree_id,time_start=None):
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
