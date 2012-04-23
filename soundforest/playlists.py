"""
Storage of playlists to database.

Purpose of this code is to store the playlists with timestamps, allowing to
compare same playlist from different sources when no timestamps are available
in the source: this allows merging of playlists, while not flawlessly but
to some degree.

Please note the playlist class does not make any considerations what type
of files are stored: you can add any strings to it.
"""

import os,sqlite3,logging,time

from systematic.sqlite import SQLiteDatabase,SQLiteError

DB_PATH = os.path.expanduser('~/.soundforest/playlists.sqlite')
DB_TABLES = [
"""
CREATE TABLE IF NOT EXISTS sources (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    path        TEXT
);
""",
"""
CREATE UNIQUE INDEX IF NOT EXISTS source_paths ON sources (name,path);
""",
"""
CREATE TABLE IF NOT EXISTS playlist (
    id          INTEGER PRIMARY KEY,
    source      INTEGER,
    updated     INTEGER,
    folder      TEXT,
    name        TEXT,
    description TEXT,
    FOREIGN KEY(source) REFERENCES sources(id) ON DELETE CASCADE
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

class PlaylistError(Exception):
    """
    Exceptions from playlist database processing
    """
    def __str__(self):
        return self.args[0]

class PlaylistDB(object):
    """
    Database of playlists from DJ sources
    """
    __instance = None
    def __init__(self,db_path=DB_PATH):
        if PlaylistDB.__instance is None:
            PlaylistDB.__instance = PlaylistDB.PlaylistDBInstance(db_path)
        self.__dict__['_PlaylistDB.__instance'] = PlaylistDB.__instance

    class PlaylistDBInstance(SQLiteDatabase):
        """
        Singleton instance of one PlaylistDB database
        """
        def __init__(self,db_path):
            try:
                SQLiteDatabase.__init__(self,db_path,tables_sql=DB_TABLES)
            except SQLiteError,emsg:
                raise PlaylistError(
                    'Error initializing database %s: %s' % (db_path,emsg)
                )

        def __getattr__(self,attr):

            try:
                return SQLiteDatabase.__getattr__(self,attr)
            except AttributeError:
                raise AttributeError(
                    'No such PlaylistDB attribute: %s' % attr
                )

    def __getattr__(self,attr):
        if attr == 'sources':
            c = self.cursor
            c.execute('SELECT name,path FROM sources')
            results = [PlaylistSource(self,r[0],r[1]) for r in c.fetchall()]
            del c
            return results
        return getattr(self.__instance,attr)

    def __setattr__(self,attr,value):
        return setattr(self.__instance,attr,value)

    def add_source(self,path,source='filesystem'):
        """
        Add a playlist data source
        source  Name of application - 'filesystem' if not given
        path    Path to source file or database
        """
        c = self.cursor
        try:
            c.execute(
                'INSERT INTO sources (name,path) VALUES (?,?)',
                (source,path,)
            )
        except sqlite3.IntegrityError:
            raise PlaylistError('Source already configured')
        self.commit()
        del c
        return PlaylistSource(self,source,path)

    def remove_source(self,source,path):
        """
        Remove a playlist data source from database
        source  Name of application
        path    Path to source file or database
        """
        c = self.cursor
        c.execute(
            'DELETE FROM sources WHERE name=? AND path=?',
            (source,path,)
        )
        del c

    def get_source(self,source,path):
        """
        Return named source from database
        source  Name of application
        path    Path to source file or database
        """
        try:
            return PlaylistSource(self,source,path)
        except KeyError:
            return None

class PlaylistSource(object):
    """
    Playlist program source database entry
    """
    def __init__(self,db,name,path):
        self.__next = None
        self.__itervalues = []
        self.log = logging.getLogger('modules')
        self.db = db
        self.name = name
        self.path = path

        c = self.db.cursor
        c.execute(
            'SELECT id FROM sources WHERE name=? AND path=?',
            (self.name,self.path,)
        )
        result = c.fetchone()
        del c
        if result is None:
            raise KeyError('Source not configured: %s %s' % (name,path))
        self.id = result[0]

    def __getattr__(self,attr):
        if attr == 'playlists':
           with self.db.conn:
                return [DBPlaylist(self.db,self,os.path.join(r[0],r[1])) for r in self.db.conn.execute(
                    'SELECT folder,name FROM playlist WHERE source=?',
                    (self.id,)
                )]
        raise AttributeError('No such PlaylistSource attribute: %s' % attr)

    def __repr__(self):
        return 'PlaylistSource: %s %s' % (self.name,self.path)

    def add_playlist(self,path,description=None):
        """
        Add a playlist to database
        path        Playlist 'path' (folder,name)
        description Optional playlist description
        """
        folder = os.path.dirname(path)
        name = os.path.basename(path)
        timestamp = int(time.mktime(time.localtime()))
        c = self.db.cursor
        try:
            c.execute(
                'INSERT INTO playlist (source,updated,folder,name,description) VALUES (?,?,?,?,?)',
                (self.id,timestamp,folder,name,description)
            )
        except sqlite3.IntegrityError:
            raise PlaylistError('Playlist already in database.')
        del c
        return DBPlaylist(self.db,self,path)

    def remove_playlist(self,playlist):
        """
        Remove given playlist from database
        source      PlaylistSource object
        playlist    DBPlaylist instance
        """
        c = self.db.cursor
        c.execute(
            'DELETE FROM playlist WHERE source=? AND folder=? AND name=?',
            (self.id,playlist.folder,playlist.name,)
        )
        self.db.commit()
        del c

    def get_playlist(self,path):
        """
        Retrieve given playlist from database
        path        Playlist 'path' (folder,name)
        """
        try:
            playlist = DBPlaylist(self.db,self,path)
        except KeyError:
            return None
        return playlist

    def __iter__(self):
        return self

    def next(self):
        """
        Iterate over DBPlaylist items
        """
        if self.__next is None:
            self.__next = 0
            self.__itervalues = self.playlists
        try:
            entry = self.__itervalues[self.__next]
            self.__next += 1
            return entry
        except IndexError:
            self.__next = None
            raise StopIteration

class DBPlaylist(list):
    """
    Database playlist from one playlist source
    source      PlaylistSource object
    path        Playlist 'path' (folder,name)

    Please note path is not intended to be filesystem path, but
    path to the playlist in hierarchical playlist trees. However,
    we DO use os.sep as path separator.
    """
    def __init__(self,db,source,path):
        list.__init__(self)
        self.log = logging.getLogger('modules')
        self.db = db
        self.source = source
        self.path = path
        self.folder = os.path.dirname(path)
        self.name = os.path.basename(path)

        if not isinstance(source,PlaylistSource):
            raise PlaylistError('Source must be PlaylistSource instance')

        c = self.db.cursor
        c.execute(
            'SELECT id,updated FROM playlist WHERE source=? AND folder=? AND name=?',
            (self.source.id,self.folder,self.name,)
        )
        result = c.fetchone()
        del c
        if result is None:
            raise KeyError('Playlist not configured: %s %s' % (source,path))
        self.id = result[0]
        self.updated = result[1]

    def __repr__(self):
        return 'Playlist %s' % self.path

    def clear(self):
        """
        Clear playlist entries from database and this object
        """
        self.__delslice__(0,len(self))
        c = self.db.cursor
        c.execute('DELETE FROM playlistfile WHERE playlist=?',(self.id,))
        self.db.commit()
        del c

    def update(self,entries):
        """
        Update playlist entries from list of paths, adding entries to this object
        """
        self.__delslice__(0,len(self))
        c = self.db.cursor
        c.execute('DELETE FROM playlistfile WHERE playlist=?',(self.id,))
        for index,entry in enumerate(entries):
            c.execute(
                'INSERT INTO playlistfile (playlist,position,path) VALUES (?,?,?)',
                (self.id,index+1,entry,)
            )
            self.append(entry)
        c.execute(
            'UPDATE playlist SET updated=? WHERE id=?',
            (int(time.mktime(time.localtime())),self.id)
        )
        self.db.commit()
        del c

if __name__ == '__main__':
    import sys
    pld = None
    try:
        pld = PlaylistDB()
    except PlaylistError,emsg:
        print emsg
        sys.exit(0)
    src = 'filesystem'
    path = '/Users/hile/Music/Playlists'
    source = pld.get_source(src,path)
    if not source:
        pld.add_source(path)

    for source in pld.sources:
        print source
        pl = source.get_playlist('Testi!')
        if pl is None:
            pl = source.add_playlist('Testi 1')
        pl.update(os.listdir('/Users/hile'))
        for path in pl:
            print path
        pl.clear()

    for pl in source:
        print pl
        source.remove_playlist(pl)
