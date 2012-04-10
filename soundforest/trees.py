"""
Module to detect and store known audio file library paths
"""

import os,sqlite3

from systematic.shell import normalized 
from systematic.sqlite import SQLiteDatabase

DB_PATH = os.path.join(os.getenv('HOME'),'.soundforest','trees.sqlite')

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
    id      INTEGER PRIMARY KEY,
    tree    INTEGER,
    path    TEXT UNIQUE,
    mtime   INTEGER,
    FOREIGN KEY(tree) REFERENCES tree(id) ON DELETE CASCADE
);
""",
"""CREATE UNIQUE INDEX IF NOT EXISTS file_paths ON files (tree,path)""",
"""CREATE UNIQUE INDEX IF NOT EXISTS file_mtimes ON files (path,mtime)""",
]

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

    def __init__(self,db_path=DB_PATH):
        if AudioTreeDB.__instance is None:
            AudioTreeDB.__instance = AudioTreeDB.AudioTreeDBInstance(db_path)
        self.__dict__['_AudioTreeDB.__instance'] = AudioTreeDB.__instance

    def __getattr__(self,attr):
        return getattr(self.__instance,attr)

    def __setattr__(self,attr,value):
        return setattr(self.__instance,attr,value)

    class AudioTreeDBInstance(SQLiteDatabase):
        def __init__(self,db_path=DB_PATH):
            SQLiteDatabase.__init__(self,db_path,tables_sql=DB_TABLES)
            for ttype,description in DEFAULT_TREE_TYPES.items():
                self.register_tree_type(ttype,description,ignore_duplicate=True)

        def __getattr__(self,attr):
            if attr == 'tree_types':
                results = self.conn.execute(
                    'SELECT id,type,description from treetypes'
                )
                return sorted(
                    [AudioTreeType(*r) for r in results],
                    lambda x,y: cmp(x.tree_type,y.tree_type) 
                )
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

        def register_tree_type(self,ttype,description,ignore_duplicate=False):
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
            c = self.cursor
            c.execute(
                'SELECT t.path FROM treetypes as tt, tree as t ' +
                'WHERE tt.id=t.treetype and tt.type=?',
                (tree_type,)
            )
            return [self.get_tree(r[0]) for r in c.fetchall()]

class AudioTreeType(object):
    def __init__(self,tree_id,tree_type,description):
        self.adb = AudioTreeDB()
        self.tree_id = tree_id
        self.tree_type = tree_type
        self.description = description

    def __repr__(self):
        return '%s %s' % (self.tree_type,self.description) 

class AudioTree(object):
    def __init__(self,tree_type,path):
        self.adb = AudioTreeDB()
        self.aliases = []
        self.tree_type = tree_type

        self.path = normalized(os.path.realpath(path))
        path = normalized(path)
        if path != self.path and path not in self.aliases:
            self.aliases.append(path)

    def __getattr__(self,attr):
        if attr in ['tree_id','tid']:
            c = self.adb.cursor
            c.execute('SELECT id FROM tree WHERE path=?',(self.path,))
            tid = c.fetchone()[0]
            del(c)
            return tid
        raise AttributeError('No such AudioTree attribute: %s' % attr)

    def __repr__(self):
        return '%s %s' % (self.tree_type,self.path)

    def scan(self):
        """
        Update files for this tree in DB
        """

        def append_file(self,tree_id,mtime,path):
            """
            Append path to database
            """
            try:
                with self.adb.conn:
                    self.adb.conn.execute(
                        'INSERT INTO files (tree,mtime,path) VALUES (?,?,?)',
                        (tree_id,mtime,path,)
                    )
                    print 'NEW: %s' % f
            except sqlite3.IntegrityError,emsg:
                raise ValueError('%s: %s' % (path,emsg))

        def remove_file(self,tree_id,path):
            """
            Remove path from database
            """
            try:
                with self.adb.conn:
                    self.adb.conn.execute(
                        'DELETE FROM files WHERE tree=? and path=?',
                        (tree_id,path,)
                    )
                print 'DELETED: %s' % f
            except sqlite3.IntegrityError:
                raise ValueError('Error removing file %s' % path)

        tree_id = self.tree_id
        c = self.adb.cursor
        c.execute('SELECT path,mtime FROM files WHERE tree=?',(tree_id,))
        tree_songs = dict([(r[0],r[1]) for r in c.fetchall()])
        for (root,dirs,files) in os.walk(self.path):
            for f in files:
                f = normalized(os.path.realpath(os.path.join(root,f)))
                mtime = int(os.stat(f).st_mtime)
                try:
                    db_mtime = tree_songs[f]
                    if db_mtime != mtime:
                        print 'MODIFIED: %s' % f
                except KeyError:
                    append_file(self,tree_id,mtime,f)
                    tree_songs[f] = mtime
        for f in tree_songs.keys():
            if not os.path.isfile(f):
                remove_file(self,tree_id,f)

if __name__ == '__main__':
    import sys

    adb = AudioTreeDB()
    print adb.trees
    if len(sys.argv)==1: sys.exit(0)

    tree_type = sys.argv[1]
    for t in sys.argv[2:]:
        try:
            t = AudioTree(tree_type,t)
            adb.register(t)
        except ValueError,emsg:
            print 'Error: %s' % emsg 

    #for tt in adb.tree_types: print tt
    for t in adb.filter_type(tree_type): print t, t.aliases

