"""
Support for various codec programs in soundforest.
"""

import os,sqlite3

from systematic.shell import normalized,CommandPathCache
from systematic.sqlite import SQLiteDatabase

DEFAULT_CODECS = {

  'mp3': {
    'description': 'MPEG-1 or MPEG-2 Audio Layer III',
    'extensions':   ['mp3'],
    'encoders': [
        'lame --quiet -b 320 --vbr-new -ms --replaygain-accurate FILE OUTFILE',
    ],
    'decoders': [
        'lame --quiet --decode FILE OUTFILE',
    ],
  },

  'aac': {  
    'description': 'Advanced Audio Coding', 
    'extensions': ['aac','m4a'], 
    'encoders': [
        'neroAacEnc -if FILE -of OUTFILE -br 256000 -2pass',
        'afconvert -b 256000 -v -f m4af -d aac FILE OUTFILE',
    ], 
    'decoders': [
        'neroAacDec -if OUTFILE -of FILE',
        'faad -q -o OUTFILE FILE -b1',
    ],
  },

  'vorbis': {
    'description': 'Ogg Vorbis',
    'extensions': ['vorbis','ogg'], 
    'encoders': [
        'oggenc --quiet -q 7 -o OUTFILE FILE',
    ],
    'decoders': [
        'oggdec --quiet -o OUTFILE FILE',
    ],
  },

  'flac': {
    'description': 'Free Lossless Audio Codec',
    'extensions': ['flac'], 
    'encoders': [
        'flac -f --silent --verify --replay-gain QUALITY -o OUTFILE FILE',
    ],
    'decoders': [
        'flac -f --silent --decode -o OUTFILE FILE',
    ],
  },

  'wavpack': {
    'description': 'WavPack Lossless Audio Codec',
    'extensions': ['wv','wavpack'], 
    'encoders': [ 'wavpack -yhx FILE -o OUTFILE', ],
    'decoders': [ 'wvunpack -yq FILE -o OUTFILE', ],
  },

}

DB_PATH = os.path.join(os.getenv('HOME'),'.soundforest','codecs.sqlite')
DB_TABLES = [
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
""",
]

PATH_CACHE = CommandPathCache()
PATH_CACHE.update()

class CodecError(Exception):
    def __str__(self):
        return self.args[0]

class CodecDB(object):
    __instance = None
    def __init__(self,db_path=DB_PATH):
        if CodecDB.__instance is None:
            CodecDB.__instance = CodecDB.CodecDBInstance(db_path)
        self.__dict__['_CodecDB.__instance'] = CodecDB.__instance

    def __getattr__(self,attr):
        return getattr(self.__instance,attr)

    def __setattr__(self,attr,value):
        return setattr(self.__instance,attr,value)

    class CodecDBInstance(SQLiteDatabase):
        def __init__(self,db_path=DB_PATH):
            SQLiteDatabase.__init__(self,db_path,tables_sql=DB_TABLES)

            for name,config in DEFAULT_CODECS.items():
                try:
                    self.get_codec(name)
                    # Already configured, skip
                except ValueError:
                    codec = self.register_codec(name,config['description'])
                    for ext in config['extensions']:
                        codec.register_extension(ext)
                    for decoder in config['decoders']:
                        codec.register_decoder(decoder)
                    for encoder in config['encoders']:
                        codec.register_encoder(encoder)

        def get_codec(self,name):
            c = self.cursor
            c.execute('SELECT name,description FROM codec where name=?',(name,))
            result = c.fetchone()
            if result is None:
                raise CodecError('Codec not configured: %s' % name)
            return Codec(result[0],result[1],self)

        def register_codec(self,name,description=''):
            try:
                with self.conn:
                    self.conn.execute(
                        'INSERT INTO codec (name,description) VALUES (?,?)',
                        (name,description,)
                    )
            except sqlite3.IntegrityError:
                self.log.debug('ERROR: codec already registered: %s' % name)
            return Codec(name,description,self)

        def unregister_codec(self,name):
            try:
                with self.conn:
                    self.conn.execute('DELETE FROM codec WHERE name=?',(name,))
            except sqlite3.IntegrityError:
                pass

        def registered_codecs(self):
            with self.conn:
                try:
                    return [Codec(r[0],r[1],self) for r in self.conn.execute(
                        'SELECT name,description from codec'
                    )]
                except sqlite3.DataBaseError,emsg:
                    raise CodecError('Error querying codec database: %s' % emsg)

class Codec(object):
    """
    Instance representing one codec from database
    """
    def __init__(self,name,description,codecdb=None):
        self.cdb = codecdb is not None and codecdb or CodecDB()
        self.name = name
        self.description = description

    def __repr__(self):
        return ': '.join([self.name,self.description])

    def __getattr__(self,attr):
        if attr in ['codec_id','cid']:
            c = self.cdb.cursor
            c.execute('SELECT id FROM codec WHERE name=?',(self.name,))
            return c.fetchone()[0]
        if attr == 'extensions':
            return [r[0] for r in self.cdb.conn.execute(
                'SELECT extension FROM extensions WHERE codec=?',(self.cid,)
            )]
        if attr == 'best_encoder':
            try:
                return filter(lambda x: x.is_available(), self.encoders)[0]
            except IndexError:
                raise CodecError('No encoders available')
        if attr == 'best_decoder':
            try:
                return filter(lambda x: x.is_available(), self.decoders)[0]
            except IndexError:
                raise CodecError('No decoders available')

        if attr == 'encoders':
            return [CodecCommand(r[0]) for r in self.cdb.conn.execute(
                'SELECT command FROM encoder WHERE codec=? ORDER BY priority DESC',
                (self.cid,)
            )]
        if attr == 'decoders':
            return [CodecCommand(r[0]) for r in self.cdb.conn.execute(
                'SELECT command FROM decoder WHERE codec=? ORDER BY priority DESC',
                (self.cid,)
            )]
        raise AttributeError('No such Codec attribute: %s' % attr)

    def register_extension(self,extension):
        extension = extension.lstrip('.')
        try:
            with self.cdb.conn:
                self.cdb.conn.execute(
                    'INSERT INTO extensions (codec,extension) VALUES (?,?)',
                    (self.cid,extension,)
                )
        except sqlite3.IntegrityError,emsg:
            self.log.debug(emsg)

    def register_decoder(self,command,priority=0):
        try:
            cmd = CodecCommand(command)
            cmd.validate()
            with self.cdb.conn:
                self.cdb.conn.execute(
                    'INSERT INTO decoder (codec,command,priority) VALUES (?,?,?)',
                    (self.cid,command,priority)
                )
        except ValueError,emsg:
            raise CodecError('Error registering decoder: %s: %s' % (
                command,emsg
            ))
         

    def register_encoder(self,command,priority=0):
        try:
            cmd = CodecCommand(command)
            cmd.validate()
            with self.cdb.conn:
                self.cdb.conn.execute(
                    'INSERT INTO encoder (codec,command,priority) VALUES (?,?,?)',
                    (self.cid,command,priority)
                )
        except ValueError,emsg:
            raise CodecError('Error registering encoder: %s: %s' % (
                command,emsg
            ))

class CodecCommand(object):
    """
    Wrapper to validate and run codec commands
    """
    def __init__(self,command):
        self.command = command.split()
    
    def validate(self):
        if self.command.count('FILE')!=1:
            raise CodecError('Command requires exactly one FILE')
        if self.command.count('OUTFILE')!=1:
            raise CodecError('Command requires exactly one OUTFILE')

    def is_available(self):
        return PATH_CACHE.which(self.command[0]) is None and True or False

    def __repr__(self):
        return ' '.join(self.command)

if __name__ == '__main__':
    cdb = CodecDB()
    for name in DEFAULT_CODECS.keys():
        codec = cdb.get_codec(name)
        print '%s %s (%s)' % (
            codec.name,codec.description,','.join(codec.extensions)
        )
        print ' BE', codec.best_encoder
        for encoder in codec.encoders:
            print ' E  %s' % encoder
        print ' BD', codec.best_decoder
        for decoder in codec.decoders:
            print ' D  %s' % decoder
        

