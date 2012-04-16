"""
Support for various codec programs in soundforest.
"""

import os,sqlite3

from subprocess import Popen,PIPE

from systematic.shell import normalized,CommandPathCache
from systematic.sqlite import SQLiteDatabase

"""
Default codec commands and parameters to register to database.
NOTE:
  Changing this dictionary after a codec is registered does NOT
  register a codec parameters, if the codec was already in DB!
"""
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
    'extensions': ['aac', 'm4a', 'mp4'],
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

  'caf': {
    'description': 'CoreAudio Format audio',
    'extensions':   ['caf'],
    'encoders': [], 'decoders': [],
  },

  # TODO - Raw audio, what should be decoder/encoder commands?
  'aif': {
      'description': 'AIFF audio',
      'extensions':   ['aif','aiff'],
      'encoders': [], 'decoders': [],
      },

  # TODO - Raw audio, what should be decoder/encoder commands?
  'wav': {
      'description': 'RIFF Wave Audio',
      'extensions':   ['wav'],
      'encoders': [], 'decoders': [],
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
                except CodecError:
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
            """
            Register a codec to database. Returns a Codec object, to add codec
            encoders or decoders use the methods in returned object.

            Codec name must unique in database. Description is just a textual
            description for the codec.
            """
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
            """
            Remove references to a registered codec from database
            """
            try:
                with self.conn:
                    self.conn.execute('DELETE FROM codec WHERE name=?',(name,))
            except sqlite3.IntegrityError:
                pass

        def registered_codecs(self):
            """
            Returns list of Codec objects matching codecs registered to database
            """
            with self.conn:
                try:
                    return [Codec(r[0],r[1],self) for r in self.conn.execute(
                        'SELECT name,description from codec'
                    )]
                except sqlite3.DataBaseError,emsg:
                    raise CodecError('Error querying codec database: %s' % emsg)

        def match(self,path):
            """
            Match given filename to codec extensions.
            Returns Codec matching file path or None if no match is found.
            """
            path = os.path.realpath(path)
            if os.path.isdir(path):
                self.log.debug('BUG: attempt to match codec extension to directory: %s' % path)
                return None
            ext = os.path.splitext(path)[1][1:].lower()
            if ext=='':
                return None
            for codec in self.registered_codecs():
                if ext in codec.extensions:
                    return codec
            return None

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
        """
        Registers given extension for this code to database.
        Extensions must be unique.
        """
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
        """
        Register a decoder command for this codec to database.
        Codec command must validate with CodecCommand.validate()
        """
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
        """
        Register encoder command for this codec to database.
        Codec command must validate with CodecCommand.validate()
        """
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
    Wrapper to validate and run codec commands from command line.

    A codec command specification must contain special arguments FILE and OUTFILE.
    These arguments are replaced with input and output file names when run() is
    called.
    """
    def __init__(self,command):
        self.command = command.split()

    def __repr__(self):
        return ' '.join(self.command)

    def validate(self):
        """
        Confirm the codec command contains exactly one FILE and OUTFILE argument
        """
        if self.command.count('FILE')!=1:
            raise CodecError('Command requires exactly one FILE')
        if self.command.count('OUTFILE')!=1:
            raise CodecError('Command requires exactly one OUTFILE')

    def is_available(self):
        """
        Check if the command is available on current path
        """
        return PATH_CACHE.which(self.command[0]) is None and True or False

    def parse_args(self,input_file,output_file):
        """
        Validates and returns the command to execute as list, replacing the
        input_file and output_file fields in command arguments.
        """

        if not self.is_available:
            raise CodecError('Command not found: %s' % self.command[0])
        try:
            self.validate()
        except CodecError,emsg:
            raise CodecError('Error validating codec command: %s' % emsg)

        # Make a copy of self.command, not reference!
        args = [x for x in self.command]
        args[args.index('FILE')] = input_file
        args[args.index('OUTFILE')] = output_file
        return args

    def run(self,input_file,output_file,stdout=None,stderr=None,shell=False):
        """
        Run codec command with given input and output files. Please note
        some command line tools may hang when executed like this!

        If stdout and stderr are not given, the command is executed without
        output. If stdout or stderr is given, the target must have a write()
        method where output lines are written.

        Returns command return code after execution.
        """

        args = self.parse_args(input_file,output_file)
        p = Popen(args,bufsize=POPEN_BUFSIZE,env=os.environ,
            stdin=PIPE,stdout=PIPE,stderr=PIPE,shell=shell
        )

        if stdout is None and stderr is None:
            p.communicate()
            rval = p.returncode
        else:
            rval = None
            while rval is None:
                while True:
                    l = p.stdout.readline()
                    if l=='': break
                    if stdout is not None:
                        stdout.write('%s\n'%l.rstrip())
                while True:
                    l = p.stderr.readline()
                    if l=='': break
                    if stderr is not None:
                        stderr.write('%s\n'%l.rstrip())
                rval = p.poll()

        if rval != 0:
            log.info('Error executing (returns %d): %s' % (rval,cmd))
        return rval

if __name__ == '__main__':
    import sys,logging
    logging.basicConfig(level=logging.DEBUG)
    cdb = CodecDB()

    for arg in sys.argv[1:]:
        print arg,cdb.match(arg)

    sys.exit(0)
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
        

