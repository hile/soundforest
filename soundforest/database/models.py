"""
Module to detect and store known audio file library paths
"""

import os,logging,sqlite3,base64,time,hashlib

from systematic.shell import normalized,ScriptLogger
from systematic.filesystems import MountPoint,MountPoints

from soundforest.metadata import MetaData
from soundforest.commands import CodecCommand,CodecCommandError
from soundforest.database import DEFAULT_DATABASE_BACKEND,DATABASE_BACKENDS,SoundForestDBError

DATABASE_LOG = os.path.expanduser('~/.soundforest/db.log')

# Valid database fields to query for tree files
VALID_FILES_FIELDS = [
    'id','tree','directory','filename','mtime','shasum','deleted'
]

# Event types in filechanges table
FILE_ADDED = 1
FILE_DELETED = 2
FILE_MODIFIED = 3

VALID_FILE_EVENTS = [FILE_ADDED,FILE_DELETED,FILE_MODIFIED]

# Ignored special filenames causing problems
IGNORED_FILES = [ 'Icon\r']

DEFAULT_SOURCE = 'filesystem'

DEFAULT_TREE_TYPES = {
    'Songs':        'Complete song files',
    'Recordings':   'Live performance recordings',
    'Playlists':    'Playlist files',
    'Loops':        'Audio loops',
    'Samples':      'Audio samples',
}

class SoundForestDB(object):
    """
    Common class to access all soundforest database backends
    """
    def __init__(self,backend=DEFAULT_DATABASE_BACKEND,**backend_args):

        # If message_callback is given, use it for logging. Otherwise log
        # to standard db.log logfile.
        self.__init_logfile__()
        message_callback = backend_args.get('message_callback',None)
        if backend_args.has_key('message_callback'):
            del backend_args['message_callback']
        if message_callback:
            self.message = message_callback
        else:
            self.message = self.dblog.debug

        try:
            backend = DATABASE_BACKENDS[backend]
            module = '.'.join(backend.split('.')[:-1])
            cname = backend.split('.')[-1]
            m = __import__(module,globals(),fromlist=[cname])
            self.backend = getattr(m,cname)(**backend_args)
        except KeyError:
            raise SoundForestDBError('Unknown backend: %s' % backend)

        registered_types = [r[0] for r in self.backend.get_tree_types()]
        for ttype,description in DEFAULT_TREE_TYPES.items():
            if ttype in registered_types:
                continue
            self.backend.register_tree_type(ttype,description,ignore_duplicate=True)

    def __init_logfile__(self):
        """
        Initialize soundforest database event log
        """
        logdir = os.path.dirname(DATABASE_LOG)
        logfile = os.path.splitext(os.path.basename(DATABASE_LOG))[0]
        if not os.path.isdir(logdir):
            os.makedirs(logdir)
        self.logger = ScriptLogger('soundforest')
        self.logger.level = logging.DEBUG
        self.logger.file_handler(logfile,logdir)
        self.dblog = self.logger[logfile]

    def register_tree_type(self,ttype,description,ignore_duplicate=False):
        """
        Register audio tree type to database
        ttype       Name of tree type ('Music','Samples' etc.)
        description Description of the usage of the files

        Returns ID for newly created tree type
        """
        return self.backend.register_tree_type(
                ttype,description,ignore_duplicate
        )

    def register_tree(self,tree,ignore_duplicate=True):
        """
        Register a audio file tree to database
        """
        if not isinstance(tree,Tree):
            raise TypeError('Tree must be a Tree object')
        return self.backend.register_tree(tree,ignore_duplicate)

    def register_removable_media(self,media,ignore_duplicate=False):
        """
        Register removable media device by name and format
        """
        if not isinstance(media,RemovableMedia):
            raise TypeError('Removable media must be RemovableMedia instance')
        return self.backend.register_removable_media(media,ignore_duplicate)

    def register_playlist_source(self,name,path):
        """
        Register a playlist source (program etc)
        """
        return self.backend.register_playlist_source(name,path)

    def register_playlist(self,source_id,folder,name,description,updated):
        """
        Register a playlist in matching source_id
        """
        return self.backend.register_playlist(
            source_id,folder,name,description,updated
        )

    def register_codec(self,name,description=''):
        """
        Register a codec to database, return codec database ID.
        """
        return self.backend.register_codec(name,description)

    def register_codec_extension(self,codec_id,extension):
        """
        Register given file extension for given codec_id
        """
        self.backend.register_codec_extension(codec_id,extension)

    def register_codec_encoder(self,codec_id,command,priority=0):
        """
        Register given encoder command to this codec. This should
        be called as Codec.register_encoder, not directly.
        """
        self.backend.register_codec_encoder(codec_id,command,priority)

    def register_codec_decoder(self,codec_id,command,priority=0):
        """
        Register given decoder command to this codec. This should
        be called as Codec.register_decoder, not directly.
        """
        self.backend.register_codec_decoder(codec_id,command,priority)

    def unregister_tree_type(self,ttype):
        """
        Unregister audio tree type from database
        """
        return self.backend.unregister_tree_type(ttype)

    def unregister_tree(self,tree):
        """
        Remove audio file tree from database.
        Will remove fill references to the tree.
        """
        if type(tree) != Tree:
            raise SoundForestDBError('Tree must be Tree object')
        return self.backend.unregister_tree(tree)

    def unregister_removable_media(self,media):
        """
        Unregister given RemovableMedia object from database.
        """
        if not isinstance(media,RemovableMedia):
            raise SoundForestDBError('Media must be RemovableMedia object')
        return self.backend.unregister_removable_media(media)

    def unregister_playlist_source(self,name,path):
        """
        Unregister a playlist in matching source_id
        """
        return self.unregister_playlist_source(name,path)

    def unregister_playlist(self,playlist_id):
        """
        Register a playlist in matching source_id
        """
        return self.unregister_playlist(playlist_id)

    def unregister_codec(self,name):
        """
        Remove references to codec with given name from database
        """
        self.backend.unregister_codec(name)

    def unregister_codec_extension(self,codec_id,extension):
        """
        Remove references to codec with given name from database
        """
        self.backend.unregister_codec_extension(codec_id,extension)

    def unregister_codec_encoder(self,codec_id,command):
        """
        Unregister given codec encoder command from database.
        """
        return self.backend.unregister_codec_encoder(codec_id,command)

    def unregister_codec_decoder(self,codec_id,command):
        """
        Unregister given codec decoder command from database.
        """
        return self.backend.unregister_codec_decoder(codec_id,command)

    def get_tree_types(self):
        """
        Return audio tree types registered to database
        """
        return self.backend.get_tree_types()

    def get_trees(self):
        """
        Return list of all audio trees registered to database
        """
        trees = []
        for path,source in self.backend.get_trees():
            trees.append(self.get_tree(path,source))
        return trees

    def get_tree_aliases(self,source=DEFAULT_SOURCE):
        """
        Return filesystem aliases (links) to given tree and source.
        """
        return self.backend.get_tree_aliases(source)

    def get_tree(self,path,source=DEFAULT_SOURCE):
        """
        Return audio tree details matching parameters:
        path        Tree path
        source      Tree source type

        Returns a Tree object from this data.
        """
        path = normalized(path)
        (details,aliases) = self.backend.get_tree(path,source)
        return Tree(self,
            path=details['path'],
            source=details['source'],
            tree_type=details['type'],
            tree_id=details['id'],
            aliases=aliases,
        )

    def match_tree(self,path,source=DEFAULT_SOURCE):
        """
        Match given tree path to trees in database
        """
        path = normalized(path)
        try:
            (details,aliases) = self.backend.match_tree(path,source)
        except ValueError:
            try:
                (details,aliases) = self.backend.match_tree_aliases(path,source)
            except ValueError:
                return None
        return Tree(self,
            path=details['path'],
            source=details['source'],
            tree_type=details['type'],
            tree_id=details['id'],
            aliases=aliases,
        )

    def match_tree_extension(self,tree_id,path):
        """
        Return files matching given extension from audio tree
        """
        return self.backend.tree_match_extension(tree_id,extension)

    def match_tree_path(self,tree_id,path):
        """
        Return file paths matching given path from audio tree
        """
        return self.backend.tree_match_path(tree_id,path)

    def match_filename(self,tree_id,name):
        """
        Return file paths matching given name from audio tree
        """
        return self.backend.match_filename(tree_id,name)

    def cleanup_tree(self,tree_id):
        """
        Remove database entries for deleted files (with deleted flag)
        """
        return self.backend.cleanup_tree(tree_id)

    def tree_add_file(self,tree_id,directory,filename,mtime):
        """
        Append a file to given audio tree
        """
        return self.backend.tree_add_file(tree_id,directory,filename,mtime)

    def tree_remove_file(self,tree_id,directory,filename):
        """
        Remove given file instantly from database
        """
        return self.backend.tree_remove_file(tree_id,directory,filename)

    def update_file_shasum(self,file_id,shasum):
        """
        Update shasum for a file in audio tree
        """
        return self.backend.update_file_shasum(file_id,shasum)

    def update_file_mtime(self,file_id,mtime):
        """
        Update mtime for a file in audio tree
        """
        return self.backend.update_file_mtime(file_id,mtime)

    def set_file_deleted_flag(self,directory,filename,value=1):
        """
        Update the 'deleted' flag for given file in database
        """
        if value in [True,1]:
            value = 1
        elif value in [False,0]:
            value = 0
        else:
            raise SoundForestDBError('Unsupported deleted flag value: %s' % value)
        return self.backend.set_file_deleted_flag(directory,filename,value)

    def get_directories(self,tree_id):
        """
        Return registered directories from given tree
        """
        return self.backend.get_directories(tree_id)

    def get_files(self,tree_id,directory=None):
        """
        Return audio tree files matching tree ID
        """
        return self.backend.get_files(tree_id,directory)

    def get_file_details(self,tree_id,directory,filename):
        """
        Return all details for specific tree file as dictionary
        """
        return self.backend.get_file_details(tree_id,directory,filename)

    def file_events(self,tree_id,time_start=None):
        """
        Return file events from database for given tree
        """
        return self.backend.tree_file_events(tree_id,time_start)

    def create_file_event(self,tree_id,path,event):
        """
        Create a new filechanges event for given file path in given tree
        Event must be from one of FILE_ADDED, FILE_DELETED, FILE_MODIFIED
        """
        try:
            event = int(event)
            if event not in VALID_FILE_EVENTS:
                raise ValueError
        except ValueError:
            raise SoundForestDBError('Invalid file event: %s' % event)
        self.backend.create_file_event(tree_id,path,event)

    def get_playlist_sources(self):
        """
        Return all playlist sources
        """
        return [PlaylistSource(self,**details) 
            for details in self.backend.get_playlist_sources()
        ]

    def get_playlist_source_id(self,name,path):
        """
        Return playlist source ID matching name and path
        """
        return self.backend.get_playlist_source_id(name,path)

    def get_playlist_source_playlists(self,source_id):
        """
        Return playlists matching given playlist source ID
        """
        return [Playlist(self,source_id,**details)
                for details in \
                self.backend.get_playlist_source_playlists(source_id)
        ]

    def get_playlist_id(self,source_id,name,path):
        """
        Return playlist ID matching source, name and path
        """
        return self.backend.get_playlist_id(source_id,name,path)

    def get_playlist(self,source_id,folder,name):
        """
        Return playlist matching folder and name
        """
        details = self.backend.get_playlist(source_id,folder,name)
        if details is not None:
            details['source_id'] = details['source']
            details['playlist_id'] = details['id']
            del details['source']
            del details['id']
            return Playlist(self,**details)
        return None

    def get_playlist_tracks(self,playlist_id):
        """
        Return playlist track paths for given playlist ID
        """
        return self.backend.get_playlist_tracks(playlist_id)

    def clear_playlist(self,playlist_id):
        """
        Clear tracks from given playlist
        """
        self.backend.clear_playlist(playlist_id)

    def replace_playlist_tracks(self,playlist_id,tracks):
        """
        Replace playlist tracks with given track paths
        """
        if not isinstance(tracks,list):
            raise TypeError('tracks must be list instance')
        return self.backend.replace_playlist_tracks(playlist_id,tracks)

    def get_registered_codecs(self):
        """
        Returns list of registered codecs from database
        """
        codecs = []
        for codec_id,name,description in self.backend.get_registered_codecs():
            codecs.append(Codec(self,name,description,codec_id))
        return codecs

    def get_codec(self,name):
        """
        Return Codec instance for given codec name.
        Raises SoundForestDBError if codec name is not configured
        """
        name,codec_id,description = self.backend.get_codec(name)
        return Codec(self,name,description,codec_id)

    def get_codec_extensions(self,codec_id):
        """
        Return extensions associated to given codec ID.
        """
        return self.backend.get_codec_extensions(codec_id)

    def get_codec_encoders(self,codec_id):
        """
        Return encoder command available for given codec_id
        """
        return [CodecCommand(encoder)
            for encoder in self.backend.get_codec_encoders(codec_id)
        ]

    def get_codec_decoders(self,codec_id):
        """
        Return decoder command available for given codec_id
        """
        return [CodecCommand(decoder)
            for decoder in self.backend.get_codec_decoders(codec_id)
        ]

    def match_codec(self,path):
        """
        Match given filename to codec extensions.
        Returns Codec matching file path or None if no match is found.
        """
        path = os.path.realpath(path)
        if os.path.isdir(path):
            return None
        ext = os.path.splitext(path)[1][1:].lower()
        if ext=='':
            return None
        for codec in self.get_registered_codecs():
            if ext in codec.extensions:
                return codec
        return None

    def set_labels(self,labels,source=DEFAULT_SOURCE):
        """
        Initialize default tag labels
        """
        self.backend.set_labels(self,labels,source)

    def get_labels(self,source=DEFAULT_SOURCE):
        """
        Return all defined tag labels in correct order
        """
        return self.backend.get_labels(source)

    def set_label(self,tag,position,label,description,source=DEFAULT_SOURCE):
        """
        Set a new tab label 
        """
        return self.backend.set_label(self,tag,position,label,description,source)

    def get_label(self,tag,source=DEFAULT_SOURCE):
        """
        Return info for a tag label
        """
        return self.backend.get_label(tag)

    def set_tags(self,path,tags,mtime=None,source=DEFAULT_SOURCE):
        """
        Set file tags file matching given path  
        """
        self.backend.set_tags(path,tags,mtime)

    def get_tags(self,path,source=DEFAULT_SOURCE):
        """
        get file tags file matching given path  
        """
        return self.backend.get_tags(path,source)

class Codec(object):
    """
    Class representing one codec from database.
    """
    def __init__(self,db,name,description=None,codec_id=None):
        self.message = db.message

        if not isinstance(db,SoundForestDB):
            raise SoundForestDBError('Not an instance of SoundForestDB')
        self.db = db
        self.name = name
        self.description = description
        if codec_id is None:
            codec_id = db.register_codec(name,description)
        self.codec_id = codec_id

    def __repr__(self):
        return ': '.join([self.name,self.description])

    def __getattr__(self,attr):
        if attr == 'extensions':
            return self.db.get_codec_extensions(self.codec_id)
        if attr == 'best_encoder':
            try:
                return filter(lambda x: x.is_available(), self.encoders)[0]
            except IndexError:
                raise SoundForestDBError('No encoders available')
        if attr == 'best_decoder':
            try:
                return filter(lambda x: x.is_available(), self.decoders)[0]
            except IndexError:
                raise SoundForestDBError('No decoders available')
        if attr == 'encoders':
            return self.db.get_codec_encoders(self.codec_id)
        if attr == 'decoders':
            return self.db.get_codec_decoders(self.codec_id)
        raise AttributeError('No such Codec attribute: %s' % attr)

    def register_extension(self,extension):
        """
        Registers given extension for this code to database.
        Extensions must be unique.
        """
        extension = extension.lstrip('.')
        try:
            self.db.register_codec_extension(self.codec_id,extension)
        except sqlite3.IntegrityError,emsg:
            self.message('Error adding extension %s: %s' % (extension,emsg))

    def register_decoder(self,command,priority=0):
        """
        Register a decoder command for this codec to database.
        Codec command must validate with CodecCommand.validate()
        """
        try:
            cmd = CodecCommand(command)
            cmd.validate()
            self.db.register_codec_decoder(self.codec_id,command,priority)
        except sqlite3.IntegrityError,emsg:
            raise SoundForestDBError(
                'Error registering decoder: %s: %s' % (command,emsg)
            )
        except ValueError,emsg:
            raise SoundForestDBError(
                'Error registering decoder: %s: %s' % (command,emsg)
            )

    def register_encoder(self,command,priority=0):
        """
        Register encoder command for this codec to database.
        Codec command must validate with CodecCommand.validate()
        """
        try:
            cmd = CodecCommand(command)
            cmd.validate()
            self.db.register_codec_encoder(self.codec_id,command,priority)
        except sqlite3.IntegrityError,emsg:
            raise SoundForestDBError(
                'Error registering encoder: %s: %s' % (command,emsg)
            )
        except ValueError,emsg:
            raise SoundForestDBError(
                'Error registering encoder: %s: %s' % (command,emsg)
            )

class Tree(object):
    """
    Representation of one audio tree object in database

    Attributes:
    db          Instance of SoundForestDB()
    path        Normalized unicode real path to tree root
    tree_type   Tree type (reference to treetypes table)
    source      Source reference, by default DEFAULT_SOURCE
    aliases     Symlink paths for this tree
    """
    def __init__(self,db,path,source=DEFAULT_SOURCE,tree_type='Songs',tree_id=None,aliases=None):

        self.message = db.message
        self.metadata = MetaData()
        if not isinstance(db,SoundForestDB):
            raise SoundForestDBError('db not an instance of SoundForestDB')

        self.__next = None
        self.__iterfiles = None
        if aliases is None:
            aliases = []
        self.aliases = aliases

        self.db = db
        self.source = source
        self.tree_type = tree_type

        self.path = normalized(os.path.realpath(path))
        path = normalized(path)
        if path != self.path and path not in self.aliases:
            self.aliases.append(path)

        if tree_id is None:
            tree_id = self.db.register_tree(self,ignore_duplicate=True)
        self.id = tree_id

    def __getattr__(self,attr):
        """
        Optional attributes:

        id              Tree database ID
        is_available    Boolean indicating if this tree is readable (mounted)
        files           Returns list of relative paths for tree files
        """
        if attr == 'is_available':
            return os.access(self.path,os.X_OK)

        if attr == 'directories':
            return self.db.get_tree_directories(self.id)

        if attr == 'files':
            return self.db.get_tree_files(self.id)

        raise AttributeError('No such Tree attribute: %s' % attr)

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
        Iterate tree files, return TreeFile items for paths in tree
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

    def update_checksums(self,force_update=False):
        """
        Update SHA1 checksum for files in tree with TreeFile.update_checksum()
        If force_update is True, all file checksums are updated.
        """
        self.message('Updating file checksums: %s' % self.path)
        for entry in self:
            entry.update_checksum(force_update)
        self.message('Finished updating file checksums: %s' % self.path)

    def update(self,update_checksums=False):
        """
        Update files for this tree in DB, adding filechanges events.
        Returns a dictionary containing paths to added, deleted and modified
        files in library.
        """
        if not self.is_available:
            self.message('Tree not available, skipping update: %s' % self.path)
            return

        self.message('Updating tree: %s' % self.path)

        changes = {'added':[],'deleted':[],'modified':[]}

        db_songs = {}
        db_file_details = self.db.get_tree_fields(
            self.id,['id','directory','filename','mtime','deleted']
        )
        for r in db_file_details:
            db_path = os.path.join(r['directory'],r['filename'])
            db_songs[db_path] = r
        db_song_paths = db_songs.keys()

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
                try:
                    mtime = long(os.stat(f).st_mtime)
                except OSError,(ecode,emsg):
                    self.message('ERROR checking %s: %s' % (f,emsg))
                    continue

                if db_path in db_song_paths:
                    db_info = db_songs[db_path]
                    if db_info['mtime'] != mtime:
                        self.db.update_tree_file_mtime(db_info['id'],mtime)
                        changes['modified'].append(db_path)
                        self.db.create_file_event(self.id,db_path,FILE_MODIFIED)
                        if update_checksums:
                            TreeFile(self,db_path).update_checksum(force_update=True)
                        self.message('Modified: %s' % db_path)

                    if db_info['deleted']:
                        self.db.tree_file_deleted_flag(
                            os.path.dirname(db_path),
                            os.path.basename(db_path),
                            False
                        )
                        is_modified = True
                        if update_checksums:
                            TreeFile(self,db_path).update_checksum(force_update=True)
                        self.message('Restored: %s' % db_path)
                else:
                    self.db.tree_append_file(
                        self.id,
                        os.path.dirname(db_path),
                        os.path.basename(db_path),
                        mtime
                    )
                    is_modified = True
                    self.db.create_file_event(self.id,db_path,FILE_ADDED)
                    db_songs[db_path] = {'mtime': mtime, 'deleted': False}
                    changes['added'].append(db_path)
                    if update_checksums:
                        TreeFile(self,db_path).update_checksum(force_update=True)
                    else:
                        self.message('Added: %s' % db_path)

        for db_path,flags in db_songs.items():
            if flags['deleted']:
                continue
            f = os.path.join(self.path,db_path)
            if not os.path.isfile(f):
                self.db.tree_file_deleted_flag(
                    os.path.dirname(db_path),
                    os.path.basename(db_path),
                    True 
                )
                self.db.create_file_event(self.id,db_path,FILE_DELETED)
                changes['deleted'].append(db_path)
                self.message('Deleted: %s' % db_path)

        self.message('Finished updating tree: %s' % self.path)

        return changes

    def remove_deleted(self):
        """
        Remove deleted entries for tree from database
        """
        self.message('Cleaning up deleted files: %s' % self.path)
        self.db.cleanup_tree(self.id) 

    def match(self,name):
        """
        Return files matching given name as list of TreeFile entries
        """
        return [TreeFile(self,os.path.join(self.path,x)) for x in self.db.tree_lookup_name(self.id,name)]

    def match_path(self,path):
        """
        Return files matching given path as list of TreeFile entries
        """
        return [TreeFile(self,os.path.join(self.path,x)) for x in self.db.tree_match_path(self.id,path)]

    def match_extension(self,extension):
        """
        Return files from tree matching given extension
        """
        return [TreeFile(self,os.path.join(self.path,x)) for x in self.db.tree_match_extension(self.id,extension)]

    def relative_path(self,path):
        """
        Return relative path for given target.

        Raises ValueError if path not under this tree.
        """
        path = normalized(os.path.realpath(path))
        mpath = path.strip('/').split(os.sep)
        tpath = self.path.strip('/').split(os.sep)
        if mpath[:len(tpath)]!=tpath:
            raise ValueError('Not in tree %s: %s' % (self.path,path))
        return os.sep.join(mpath[len(tpath):])

class TreeFile(object):
    """
    Class for one file in given sound forest tree.

    Note the file format is not processed yet, this file can be jpg,
    pdf or anything else when initialized.
    """
    def __init__(self,tree,path):
        
        self.message = tree.message
        if not isinstance(tree.db,SoundForestDB):
            raise SoundForestDBError('Not an instance of SoundForestDB')
        self.db = tree.db
        self.tree = tree
        self.__filetype = None
        self.__fileformat = None
        self.__tags = None
        self.__cached_attrs  = {}

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
        if attr == 'tags':
            if self.__tags is None:
                self.__tags = TreeFileTags(self.db,self)
            return self.__tags
        raise AttributeError('No such TreeFile attribute: %s' % attr)

    def __match_fileformat(self):
        """
        Match file format to registered metadata and codecs
        """
        m = self.db.metadata.match(self.realpath)
        if m:
            self.__filetype = 'metadata'
            self.__fileformat = m
            return
        m = self.db.codec_match(self.realpath)
        if m:
            self.__filetype = 'audio'
            self.__fileformat = m
            return
        else:
            self.__filetype = 'unknown'
            self.__fileformat = None

    def __update_cached_attrs(self):
        self.__cached_attrs.update(self.db.get_tree_file_details(
            self.tree.id,self.directory,self.filename
        ))

    def __repr__(self):
        return '%s%s' % (self.path,self.deleted==1 and ' (DELETED)' or '')

    def update_checksum(self,force_update=False):
        """
        Update SHA1 checksum stored for file into database
        """
        if not os.path.isfile(self.realpath):
            self.db.log.debug('Not updating sha1, file missing: %s' % self.realpath)
            return

        mtime = long(os.stat(self.realpath).st_mtime)
        if self.mtime==mtime and self.shasum is not None and not force_update:
            # Do not update existing mtime if file is not modified
            return

        self.db.log.debug('Updating SHA1 for %s' % self.realpath.encode('utf-8'))
        shasum = hashlib.sha1()
        shasum.update(open(self.realpath,'r').read())
        self.db.update_tree_file_checksum(self.id,shasum.hexdigest())
        return shasum.hexdigest()

class Base64Tag(object):
    """
    Wrapper to store a base64 formatted value to tags cleanly.
    """
    def __init__(self,value):
        if not isinstance(value,basestring):
            raise ValueError('base64_tag value must be a string')
        try:
            base64.b64decode(value)
        except TypeError:
            raise ValueError('base64_tag value is not valid base64 string')
        self.value = unicode(value)

    def __repr__(self):
        return self.value

    def __unicode__(self):
        return self.value

    def decode(self):
        """
        Return decoded base64 tag value as binary
        """
        return base64.b64decode(str(self.value))

class TreeFileTags(dict):
    """
    Tags stored to database for given file
    """
    def __init__(self,db,tree_file):
        dict.__init__(self)
        if not isinstance(db,SoundForestDB):
            raise SoundForestDBError('Not an instance of SoundForestDB')
        self.db = db
        self.path = tree_file.path
        self.source = tree_file.tree.source

        for entry in self.db.get_tags(tree_file.path,tree_file.tree.source):
            if entry['base64']:
                self[entry['tag']] = Base64Tag(entry['value'])
            else:
                self[entry['tag']] = entry['value']

    def update_tags(self,tags):
        """
        Update given tags for this file to database
        """
        if not isinstance(tags,dict):
            raise ValueError('Tags argument must be dictionary')
        data = []
        for name,value in tags.items():
            try:
                data.append({
                    'tag': name,
                    'value': value,
                    'base64': isinstance(value,Base64Tag)
                })
            except KeyError:
                raise ValueErro('Invalid tags to set')
        self.db.set_tags(self.path,data,self.source)
        self.clear()
        for entry in self.db.get_tags(self.path,self.source):
            if entry['base64']:
                value = Base64Tag(entry['value'])
            else:
                value = entry['value']
            self[entry['tag']] = value

class PlaylistSource(object):
    """
    Database entry for playlist data sources (program,path)
    """
    def __init__(self,db,name,path,source_id=None):
        if not isinstance(db,SoundForestDB):
            raise SoundForestDBError('Not an instance of SoundForestDB')
        self.message = db.message
        self.db = db
        self.name = name
        self.path = path

        if source_id is None:
            self.source_id = db.get_playlist_source_id(name,path)
        self.source_id = source_id

    def __getattr__(self,attr):
        if attr == 'playlists':
            return self.db.get_playlist_source_playlists(self.source_id)
        raise AttributeError('No such PlaylistSource attribute: %s' % attr)

    def __repr__(self):
        return 'PlaylistSource: %s %s' % (self.name,self.path)

    def add_playlist(self,path,description=None):
        """
        Add a playlist to database
        path        Playlist 'path', split to folder and name
        description Optional playlist description
        """
        folder = os.path.dirname(path)
        name = os.path.basename(path)
        timestamp = int(time.mktime(time.localtime()))
        return self.db.register_playlist(self.source_id,folder,name,description,timestamp)

    def remove_playlist(self,playlist):
        """
        Remove given playlist from database
        source      PlaylistSource object
        playlist    DBPlaylist instance
        """
        folder = os.path.dirname(path)
        name = os.path.basename(path)
        self.db.unregister_playlist(self.source_id,folder,name)

class Playlist(list):
    """
    Database playlist from one playlist source
    source      PlaylistSource object
    path        Playlist 'path' (folder,name)

    Please note path is not intended to be filesystem path, but
    path to the playlist in hierarchical playlist trees. However,
    we DO use os.sep as path separator.
    """
    def __init__(self,db,source_id,folder,name,description=None,updated=0,playlist_id=None):
        if not isinstance(db,SoundForestDB):
            raise SoundForestDBError('Not an instance of SoundForestDB')
        self.message = db.message
        self.db = db
        self.folder = folder
        self.name = name
        self.path = os.path.join(folder,name)

        if playlist_id is None:
            playlist_id = db.get_playlist_id(source_id,name,path)
        self.source_id = source_id
        self.playlist_id = playlist_id
        self.extend(self.db.get_playlist_tracks(self.playlist_id))

    def __repr__(self):
        return '%5d songs %s' % (len(self),self.path)

    def update(self,entries):
        """
        Update playlist entries from list of paths, adding entries to this object
        """
        self.__delslice__(0,len(self))
        self.append(entries)
        self.db.replace_playlist_tracks(self.playlist_id,entries)

class RemovableMedia(object):
    """
    Removable media device accessed by mountpoint name
    """
    def __init__(self,db,name,format,db_id=None):
        self.db = db
        self.name = name
        self.format = format
        if db_id is None:
            db_id = self.db.register_removable_media(self)
        self.id = db_id

    def __getattr__(self,attr):
        if attr == 'volume':
            mp = MountPoints()
            matches = filter(lambda x: x.name==self.name, mp)
            if len(matches)>1:
                raise AudioTreeError('Multiple removable devices with name %s' % self.name)
            elif matches:
                return matches[0]
            else:
                raise AttributeError('Removable media volume not available: %s' % self.name)
        if attr == 'is_available':
            try:
                return isinstance(self.volume,MountPoint)
            except AttributeError:
                return False
        raise AttributeError('No such RemovableMedia attribute: %s' % attr)

class RemovableMediaFile(object):
    """
    Database info for one file on a removable media device
    """
    def __init__(self,media,source_track):
        if not isinstance(media,RemovableMedia):
            raise TypeError('Media must be instance of RemovableMedia')
        if not isinstance(source_track,TreeFile):
            raise TypeError('Source track must be instance of TreeFile')
        self.media = media
        self.source = source_track
        self.path = '%s.%s' % (
            os.path.splitext(self.source.path)[0],
            self.media.format,
        )

    def __repr__(self):
        return '%s on removable media %s from %s' % (
            self.path,self.media.name,
            self.source.realpath
        )

