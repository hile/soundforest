# coding=utf-8
"""Soundforest database models

SQLAlchemy models for soundforest configuration and music tree databases

"""

import os
import hashlib
import base64
import json
import pytz
from datetime import datetime

from sqlite3 import Connection as SQLite3Connection
from sqlalchemy import create_engine, event
from sqlalchemy import Column, ForeignKey, Integer, Boolean, String, Date
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.types import TypeDecorator, Unicode
from sqlalchemy.ext.declarative import declarative_base

from soundforest import SoundforestError, SOUNDFOREST_USER_DIR
from soundforest.log import SoundforestLogger

logger = SoundforestLogger().default_stream

DEFAULT_DATABASE = os.path.join(SOUNDFOREST_USER_DIR, 'soundforest.sqlite')
Base = declarative_base()


class SafeUnicode(TypeDecorator):

    """SafeUnicode columns

    Safely coerce Python bytestrings to Unicode before passing off to the database.

    """

    impl = Unicode

    def process_bind_param(self, value, dialect):
        if isinstance(value, str):
            value = value.decode('utf-8')
        return value


class Base64Field(TypeDecorator):

    """Base64Field

    Column encoded as base64 to a string field in database

    """

    impl = String

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        return base64.encode(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        return base64.decode(value)


class BasePathNamedModel(object):
    """Base name comparable with name string"""

    def __cmp__(self, other):
        if isinstance(other, basestring):
            return cmp(self.path, other)
        return cmp(self, other)

    def __eq__(self, other):
        return self.__cmp__(other) == 0

    def __ne__(self, other):
        return self.__cmp__(other) != 0

    def __lt__(self, other):
        return self.__cmp__(other) < 0

    def __lte__(self, other):
        return self.__cmp__(other) <= 0

    def __gt__(self, other):
        return self.__cmp__(other) > 0

    def __gte__(self, other):
        return self.__cmp__(other) >= 0


class BaseNamedModel(object):
    """Base name comparable with name string"""

    def __cmp__(self, other):
        if isinstance(other, basestring):
            return cmp(self.name, other)
        return cmp(self, other)

    def __eq__(self, other):
        return self.__cmp__(other) == 0

    def __ne__(self, other):
        return self.__cmp__(other) != 0

    def __lt__(self, other):
        return self.__cmp__(other) < 0

    def __lte__(self, other):
        return self.__cmp__(other) <= 0

    def __gt__(self, other):
        return self.__cmp__(other) > 0

    def __gte__(self, other):
        return self.__cmp__(other) >= 0


class SettingModel(Base):

    """SettingModel

    Soundforest internal application preferences

    """

    __tablename__ = 'settings'

    id = Column(Integer, primary_key=True)
    key = Column(SafeUnicode, unique=True)
    value = Column(SafeUnicode)


class SyncTargetModel(Base, BaseNamedModel):

    """SyncTargetModel

    Library tree synchronization target entry

    """

    __tablename__ = 'sync_targets'

    id = Column(Integer, primary_key=True)
    name = Column(SafeUnicode, unique=True)
    type = Column(SafeUnicode)
    src = Column(SafeUnicode)
    dst = Column(SafeUnicode)
    flags = Column(SafeUnicode)
    defaults = Column(Boolean)

    def __repr__(self):
        return '{0} {1} from {2} to {3} (flags {4})'.format(
            self.name, self.type, self.src, self.dst, self.flags
        )

    def as_dict(self):
        return {
            'name': self.name,
            'type': self.type,
            'src':  self.src,
            'dst':  self.dst,
            'flags': self.flags,
            'defaults': self.defaults,
        }


class CodecModel(Base, BaseNamedModel):

    """CodecModel

    Audio format codecs

    """

    __tablename__ = 'codecs'

    id = Column(Integer, primary_key=True)
    name = Column(SafeUnicode)
    description = Column(SafeUnicode)

    def __repr__(self):
        return self.name

    def register_extension(self, session, extension):
        existing = session.query(ExtensionModel).filter(
            ExtensionModel.extension == extension
        ).first()
        if existing:
            raise SoundforestError('ExtensionModel already registered: {0}'.format(extension))

        session.add(ExtensionModel(codec=self, extension=extension))
        session.commit()

    def unregister_extension(self, session, extension):
        existing = session.query(ExtensionModel).filter(
            ExtensionModel.extension == extension
        ).first()
        if not existing:
            raise SoundforestError('ExtensionModel was not registered: {0}'.format(extension))

        session.delete(existing)
        session.commit()

    def register_decoder(self, session, command):
        existing = session.query(DecoderModel).filter(
            DecoderModel.codec == self,
            DecoderModel.command == command
        ).first()
        if existing:
            raise SoundforestError('DecoderModel already registered: {0}'.format(command))

        session.add(DecoderModel(codec=self, command=command))
        session.commit()

    def unregister_decoder(self, session, command):
        existing = session.query(DecoderModel).filter(
            DecoderModel.codec == self,
            DecoderModel.command == command
        ).first()
        if not existing:
            raise SoundforestError('DecoderModel was not registered: {0}'.format(command))

        session.delete(existing)
        session.commit()

    def register_encoder(self, session, command):
        existing = session.query(EncoderModel).filter(
            EncoderModel.codec == self,
            EncoderModel.command == command
        ).first()
        if existing:
            raise SoundforestError('EncoderModel already registered: {0}'.format(command))

        session.add(EncoderModel(codec=self, command=command))
        session.commit()

    def unregister_encoder(self, session, command):
        existing = session.query(EncoderModel).filter(
            EncoderModel.codec == self,
            EncoderModel.command == command
        ).first()
        if not existing:
            raise SoundforestError('EncoderModel was not registered: {0}'.format(command))

        session.delete(existing)
        session.commit()

    def register_tester(self, session, command):
        existing = session.query(TesterModel).filter(
            TesterModel.codec == self,
            TesterModel.command == command
        ).first()
        if existing:
            raise SoundforestError('File tester already registered: {0}'.format(command))

        session.add(TesterModel(codec=self, command=command))
        session.commit()

    def unregister_tester(self, session, command):
        existing = session.query(TesterModel).filter(
            TesterModel.codec == self,
            TesterModel.command == command
        ).first()
        if not existing:
            raise SoundforestError('File tester was not registered: {0}'.format(command))

        session.delete(existing)
        session.commit()


class ExtensionModel(Base):

    """ExtensionModel

    Filename extensions associated with audio format codecs

    """

    __tablename__ = 'extensions'

    id = Column(Integer, primary_key=True)
    extension = Column(SafeUnicode)
    codec_id = Column(Integer, ForeignKey('codecs.id'), nullable=False)
    codec = relationship('CodecModel',
        single_parent=False,
        backref=backref('extensions',
            order_by=extension,
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return self.extension


class TesterModel(Base):
    """TesterModel

    Command to test audio files with given codec

    """
    __tablename__ = 'formattester'

    id = Column(Integer, primary_key=True)
    priority = Column(Integer)
    command = Column(SafeUnicode)

    codec_id = Column(Integer, ForeignKey('codecs.id'), nullable=False)
    codec = relationship('CodecModel',
        single_parent=False,
        backref=backref('testers',
            order_by=command,
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return '{0} format tester: {1}'.format(self.codec.name, self.command)


class DecoderModel(Base):

    """DecoderModel

    Audio format codec decoder commands

    """

    __tablename__ = 'decoders'

    id = Column(Integer, primary_key=True)
    priority = Column(Integer)
    command = Column(SafeUnicode)
    codec_id = Column(Integer, ForeignKey('codecs.id'), nullable=False)
    codec = relationship('CodecModel',
        single_parent=False,
        backref=backref('decoders',
            order_by=priority,
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return '{0} decoder: {1}'.format(self.codec.name, self.command)


class EncoderModel(Base):

    """EncoderModel

    Audio format codec encoder commands

    """

    __tablename__ = 'encoders'

    id = Column(Integer, primary_key=True)
    priority = Column(Integer)
    command = Column(SafeUnicode)
    codec_id = Column(Integer, ForeignKey('codecs.id'), nullable=False)
    codec = relationship('CodecModel',
        single_parent=False,
        backref=backref('encoders',
            order_by=priority,
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return '{0} encoder: {1}'.format(self.codec.name, self.command)


class PlaylistTreeModel(Base, BaseNamedModel):

    """PlaylistTreeModel

    PlaylistTreeModel parent folders

    """

    __tablename__ = 'playlist_trees'

    id = Column(Integer, primary_key=True)
    name = Column(SafeUnicode)
    path = Column(SafeUnicode)

    def __repr__(self):
        return '{0}: {1}'.format(self.name, self.path)

    @property
    def exists(self):
        """Check if path exists

        Return true if registered path exists

        """
        return os.path.isdir(os.path.realpath(self.path))

    def update(self, session, source):
        """Read playlists to database from source

        Source must be iterable playlist object, for example
        soundforest.playlist.m3uPlaylistDirectory

        """
        for playlist in source:

            directory = os.path.realpath(playlist.directory)
            db_playlist = session.query(PlaylistModel).filter(
                PlaylistModel.parent == self,
                PlaylistModel.folder == directory,
                PlaylistModel.name == playlist.name,
                PlaylistModel.extension == playlist.extension
            ).first()

            if db_playlist is None:
                db_playlist = PlaylistModel(
                    parent=self,
                    folder=directory,
                    name=playlist.name,
                    extension=playlist.extension
                )
                session.add(db_playlist)

            for existing_track in db_playlist.tracks:
                session.delete(existing_track)

            try:
                playlist.read()
            except PlaylistError, emsg:
                logger.debug('Error reading playlist {0}: {1}'.format(playlist, emsg))
                continue

            tracks = []
            for index, path in enumerate(playlist):
                position = index+1
                tracks.append(PlaylistTrackModel(
                    playlist=db_playlist,
                    path=path,
                    position=position
                ))
            session.add_all(tracks)
            db_playlist.updated = datetime.now()
            session.commit()


class PlaylistModel(Base, BaseNamedModel):

    """PlaylistModel

    PlaylistModel file of audio tracks

    """

    __tablename__ = 'playlists'

    id = Column(Integer, primary_key=True)

    updated = Column(Date)
    folder = Column(SafeUnicode)
    name = Column(SafeUnicode)
    extension = Column(SafeUnicode)
    description = Column(SafeUnicode)

    parent_id = Column(Integer, ForeignKey('playlist_trees.id'), nullable=False)
    parent = relationship('PlaylistTreeModel',
        single_parent=False,
        backref=backref('playlists',
            order_by=[folder, name],
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return '{0}: {1:d} tracks'.format(os.sep.join([self.folder, self.name]), len(self.tracks))

    def __len__(self):
        return len(self.tracks)


class PlaylistTrackModel(Base, BasePathNamedModel):

    """PlaylistTrackModel

    Audio track in a playlist

    """

    __tablename__ = 'playlist_tracks'

    id = Column(Integer, primary_key=True)

    position = Column(Integer)
    path = Column(SafeUnicode)

    playlist_id = Column(Integer, ForeignKey('playlists.id'), nullable=False)
    playlist = relationship('PlaylistModel',
        single_parent=False,
        backref=backref('tracks',
            order_by=position,
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return '{0:d} {1}'.format(self.position, self.path)


class TreeTypeModel(Base, BaseNamedModel):

    """TreeTypeModel

    Audio file tree types (music, samples, loops etc.)

    """

    __tablename__ = 'treetypes'

    id = Column(Integer, primary_key=True)
    name = Column(SafeUnicode)
    description = Column(SafeUnicode)

    def __repr__(self):
        return self.name


class PrefixModel(Base, BasePathNamedModel):

    """PrefixModel

    Audio tree prefix paths

    """

    __tablename__ = 'prefixes'

    id = Column(Integer, primary_key=True)
    path = Column(SafeUnicode, unique=True)

    def __repr__(self):
        return self.path


class TreeModel(Base, BasePathNamedModel):

    """TreeModel

    Audio file tree

    """

    __tablename__ = 'trees'

    id = Column(Integer, primary_key=True)
    path = Column(SafeUnicode, unique=True)
    source = Column(SafeUnicode, default=u'filesystem')
    description = Column(SafeUnicode)

    type_id = Column(Integer, ForeignKey('treetypes.id'), nullable=True)
    type = relationship('TreeTypeModel',
        single_parent=True,
        backref=backref('trees',
            order_by=path,
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return self.path

    def album_count(self, session):
        return session.query(AlbumModel).filter(
            AlbumModel.tree == self
        ).count()

    def song_count(self, session):
        return session.query(TrackModel).filter(
            TrackModel.tree == self
        ).count()

    def tag_count(self, session):
        return session.query(TagModel)\
            .filter(TrackModel.tree == self)\
            .filter(TagModel.track_id == TrackModel.id)\
            .count()

    def match_tag(self, session, match):
        """Match database track tags

        Return tracks matching given tag value

        """
        return session.query(TrackModel)\
            .filter(TrackModel.tree == self)\
            .filter(TagModel.track_id == TrackModel.id)\
            .filter(TagModel.value.like('%{0}%'.format(match)))\
            .all()

    def filter_tracks(self, session, path):
        res = session.query(TrackModel).filter(TrackModel.tree == self)
        return res.filter(
            TrackModel.directory.like('%{0}%'.format(path)) |
            TrackModel.filename.like('%{0}%'.format(path))
        ).all()

    def to_json(self):
        """Return tree as JSON

        Return tree path, description albums and total counters as JSON

        """
        album_info = [{'id': a.id, 'path': a.directory} for a in self.albums]

        return json.dumps({
            'id': self.id,
            'path': self.path,
            'description': self.description,
            'albums': album_info,
            'total_albums': len(self.albums),
            'total_songs': len(self.songs),
        })


class AlbumModel(Base, BasePathNamedModel):

    """AlbumModel

    AlbumModel of music tracks in tree database.

    """

    __tablename__ = 'albums'

    id = Column(Integer, primary_key=True)

    directory = Column(SafeUnicode)
    mtime = Column(Integer)
    tree_id = Column(Integer, ForeignKey('trees.id'), nullable=True)
    tree = relationship('TreeModel',
        single_parent=False,
        backref=backref('albums',
            order_by=directory,
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return self.directory

    @property
    def path(self):
        return self.directory

    @property
    def relative_path(self):
        path = self.directory
        if self.tree and path[:len(self.tree.path)] == self.tree.path:
            path = path[len(self.tree.path):].lstrip(os.sep)
        return path

    @property
    def exists(self):
        return os.path.isdir(self.directory)

    @property
    def modified_isoformat(self, tz=None):
        if self.mtime is None:
            return None

        tval = datetime.fromtimestamp(self.mtime).replace(tzinfo=pytz.utc)

        if tz is not None:
            if isinstance(tz, basestring):
                tval = tval.astimezone(pytz.timezone(tz))
            else:
                tval = tval.astimezone(tz)

        return tval.isoformat()

    def to_json(self):
        """Return album as JSON

        Return album metadata + tracks IDs and filenames as JSON

        """
        track_info = [{'id': t.id, 'filename': t.filename} for t in self.tracks]
        return json.dumps({
            'id': self.id,
            'path': self.directory,
            'modified': self.modified_isoformat,
            'tracks': track_info
        })


class AlbumArtModel(Base):

    """AlbumArtModel

    AlbumArtModel files for music albums in tree database.

    """

    __tablename__ = 'albumarts'

    id = Column(Integer, primary_key=True)
    mtime = Column(Integer)
    albumart = Column(Base64Field)

    album_id = Column(Integer, ForeignKey('albums.id'), nullable=True)
    album = relationship('AlbumModel',
        single_parent=False,
        backref=backref('albumarts',
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return 'AlbumArtModel for {0}'.format(self.album.path)


class TrackModel(Base, BasePathNamedModel):

    """TrackModel

    Audio file. Optionally associated with a audio file tree

    """

    __tablename__ = 'tracks'

    id = Column(Integer, primary_key=True)

    directory = Column(SafeUnicode)
    filename = Column(SafeUnicode)
    extension = Column(SafeUnicode)
    checksum = Column(SafeUnicode)
    mtime = Column(Integer)
    deleted = Column(Boolean)

    tree_id = Column(Integer, ForeignKey('trees.id'), nullable=True)
    tree = relationship('TreeModel',
        single_parent=False,
        backref=backref('tracks',
            order_by=[directory, filename],
            cascade='all, delete, delete-orphan'
        )
    )
    album_id = Column(Integer, ForeignKey('albums.id'), nullable=True)
    album = relationship('AlbumModel',
        single_parent=False,
        backref=backref('tracks',
            order_by=[directory, filename],
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return os.sep.join([self.directory, self.filename])

    @property
    def path(self):
        return os.path.join(self.directory, self.filename)

    @property
    def relative_path(self):
        path = os.path.join(self.directory, self.filename)
        if self.tree and path[:len(self.tree.path)] == self.tree.path:
            path = path[len(self.tree.path):].lstrip(os.sep)
        return path

    @property
    def exists(self):
        return os.path.isfile(self.path)

    @property
    def modified_isoformat(self, tz=None):
        if self.mtime is None:
            return None

        tval = datetime.fromtimestamp(self.mtime).replace(tzinfo=pytz.utc)

        if tz is not None:
            if isinstance(tz, basestring):
                tval = tval.astimezone(pytz.timezone(tz))
            else:
                tval = tval.astimezone(tz)

        return tval.isoformat()

    def update(self, session, track):
        for tag in session.query(TagModel).filter(TagModel.track==self):
            session.delete(tag)
        for tag, value in track.tags.items():
            session.add(TagModel(track=self, tag=tag, value=value))
        session.commit()

    def to_json(self):
        return json.dumps({
            'id': self.id,
            'filename': self.path,
            'md5': self.checksum,
            'modified': self.modified_isoformat,
            'tags': dict((t.tag, t.value) for t in self.tags)
        })


class TagModel(Base):
    """TagModel

    Metadata tag for an audio file

    """

    __tablename__='tags'

    id=Column(Integer, primary_key = True)
    tag=Column(SafeUnicode)
    value=Column(SafeUnicode)
    base64_encoded=Column(Boolean)

    track_id=Column(Integer, ForeignKey('tracks.id'), nullable = False)
    track=relationship('TrackModel',
        single_parent = False,
        backref = backref('tags',
            order_by=tag,
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return '{0}={1}'.format(self.tag, self.value)


class SoundforestDB(object):

    """SoundforestDB

    Music database storing settings, synchronization data and music tree file metadata

    """

    def __init__(self, path=None, engine=None, debug=False):
        """
        By default, use sqlite databases in file given by path.
        """

        if engine is None:
            if path is None:
                path = DEFAULT_DATABASE

            config_dir = os.path.dirname(path)
            if not os.path.isdir(config_dir):
                try:
                    os.makedirs(config_dir)
                except OSError, (ecode, emsg):
                    raise SoundforestError('Error creating directory: {0}'.format(config_dir))

            engine = create_engine('sqlite:///{0}'.format(path), encoding='utf-8', echo=debug)

        event.listen(engine, 'connect', self._fk_pragma_on_connect)
        Base.metadata.create_all(engine)

        session_instance = sessionmaker(bind=engine)
        self.session = session_instance()

    def _fk_pragma_on_connect(self, connection, record):
        """Enable foreign keys for sqlite databases"""
        if isinstance(connection, SQLite3Connection):
            cursor = connection.cursor()
            cursor.execute('pragma foreign_keys=ON')
            cursor.close()

    def query(self, *args, **kwargs):
        """Wrapper to do a session query"""
        return self.session.query(*args, **kwargs)

    def rollback(self):
        """Wrapper to rolllback current session query"""
        return self.session.rollback()

    def commit(self):
        """Wrapper to commit current session query"""
        return self.session.commit()

    def as_dict(self, result):
        """Returns current query Base result as dictionary"""
        if not hasattr(result, '__table__'):
            raise ValueError('Not a sqlalchemy ORM result')
        return dict((k.name, getattr(result, k.name)) for k in result.__table__.columns)

    def add(self, items):
        """Add items in query session, committing changes"""
        if isinstance(items, list):
            self.session.add_all(items)
        else:
            self.session.add(items)

        self.session.commit()

    def delete(self, items):
        """Delete items in query session, committing changes"""
        if isinstance(items, list):
            for item in items:
                self.session.delete(item)
        else:
            self.session.delete(items)

        self.session.commit()

    @property
    def registered_sync_targets(self):
        return self.query(SyncTargetModel).all()

    @property
    def registered_codecs(self):
        return self.query(CodecModel).order_by(CodecModel.name).all()

    @property
    def registered_tree_types(self):
        return self.query(TreeTypeModel).order_by(TreeTypeModel.name).all()

    @property
    def registered_playlist_trees(self):
        """Return registered PlaylistTreeModel objects from database"""
        return self.query(PlaylistTreeModel).order_by(PlaylistTreeModel.name).all()

    @property
    def playlists(self):
        """Return registered PlaylistModel objects from database"""
        return self.query(PlaylistModel).all()

    @property
    def prefixes(self):
        """Return registered PrefixModel objects from database"""
        return self.query(PrefixModel).all()

    @property
    def trees(self):
        """Return registered TreeModel objects from database"""
        return self.query(TreeModel).all()

    @property
    def albums(self):
        """Return registered AlbumModel objects from database"""
        return self.query(AlbumModel).all()

    @property
    def tracks(self):
        """Return registered TrackModel objects from database"""
        return self.query(TrackModel).all()

    @property
    def registered_settings(self):
        """Return registered SettingModel objects from database"""
        return self.query(SettingModel).order_by(SettingModel.key).all()

    def update_setting(self, key, value):
        setting = self.query(SettingModel).filter(
            SettingModel.key == key
        ).first()

        if setting:
            existing.value = value
            self.update(existing)
        else:
            self.add(SettingModel(key=key, value=value))

    def get_setting(self, key):
        setting = self.query(SettingModel).filter(
            SettingModel.key == key
        ).first()
        if setting:
            return setting.value
        else:
            return None

    def register_sync_target(self, name, synctype, src, dst, flags=None, defaults=False):
        """Register a sync target"""

        existing = self.query(SyncTargetModel).filter(
            SyncTargetModel.name == name
        ).first()
        if existing:
            raise SoundforestError('Sync target was already registered: {0}'.format(name))

        target = SyncTargetModel(
            name=name,
            type=synctype,
            src=src,
            dst=dst,
            flags=flags,
            defaults=defaults
        )

        self.add(target)
        return target.as_dict()

    def unregister_sync_target(self, name):
        existing = self.query(SyncTargetModel).filter(SyncTargetModel.name == name).first()
        if not existing:
            raise SoundforestError('Sync target was not registered: {0}'.format(name))

        self.delete(existing)

    def register_codec(self, name, extensions, description='', decoders=[], encoders=[], testers=[]):
        """
        Register codec with given parameters
        """
        codec = CodecModel(name=name, description=description)

        extension_instances = []
        for ext in extensions:
            extension_instances.append(ExtensionModel(codec=codec, extension=ext))

        decoder_instances = []
        for priority, command in enumerate(decoders):
            decoder_instances.append(DecoderModel(codec=codec, priority=priority, command=command))

        encoder_instances = []
        for priority, command in enumerate(encoders):
            encoder_instances.append(EncoderModel(codec=codec, priority=priority, command=command))

        tester_instances = []
        for priority, command in enumerate(testers):
            tester_instances.append(TesterModel(codec=codec, priority=priority, command=command))

        self.add([codec] + extension_instances + decoder_instances + encoder_instances + tester_instances)
        return codec

    def register_tree_type(self, name, description=''):
        existing = self.query(TreeTypeModel).filter(
            TreeTypeModel.name == name
        ).first()
        if existing:
            raise SoundforestError('Tree type was already registered: {0}'.format(name))

        self.add(TreeTypeModel(name=name, description=description))

    def unregister_tree_type(self, name, description=''):
        existing = self.query(TreeTypeModel).filter(
            TreeTypeModel.name == name
        ).first()
        if not existing:
            raise SoundforestError('Tree type was not registered: {0}'.format(name))

        self.delete(existing)

    def register_playlist_tree(self, path, name='Playlists'):
        existing = self.query(PlaylistTreeModel).filter(
            PlaylistTreeModel.path == path
        ).first()
        if existing:
            raise SoundforestError('Playlist source is already registered: {0}'.format(path))

        self.add(PlaylistTreeModel(path=path, name=name))

    def unregister_playlist_tree(self, path):
        existing = self.query(PlaylistTreeModel).filter(
            PlaylistTreeModel.path == path
        ).first()
        if not existing:
            raise SoundforestError('Playlist source is not registered: {0}'.format(path))

        self.delete(existing)

    def register_prefix(self, path):
        """Register a prefix"""
        if isinstance(path, str):
            path = unicode(path, 'utf-8')

        existing = self.query(PrefixModel).filter(
            PrefixModel.path == path
        ).first()
        if existing:
            raise SoundforestError('Prefix was already registered: {0}'.format(path))

        self.add(PrefixModel(path=path))

    def unregister_prefix(self, path):
        """Unregister a prefix"""
        existing = self.query(PrefixModel).filter(
            PrefixModel.path == path
        ).first()
        if not existing:
            raise SoundforestError('Prefix was not registered: {0}'.format(path))

        self.delete(existing)

    def register_tree(self, path, description='', tree_type='songs'):
        """Register tree"""
        if isinstance(path, str):
            path = unicode(path, 'utf-8')

        existing = self.query(TreeModel).filter(
            TreeModel.path == path
        ).first()
        if existing:
            raise SoundforestError('Tree was already registered: {0}'.format(path))

        tt = self.get_tree_type(tree_type)
        self.add(TreeModel(path=path, description=description, type=tt))

    def unregister_tree(self, path, description=''):
        """Unregister tree"""
        existing = self.query(TreeModel).filter(
            TreeModel.path == path
        ).first()
        if not existing:
            raise SoundforestError('Tree was not registered: {0}'.format(path))

        self.delete(existing)

    def get_codec(self, name):
        """Return codec matching name"""
        return self.query(CodecModel).filter(
            CodecModel.name == name
        ).first()

    def get_tree_type(self, name):
        """Return tree type matching name"""
        return self.query(TreeTypeModel).filter(
            TreeTypeModel.name == name
        ).first()

    def get_tree(self, path, tree_type='songs'):
        """Return tree matching path"""
        return self.query(TreeModel).filter(
            TreeModel.path == path
        ).first()

    def get_album(self, path):
        """Return album matching path"""
        return self.query(AlbumModel).filter(
            AlbumModel.directory == path
        ).first()

    def get_track(self, path):
        """Return trach matching path"""
        return self.query(TrackModel).filter(
            TrackModel.directory == os.path.dirname(path),
            TrackModel.filename == os.path.basename(path),
        ).first()

    def get_playlist_tree(self, path):
        return self.query(PlaylistTreeModel).filter(
            PlaylistTreeModel.path == path
        ).first()

    def get_playlist(self, path):
        """Get playlist by path"""
        return self.query(PlaylistModel).filter(
            PlaylistModel.path == path
        ).first()
