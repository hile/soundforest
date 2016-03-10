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
from sqlalchemy import create_engine, event, \
                       Column, ForeignKey, Integer, Boolean, String, Date, \
                       UniqueConstraint, Index
from sqlalchemy.engine import reflection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.types import TypeDecorator, Unicode

from soundforest import SoundforestError, SOUNDFOREST_USER_DIR
from soundforest.log import SoundforestLogger
from soundforest.playlist import m3uPlaylist, m3uPlaylistDirectory

logger = SoundforestLogger().default_stream

DEFAULT_DATABASE = os.path.join(SOUNDFOREST_USER_DIR, 'soundforest.sqlite')

Base = declarative_base()

class SafeUnicode(TypeDecorator):
    """SafeUnicode columns

    Safely coerce Python bytestrings to Unicode before passing off to the database
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
    """BasePathNameModel

    Base name comparable with path
    """

    def __cmp__(self, other):
        if isinstance(other, basestring):
            return cmp(self.path, other)
        return 0


class BaseNamedModel(object):
    """BaseNameModel

    Base name comparable with name string
    """

    def __cmp__(self, other):
        if isinstance(other, basestring):
            return cmp(self.name, other)
        return 0


class SettingModel(Base):
    """SettingModel

    Soundforest internal application preferences

    """

    __tablename__ = 'setting'

    id = Column(Integer, primary_key=True)
    key = Column(SafeUnicode, unique=True)
    value = Column(SafeUnicode)


class SyncTargetModel(Base, BaseNamedModel):
    """SyncTargetModel

    Library tree synchronization target entry

    """

    __tablename__ = 'sync_target'

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

    __tablename__ = 'codec'

    id = Column(Integer, primary_key=True)
    name = Column(SafeUnicode)
    description = Column(SafeUnicode)

    def __repr__(self):
        return self.name

    def add_extension(self, session, extension):
        existing = session.query(ExtensionModel).filter(
            ExtensionModel.extension == extension
        ).first()
        if existing:
            raise SoundforestError('ExtensionModel already registered: {0}'.format(extension))

        session.add(ExtensionModel(codec=self, extension=extension))
        session.commit()

    def delete_extension(self, session, extension):
        existing = session.query(ExtensionModel).filter(
            ExtensionModel.extension == extension
        ).first()
        if not existing:
            raise SoundforestError('ExtensionModel was not registered: {0}'.format(extension))

        session.delete(existing)
        session.commit()

    def add_decoder(self, session, command):
        existing = session.query(DecoderModel).filter(
            DecoderModel.codec == self,
            DecoderModel.command == command
        ).first()
        if existing:
            raise SoundforestError('DecoderModel already registered: {0}'.format(command))

        session.add(DecoderModel(codec=self, command=command))
        session.commit()

    def delete_decoder(self, session, command):
        existing = session.query(DecoderModel).filter(
            DecoderModel.codec == self,
            DecoderModel.command == command
        ).first()
        if not existing:
            raise SoundforestError('DecoderModel was not registered: {0}'.format(command))

        session.delete(existing)
        session.commit()

    def add_encoder(self, session, command):
        existing = session.query(EncoderModel).filter(
            EncoderModel.codec == self,
            EncoderModel.command == command
        ).first()
        if existing:
            raise SoundforestError('EncoderModel already registered: {0}'.format(command))

        session.add(EncoderModel(codec=self, command=command))
        session.commit()

    def delete_encoder(self, session, command):
        existing = session.query(EncoderModel).filter(
            EncoderModel.codec == self,
            EncoderModel.command == command
        ).first()
        if not existing:
            raise SoundforestError('EncoderModel was not registered: {0}'.format(command))

        session.delete(existing)
        session.commit()

    def add_tester(self, session, command):
        existing = session.query(TesterModel).filter(
            TesterModel.codec == self,
            TesterModel.command == command
        ).first()
        if existing:
            raise SoundforestError('File tester already registered: {0}'.format(command))

        session.add(TesterModel(codec=self, command=command))
        session.commit()

    def delete_tester(self, session, command):
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

    __tablename__ = 'extension'

    id = Column(Integer, primary_key=True)
    extension = Column(SafeUnicode)
    codec_id = Column(Integer, ForeignKey('codec.id'), nullable=False)
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

    codec_id = Column(Integer, ForeignKey('codec.id'), nullable=False)
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

    __tablename__ = 'decoder'

    id = Column(Integer, primary_key=True)
    priority = Column(Integer)
    command = Column(SafeUnicode)
    codec_id = Column(Integer, ForeignKey('codec.id'), nullable=False)
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

    __tablename__ = 'encoder'

    id = Column(Integer, primary_key=True)
    priority = Column(Integer)
    command = Column(SafeUnicode)
    codec_id = Column(Integer, ForeignKey('codec.id'), nullable=False)
    codec = relationship('CodecModel',
        single_parent=False,
        backref=backref('encoders',
            order_by=priority,
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return '{0} encoder: {1}'.format(self.codec.name, self.command)


class TreeTypeModel(Base, BaseNamedModel):
    """TreeTypeModel

    Audio file tree types (music, samples, loops etc.)
    """

    __tablename__ = 'treetype'

    id = Column(Integer, primary_key=True)
    name = Column(SafeUnicode, unique=True)
    description = Column(SafeUnicode)

    def __repr__(self):
        return self.name


class TreePrefixModel(Base, BasePathNamedModel):
    """TreePrefixModel

    Audio tree prefix paths
    """

    __tablename__ = 'treeprefix'

    id = Column(Integer, primary_key=True)
    path = Column(SafeUnicode, unique=True)

    def __repr__(self):
        return self.path


class TreeModel(Base, BasePathNamedModel):
    """TreeModel

    Audio file tree
    """

    __tablename__ = 'tree'

    id = Column(Integer, primary_key=True)
    path = Column(SafeUnicode, index=True, unique=True)
    source = Column(SafeUnicode, default=u'filesystem')
    description = Column(SafeUnicode)

    type_id = Column(Integer, ForeignKey('treetype.id'), nullable=True)
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
        return session.query(TagModel).filter(
            TrackModel.tree == self).filter(
            TagModel.track_id == TrackModel.id
        ).count()

    def match_tag(self, session, match):
        """Match database track tags

        Return tracks matching given tag value

        """
        return session.query(TrackModel).filter(
            TrackModel.tree == self).filter(
            TagModel.track_id == TrackModel.id).filter(
            TagModel.value.like('%{0}%'.format(match))
        ).all()

    def filter_tracks(self, session, path):
        res = session.query(TrackModel).filter(TrackModel.tree == self)
        return res.filter(
            TrackModel.directory.like('%{0}%'.format(path)) |
            TrackModel.name.like('%{0}%'.format(path))
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

    Music albums in tree, relative to tree path
    """

    __tablename__ = 'album'
    __table_args__ = (
        Index( 'tree_album_directory', 'tree_id', 'directory', ),
    )

    id = Column(Integer, primary_key=True)

    directory = Column(SafeUnicode, index=True)
    mtime = Column(Integer)

    parent_id = Column(Integer, ForeignKey('albumpathcomponent.id'), nullable=True)
    parent = relationship('AlbumPathComponentModel',
        single_parent=False,
        backref=backref('albums',
            order_by=directory,
            cascade='all, delete, delete-orphan'
        )
    )

    tree_id = Column(Integer, ForeignKey('tree.id'), nullable=False)
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
        return os.path.join(self.tree.path, self.directory)

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

        Return album details as JSON

        """
        track_info = [{'id': t.id, 'name': t.name} for t in self.tracks]
        return json.dumps({
            'id': self.id,
            'tree': self.tree.id,
            'directory': self.directory,
            'modified': self.modified_isoformat,
            'tracks': track_info
        })


class AlbumPathComponentModel(Base, BasePathNamedModel):
    """AlbumPathComponentModel

    Album relative path components
    """

    __tablename__ = 'albumpathcomponent'
    __table_args__ = (
        Index( 'album_path_component', 'tree_id', 'level', 'name', ),
    )

    id = Column(Integer, primary_key=True)
    level = Column(Integer)
    name = Column(SafeUnicode, index=True)

    tree_id = Column(Integer, ForeignKey('tree.id'), nullable=True)
    tree = relationship('TreeModel',
        single_parent=False,
        backref=backref('components',
            order_by=level,
            cascade='all, delete, delete-orphan'
        )
    )

    parent_id = Column(Integer, ForeignKey('albumpathcomponent.id'), nullable=True)
    parent = relationship('AlbumPathComponentModel',
        single_parent=True,
        remote_side=[id],
        backref=backref('children',
            order_by=name,
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return '{0} {1}'.format(self.level, self.name)


class AlbumArtModel(Base):
    """AlbumArtModel

    Albumart files for music albums in tree database
    """

    __tablename__ = 'albumart'

    id = Column(Integer, primary_key=True)
    mtime = Column(Integer)
    albumart = Column(Base64Field)

    album_id = Column(Integer, ForeignKey('album.id'), nullable=True)
    album = relationship('AlbumModel',
        single_parent=False,
        backref=backref('albumart',
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return 'AlbumArtModel for {0}'.format(self.album.path)


class TrackModel(Base, BasePathNamedModel):
    """TrackModel

    Audio file. Optionally associated with a audio file tree
    """

    __tablename__ = 'track'
    __table_args__ = (
        Index('track_directory_name_extension', 'directory', 'name', 'extension', ),
    )

    id = Column(Integer, primary_key=True)

    directory = Column(SafeUnicode)
    name = Column(SafeUnicode)
    extension = Column(SafeUnicode)

    checksum = Column(SafeUnicode)
    mtime = Column(Integer)
    deleted = Column(Boolean)

    tree_id = Column(Integer, ForeignKey('tree.id'), nullable=True)
    tree = relationship('TreeModel',
        single_parent=False,
        backref=backref('tracks',
            order_by=[directory, name],
            cascade='all, delete, delete-orphan'
        )
    )

    album_id = Column(Integer, ForeignKey('album.id'), nullable=True)
    album = relationship('AlbumModel',
        single_parent=False,
        backref=backref('tracks',
            order_by=[directory, name],
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return os.sep.join([self.directory, self.name])

    @property
    def path(self):
        return os.path.join(self.directory, '{0}.{1}'.format(self.name, self.extension))

    def relative_path(self):
        path = self.path
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

    def to_json(self):
        return json.dumps({
            'id': self.id,
            'directory': self.directory,
            'name': self.name,
            'extension': self.extension,
            'checksum': self.checksum,
            'modified': self.modified_isoformat,
            'tags': dict((t.tag, t.value) for t in self.tags)
        })


class TagModel(Base):
    """TagModel

    Metadata tag for an audio file
    """

    __tablename__ = 'tag'
    __table_args = (
        Index( 'track_tag', 'track_id', 'tag', ),
    )

    id = Column(Integer, primary_key=True)
    tag = Column(SafeUnicode)
    value = Column(SafeUnicode)
    base64_encoded = Column(Boolean)

    track_id = Column(Integer, ForeignKey('track.id'), nullable=False)
    track = relationship('TrackModel',
        single_parent=False,
        backref=backref('tags',
            order_by=tag,
            cascade='all, delete, delete-orphan'
        )
    )

    def __repr__(self):
        return '{0}={1}'.format(self.tag, self.value)


class PlaylistTreeModel(Base, BaseNamedModel):
    """PlaylistTreeModel

    PlaylistTreeModel parent folders

    """

    __tablename__ = 'playlist_tree'

    id = Column(Integer, primary_key=True)
    name = Column(SafeUnicode)
    path = Column(SafeUnicode, unique=True)

    def __repr__(self):
        return '{0}: {1}'.format(self.name, self.path)

    @property
    def exists(self):
        """Check if path exists

        Return true if registered path exists

        """
        return os.path.isdir(os.path.realpath(self.path))

    def update(self, session, tree):
        """Read playlists to database from tree

        Tree must be iterable playlist tree object, for example
        soundforest.playlist.m3uPlaylistDirectory

        """
        for playlist in tree:

            db_playlist = session.query(PlaylistModel).filter(
                PlaylistModel.parent == self,
                PlaylistModel.directory == playlist.directory,
                PlaylistModel.name == playlist.name,
                PlaylistModel.extension == playlist.extension
            ).first()

            if db_playlist is None:
                db_playlist = PlaylistModel(
                    parent=self,
                    directory=playlist.directory,
                    name=playlist.name,
                    extension=playlist.extension
                )
                session.add(db_playlist)

            db_playlist.update(session, playlist)


class PlaylistModel(Base, BaseNamedModel):
    """PlaylistModel

    PlaylistModel file of audio tracks
    """

    __tablename__ = 'playlist'
    __table_args__ = (
        Index('playlist_directory_name', 'directory', 'name', ),
    )

    id = Column(Integer, primary_key=True)

    updated = Column(Date)
    directory = Column(SafeUnicode)
    name = Column(SafeUnicode)
    extension = Column(SafeUnicode)
    description = Column(SafeUnicode)

    parent_id = Column(Integer, ForeignKey('playlist_tree.id'), nullable=True)
    parent = relationship('PlaylistTreeModel',
        single_parent=False,
        backref=backref('playlists',
            order_by=(directory, name,),
            cascade='all, delete, delete-orphan',
        )
    )

    def __repr__(self):
        return '{0}: {1:d} tracks'.format(os.sep.join([self.directory, self.name]), len(self.tracks))

    def __len__(self):
        return len(self.tracks)

    def update(self, session, playlist):
        for existing_track in self.tracks:
            session.delete(existing_track)

        try:
            playlist.read()
        except PlaylistError, emsg:
            logger.debug('Error reading playlist {0}: {1}'.format(playlist, emsg))
            return

        tracks = []
        for index, path in enumerate(playlist):
            position = index+1
            tracks.append(PlaylistTrackModel(playlist=self, path=path, position=position))

        session.add(tracks)
        self.updated = datetime.now()
        session.commit()


class PlaylistTrackModel(Base, BasePathNamedModel):
    """PlaylistTrackModel

    Audio track in a playlist
    """
    __table_args__ = (
        Index('playlist_track_playlist_position', 'playlist_id', 'position', 'path', ),
    )

    __tablename__ = 'playlist_track'

    id = Column(Integer, primary_key=True)

    position = Column(Integer)
    path = Column(SafeUnicode)

    playlist_id = Column(Integer, ForeignKey('playlist.id'), nullable=False)
    playlist = relationship('PlaylistModel',
        single_parent=False,
        backref=backref('tracks',
            order_by=(position, path),
            cascade='all, delete, delete-orphan',
        )
    )

    def __repr__(self):
        return '{0:d} {1}'.format(self.position, self.path)


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
        self.session = sessionmaker(bind=engine)()

        indexes = (
        )
        inspector = reflection.Inspector.from_engine(engine)
        existing_index_names = [index['name'] for table in inspector.get_table_names() for index in inspector.get_indexes(table)]
        for index in indexes:
            if index.name not in existing_index_names:
                index.create(bind=engine)

    def _fk_pragma_on_connect(self, connection, record):
        """Enable foreign keys

        Enable foreign keys for sqlite databases during connect
        """
        if isinstance(connection, SQLite3Connection):
            cursor = connection.cursor()
            cursor.execute('pragma foreign_keys=ON')
            cursor.close()

    def query(self, *args, **kwargs):
        """Query session

        Wrapper to do a session query
        """
        return self.session.query(*args, **kwargs)

    def rollback(self):
        """Rollback session

        Wrapper to rolllback current session query
        """
        return self.session.rollback()

    def commit(self):
        """Commit session

        Wrapper to commit current session query
        """
        return self.session.commit()

    def as_dict(self, result):
        """Return query result dictionary

        Returns current query Base result as dictionary
        """
        if not hasattr(result, '__table__'):
            raise ValueError('Not a sqlalchemy ORM result')
        return dict((k.name, getattr(result, k.name)) for k in result.__table__.columns)

    def add(self, items):
        """Add items

        Add items in query session, committing changes
        """
        if isinstance(items, list):
            self.session.add_all(items)
        else:
            self.session.add(items)

        self.session.commit()

    def delete(self, items):
        """Delete items

        Delete items in query session, committing changes
        """
        if isinstance(items, list):
            for item in items:
                self.session.delete(item)
        else:
            self.session.delete(items)

        self.session.commit()

    @property
    def sync_targets(self):
        """Registered sync targets

        Return all SyncTargetModel objects
        """
        return self.query(SyncTargetModel).all()

    @property
    def codecs(self):
        """Registered codecs

        Return all CodecModel objects
        """
        return self.query(CodecModel).order_by(CodecModel.name).all()

    @property
    def tree_types(self):
        """Registered tree types

        Return all TreeTypeModel objects
        """
        return self.query(TreeTypeModel).order_by(TreeTypeModel.name).all()

    @property
    def playlist_trees(self):
        """Playlist trees

        Return all PlaylistTreeModel objects
        """
        return self.query(PlaylistTreeModel).order_by(PlaylistTreeModel.name).all()

    @property
    def playlists(self):
        """Playlists

        Return all PlaylistModel objects
        """
        return self.query(PlaylistModel).all()

    @property
    def tree_prefixes(self):
        """Path prefixes

        Return all TreePrefixModel objects
        """
        return self.query(TreePrefixModel).all()

    @property
    def trees(self):
        """Trees

        Return all TreeModel objects
        """
        return self.query(TreeModel).all()

    @property
    def albums(self):
        """Albums

        Return all AlbumModel objects
        """
        return self.query(AlbumModel).all()

    @property
    def tracks(self):
        """Tracks

        Return all TrackModel objects
        """
        return self.query(TrackModel).all()

    @property
    def settings(self):
        """Settings

        Return all SettingModel objects
        """
        return self.query(SettingModel).order_by(SettingModel.key).all()

    def add_setting(self, key, value):
        """Set setting value

        Update or create setting key, value pair
        """
        setting = self.query(SettingModel).filter(
            SettingModel.key == key
        ).first()

        if setting:
            self.delete(setting)

        self.add(SettingModel(key=key, value=value))

    def delete_setting(self, key):
        """Delete setting

        """
        setting = self.query(SettingModel).filter(
            SettingModel.key == key
        ).first()
        if setting:
            self.delete(setting)

    def get_setting(self, key):
        """Get setting

        Return value for setting key, or None if setting is not found
        """
        setting = self.query(SettingModel).filter(
            SettingModel.key == key
        ).first()
        if setting:
            return setting.value
        else:
            return None

    def add_sync_target(self, name, synctype, src, dst, flags=None, defaults=False):
        """Add sync target

        Add new sync target
        """

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

    def delete_sync_target(self, name):
        existing = self.query(SyncTargetModel).filter(SyncTargetModel.name == name).first()
        if not existing:
            raise SoundforestError('Sync target was not registered: {0}'.format(name))

        self.delete(existing)

    def add_codec(self, name, extensions, description='', decoders=[], encoders=[], testers=[]):
        """Register codec

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

    def add_tree_type(self, name, description=''):
        """Register tree type

        """
        existing = self.query(TreeTypeModel).filter(
            TreeTypeModel.name == name
        ).first()
        if existing:
            raise SoundforestError('Tree type was already registered: {0}'.format(name))

        self.add(TreeTypeModel(name=name, description=description))

    def delete_tree_type(self, name, description=''):
        """Unregister tree type

        """
        existing = self.query(TreeTypeModel).filter(
            TreeTypeModel.name == name
        ).first()
        if not existing:
            raise SoundforestError('Tree type was not registered: {0}'.format(name))

        self.delete(existing)

    def add_playlist(self, path, name='Playlists'):
        """Register playlist or playlist tree

        """
        if os.path.isdir(os.path.realpath(path)):
            existing = self.query(PlaylistTreeModel).filter(PlaylistTreeModel.path==path).first()
            if existing is not None:
                raise SoundforestError('Playlist source is already in database: {0}'.format(path))

            tree = PlaylistTreeModel(path=path, name=name)
            self.add(tree)
            tree.update(self, m3uPlaylistDirectory(path))

        elif os.path.isfile(os.path.realpath(path)):
            directory = os.path.dirname(path)
            name, extension = os.path.splitext(os.path.basename(path))
            extension = extension[1:]

            existing = self.query(PlaylistModel).filter(
                PlaylistModel.directory == directory,
                PlaylistModel.name == name,
                PlaylistModel.extension == extension,
            ).first()
            if existing is not None:
                raise SoundforestError('Playlist is already in database: {0}'.format(path))

            playlist = PlaylistModel(directory=directory, name=name, extension=extension)
            playlist.update(self, m3uPlaylist(path))
            self.add(playlist)

    def update_playlist(self, path):
        if os.path.isdir(os.path.realpath(path)):

            existing = self.query(PlaylistTreeModel).filter(
                PlaylistTreeModel.path == path
            ).first()
            if existing is None:
                raise SoundforestError('Playlist tree not found: {0}'.format(path))

            existing.update(self, m3uPlaylistDirectory(path))

        elif os.path.isfile(os.path.realpath(path)):
            directory = os.path.dirname(path)
            name, extension = os.path.splitext(os.path.basename(path))
            extension = extension[1:]

            existing = self.query(PlaylistModel).filter(
                PlaylistModel.directory == directory,
                PlaylistModel.name == name,
                PlaylistModel.extension == extension,
            ).first()
            if existing is None:
                raise SoundforestError('Playlist not found: {0}'.format(path))

            existing.update(self, m3uPlaylist(path))

    def delete_playlist(self, path):
        """Delete playlist tree

        """
        if os.path.isdir(os.path.realpath(path)):

            existing = self.query(PlaylistTreeModel).filter(
                PlaylistTreeModel.path == path
            ).first()
            if existing is None:
                raise SoundforestError('Playlist tree not found: {0}'.format(path))

            self.delete(existing)

        elif os.path.isfile(os.path.realpath(path)):
            directory = os.path.dirname(path)
            name, extension = os.path.splitext(os.path.basename(path))
            extension = extension[1:]

            existing = self.query(PlaylistModel).filter(
                PlaylistModel.directory == directory,
                PlaylistModel.name == name,
                PlaylistModel.extension == extension,
            ).first()

            if existing is None:
                raise SoundforestError('Playlist not found: {0}'.format(path))

            self.delete(existing)

    def add_prefix(self, path):
        """Register path prefix

        """
        if isinstance(path, str):
            path = unicode(path, 'utf-8')

        existing = self.query(TreePrefixModel).filter(
            TreePrefixModel.path == path
        ).first()
        if existing:
            raise SoundforestError('TreePrefix was already registered: {0}'.format(path))

        self.add(TreePrefixModel(path=path))

    def delete_prefix(self, path):
        """Unregister path prefix

        """
        existing = self.query(TreePrefixModel).filter(
            TreePrefixModel.path == path
        ).first()
        if not existing:
            raise SoundforestError('TreePrefix was not registered: {0}'.format(path))

        self.delete(existing)

    def add_tree(self, path, description='', tree_type='songs'):
        """Register tree

        """
        if isinstance(path, str):
            path = unicode(path, 'utf-8')

        existing = self.query(TreeModel).filter(
            TreeModel.path == path
        ).first()
        if existing:
            raise SoundforestError('Tree was already registered: {0}'.format(path))

        tt = self.get_tree_type(tree_type)
        self.add(TreeModel(path=path, description=description, type=tt))

    def delete_tree(self, path, description=''):
        """Unregister tree

        """
        existing = self.query(TreeModel).filter(
            TreeModel.path == path
        ).first()
        if not existing:
            raise SoundforestError('Tree was not registered: {0}'.format(path))

        self.delete(existing)

    def get_codec(self, name):
        """Return codec matching name

        """
        return self.query(CodecModel).filter(
            CodecModel.name == name
        ).first()

    def get_tree_type(self, name):
        """Return tree type matching name

        """
        return self.query(TreeTypeModel).filter(
            TreeTypeModel.name == name
        ).first()

    def get_tree(self, path, tree_type='songs'):
        """Return tree matching path

        """
        return self.query(TreeModel).filter(
            TreeModel.path == path
        ).first()

    def get_album(self, path):
        """Return album matching path

        """
        return self.query(AlbumModel).filter(
            AlbumModel.directory == path
        ).first()

    def get_track(self, path):
        """Return trach matching path

        """
        return self.query(TrackModel).filter(
            TrackModel.directory == os.path.dirname(path),
            TrackModel.name == os.path.basename(path),
        ).first()

    def get_playlist_tree(self, path):
        """Get playlist tree

        """
        return self.query(PlaylistTreeModel).filter(
            PlaylistTreeModel.path == path
        ).first()

    def get_playlist(self, path):
        """Get playlist by path
        """
        return self.query(PlaylistModel).filter(
            PlaylistModel.path == path
        ).first()

    def match_tracks_by_tree_prefix(self, path):
        """Match tracks

        Return tracks matching path prefix
        """
        return self.query(TrackModel).filter(TrackModel.directory.like('%{0}%'.format(path))).all()

