#!/usr/bin/env python
"""
SQLAlchemy models for soundforest music database
"""

import os

from sqlite3 import Connection as SQLite3Connection
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy import Column, ForeignKey, Integer, String, Boolean, Date

DEFAULT_DATABASE = '~/.soundforest/soundforest.sqlite'

Base = declarative_base()


class Codec(Base):
    """
    Audio format codecs
    """

    __tablename__ = 'codec'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)

    def __repr__(self):
        return self.name

class Extensions(Base):
    """
    Filename extensions associated with audio format codecs
    """

    __tablename__ = 'extensions'

    id = Column(Integer, primary_key=True)
    extension = Column(String)
    codec_id = Column(Integer, ForeignKey('codec.id'), nullable=False)
    codec = relationship("Codec", single_parent=False,
        backref=backref('extensions', order_by=extension, cascade="all, delete, delete-orphan")
    )

    def __repr__(self):
        return self.extension


class Decoder(Base):
    """
    Audio format codec decoders
    """

    __tablename__ = 'decoders'

    id = Column(Integer, primary_key=True)
    priority = Column(Integer)
    command = Column(String)
    codec_id = Column(Integer, ForeignKey('codec.id'), nullable=False)
    codec = relationship("Codec", single_parent=False,
        backref=backref('decoders', order_by=priority, cascade="all, delete, delete-orphan")
        )


    def __repr__(self):
        return '%s decoder: %s' % (self.codec.name, self.command)


class Encoder(Base):
    """
    Audio format codec encoders
    """

    __tablename__ = 'encoders'

    id = Column(Integer, primary_key=True)
    priority = Column(Integer)
    command = Column(String)
    codec_id = Column(Integer, ForeignKey('codec.id'), nullable=False)
    codec = relationship("Codec", single_parent=False,
        backref=backref('encoders', order_by=priority, cascade="all, delete, delete-orphan")
        )

    def __repr__(self):
        return '%s encoder: %s' % (self.codec.name, self.command)


class PlaylistSource(Base):
    """
    Playlist parent folders
    """

    __tablename__ = 'playlist_sources'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    path = Column(String)

    def __repr__(self):
        return '%s: %s' % (self.name,self.path)

class Playlist(Base):
    """
    Playlist of audio tracks
    """

    __tablename__ = 'playlists'

    id = Column(Integer, primary_key=True)

    updated = Column(Date)
    folder = Column(String)
    name = Column(String)
    description = Column(String)

    parent_id = Column(Integer, ForeignKey('playlist_sources.id'), nullable=False)
    parent = relationship("PlaylistSource", single_parent=False,
        backref=backref('playlists', order_by=[folder,name], cascade="all, delete, delete-orphan")
    )

    def __repr__(self):
        return '%s: %d tracks' % (os.sep.join([self.folder, self.name]), len(self.tracks))


class PlaylistTrack(Base):
    """
    Audio track in a playlist
    """

    __tablename__ = 'playlist_tracks'

    id = Column(Integer, primary_key=True)

    position = Column(Integer, unique=True)
    path = Column(String)

    playlist_id = Column(Integer, ForeignKey('playlists.id'), nullable=False)
    playlist = relationship("Playlist", single_parent=False,
        backref=backref('tracks', order_by=position, cascade="all, delete, delete-orphan")
    )

    def __repr__(self):
        return '%d %s' % (self.position, self.path)

class TreeType(Base):
    """
    Audio file tree types (music, samples, loops etc.)
    """

    __tablename__ = 'treetypes'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)

    def __repr__(self):
        return self.name


class Tree(Base):
    """
    Audio file tree
    """

    __tablename__ = 'trees'

    id = Column(Integer, primary_key=True)
    path = Column(String)

    type_id = Column(Integer, ForeignKey('treetypes.id'), nullable=True)
    type = relationship("TreeType", single_parent=True,
        backref=backref('trees', order_by=path, cascade="all, delete, delete-orphan")
    )

    def __repr__(self):
        return self.path


class Track(Base):
    """
    Audio file. Optionally associated with a audio file tree
    """

    __tablename__ = 'tracks'

    id = Column(Integer, primary_key=True)

    directory = Column(String)
    filename = Column(String)
    extension = Column(String)
    checksum = Column(String)
    mtime = Column(Integer)
    deleted = Column(Boolean)

    tree_id = Column(Integer, ForeignKey('trees.id'), nullable=True)
    tree = relationship("Tree", single_parent=True,
        backref=backref('tracks', order_by=[directory, filename], cascade="all, delete, delete-orphan")
        )

    def __repr__(self):
        return os.sep.join([self.directory, self.filename])


class Tag(Base):
    """
    Tags for an audio file
    """

    __tablename__ = 'tags'

    id = Column(Integer, primary_key=True)
    tag = Column(String)
    value = Column(String)
    base64_encoded = Column(Boolean)

    track_id = Column(Integer, ForeignKey('tracks.id'), nullable=False)
    track = relationship("Track", single_parent=False,
        backref=backref('tags', order_by=tag, cascade="all, delete, delete-orphan")
    )

    def __repr__(self):
        return '%s=%s' % (self.tag, self.value)


class MusaDB(object):
    def __init__(self,engine=None,echo=False):
        if engine is None:
            engine = create_engine('sqlite:///%s' % os.path.expanduser(DEFAULT_DATABASE), echo=echo)
        event.listen(engine, 'connect', self._fk_pragma_on_connect)
        Base.metadata.create_all(engine)

        session_instance = sessionmaker(bind=engine)
        self.session = session_instance()

    def _fk_pragma_on_connect(self, connection, record):
        if isinstance(connection, SQLite3Connection):
            cursor = connection.cursor()
            cursor.execute('pragma foreign_keys=ON')
            cursor.close()


if __name__ == '__main__':
    db = MusaDB()
    tree = Tree(path='/tmp')
    track = Track(tree=tree, directory='/tmp', filename='test.m4a')

    db.session.add_all([
        track,
        Tag(track=track, tag="artist", value="test artist"),
        Tag(track=track, tag="album", value="test album"),
    ])

    print 'After create'
    for track in db.session.query(Track).all():
        print track
        for tag in track.tags:
            print '  %s' % tag
        # db.session.delete(track)

    [db.session.delete(t) for t in db.session.query(Tree).all()]
    db.session.commit()

    print 'After delete'
    for track in db.session.query(Track).all():
        print track
        for tag in track.tags:
            print '  %s' % tag
        # db.session.delete(track)
