# coding=utf-8
"""Soundforest configuration database

Soundforest configuration database, implementing the database
classes in soundforest.models for cli scripts.

"""

import os
import hashlib

from soundforest import models, TreeError, SoundforestError
from soundforest.log import SoundforestLogger
from soundforest.defaults import DEFAULT_CODECS, DEFAULT_TREE_TYPES

from sqlalchemy.orm.exc import NoResultFound

FIELD_CONVERT_MAP = {
    'threads': lambda x: int(x)
}


class ConfigDB(object):
    """ConfigDB

    Configuration database settings API

    """

    __db_instance = None
    def __init__(self, path=None):
        if not ConfigDB.__db_instance:
            ConfigDB.__db_instance = ConfigDB.ConfigInstance(path)
        self.__dict__['ConfigDB.__db_instance'] = ConfigDB.__db_instance

    def __getattr__(self, attr):
        return getattr(self.__db_instance, attr)

    class ConfigInstance(models.SoundforestDB):
        """Configuration database instance

        Singleton instance of configuration database

        """
        def __init__(self, path):
            models.SoundforestDB.__init__(self, path=path)

            self.log = SoundforestLogger().default_stream

            treetypes = self.session.query(models.TreeTypeModel).all()
            if not treetypes:
                treetypes = []

                for name,description in DEFAULT_TREE_TYPES.items():
                    treetypes.append(models.TreeTypeModel(name=name, description=description))

                self.add(treetypes)
                self.commit()

            self.codec_configuration = CodecConfiguration(db=self)
            self.sync_configuration = SyncConfiguration(db=self)

        def get(self, key):
            entry = self.session.query(models.SettingModel).filter(models.SettingModel.key==key).first()

            return entry is not None and entry.value or None

        def set(self, key, value):
            existing = self.session.query(models.SettingModel).filter(models.SettingModel.key==key).first()
            if existing:
                self.session.delete(existing)

            self.session.add(models.SettingModel(key=key, value=value))
            self.session.commit()

        def __getitem__(self, key):
            value = self.get(key)
            if value is None:
                raise KeyError

            return value

        def __setitem__(self, key, value):
            self.set(key, value)

        def __format_item__(self, key, value):
            if key in FIELD_CONVERT_MAP.keys():
                try:
                    value = FIELD_CONVERT_MAP[key](value)
                except ValueError:
                    raise SoundforestError('Invalid data in configuration for field {0}'.format(key))

            return value

        def has_key(self, key):
            return self.get(key) is not None

        def keys(self):
            return [s.key for s in self.session.query(models.SettingModel).all()]

        def items(self):
            return [(s.key, s.value) for s in self.session.query(models.SettingModel).all()]

        def values(self):
            return [s.value for s in self.session.query(models.SettingModel).all()]

    def update_tree(self, tree, update_checksum=False, progresslog=False):
        """
        Update tracks in database from loaded tree instance
        """
        added, updated, deleted, errors = 0, 0, 0, 0

        db_tree = self.get_tree(tree.path)
        albums = tree.as_albums()

        album_paths = [album.path for album in albums]
        track_paths = tree.realpaths

        processed = 0

        self.log.debug('{0} update tree'.format(tree.path))
        for album in albums:

            album_relative_path = tree.relative_path(album.path)
            if not album_relative_path:
                raise SoundforestError('{0} album relative path is empty: {1}'.format(tree.path, album.path))

            db_album = self.query(models.AlbumModel).filter(
                models.AlbumModel.tree == db_tree,
                models.AlbumModel.directory == album_relative_path
            ).first()

            if db_album is None:
                self.log.debug('{0} add album {1}'.format(tree.path, album_relative_path))
                db_album = models.AlbumModel(
                    tree=db_tree,
                    directory=album_relative_path,
                    mtime=album.mtime
                )

            elif db_album.mtime != album.mtime:
                self.log.debug('{0} update album mtime {1}'.format(tree.path, album_relative_path))
                db_album.mtime = album.mtime

            self.update_album_path_components(db_album)

            self.log.debug('{0} update album tracks {1}'.format(tree.path, album_relative_path))
            for track in album:
                db_track = self.query(models.TrackModel).filter(
                    models.TrackModel.directory == track.directory,
                    models.TrackModel.name == track.filename_no_extension,
                    models.TrackModel.extension == track.extension,
                ).first()

                if db_track is None:
                    self.log.debug('{0} add track {1}'.format(tree.path, tree.relative_path(track)))
                    db_track = models.TrackModel(
                        tree=db_tree,
                        album=db_album,
                        directory=track.directory,
                        name=track.filename_no_extension,
                        extension=track.extension,
                        mtime=track.mtime,
                        deleted=False,
                    )
                    if self.update_track(track, update_checksum, db_track=db_track):
                        added +=1
                    else:
                        errors +=1

                elif db_track.mtime != track.mtime:
                    self.log.debug('{0} update track {1}'.format(tree.path, tree.relative_path(track)))
                    if self.update_track(track, update_checksum):
                        updated += 1
                    else:
                        errors +=1

                elif not db_track.checksum and update_checksum:
                    self.log.debug('{0} update checksum {1}'.format(tree.path, tree.relative_path(track)))
                    if self.update_track_checksum(track) is not None:
                        updated += 1
                    else:
                        errors +=1

                processed += 1
                if progresslog and processed % 1000 == 0:
                    self.log.debug('{0} processed {1:d} tracks'.format(tree.path, processed))

            self.commit()

        self.log.debug('{0} check for removed albums'.format(tree.path))
        for album in db_tree.albums:
            if album.path in album_paths or album.exists:
                continue

            self.log.debug('{0} remove album {1}'.format(tree.path, album.relative_path()))
            self.delete(album)

        self.log.debug('{0} check for removed tracks'.format(tree.path))
        for db_track in db_tree.tracks:
            if db_track.path in track_paths or db_track.exists:
                continue

            self.log.debug('{0} remove track {1}'.format(tree.path, db_track.relative_path()))
            self.delete(db_track)
            deleted += 1

        self.commit()

        self.log.debug('{0} {1:d} added, {2:d} updated, {3:d} deleted, {4:d} errors'.format(tree.path, added, updated, deleted, errors,))

        return added, updated, deleted, processed, errors

    def find_tracks(self, path):
        track = self.get_track(path)
        if track is not None:
            return [track]

        db_tree = self.get_tree(path)
        if db_tree is not None:
            return db_tree.tracks

        db_album = self.get_album(path)
        if db_album is not None:
            return db_album.tracks

        if os.path.isdir(os.path.realpath(path)):
            return self.match_tracks_by_tree_prefix(path)

        return []

    def update_album_path_components(self, album):
        parts = album.directory.split(os.sep)

        if not parts:
            return

        parent = None
        component = None
        for level, name in enumerate(parts):

            if name == '':
                continue

            try:
                component = self.query(models.AlbumPathComponentModel).filter(
                    models.AlbumPathComponentModel.tree==album.tree,
                    models.AlbumPathComponentModel.parent==parent,
                    models.AlbumPathComponentModel.name==name,
                    models.AlbumPathComponentModel.level==level,
                ).one()

                if component.name != name:
                    component.name = name

            except NoResultFound:
                component = models.AlbumPathComponentModel(tree=album.tree, parent=parent, level=level, name=name)
                self.add(component)

            parent = component

        album.parent = component.parent

        for invalid in self.query(models.AlbumPathComponentModel).filter(
            models.AlbumPathComponentModel.tree==album.tree,
            models.AlbumPathComponentModel.name==name,
            level>len(parts),
        ):
            self.delete(invalid)

    def update_track(self, track, update_checksum=False, db_track=None):
        if db_track is None:
            db_track = self.get_track(track.path)

        if db_track is None:
            self.log.debug('ERROR updating track {0}: not found in database'.format(track.path))

        db_track.mtime = track.mtime

        oldtags = self.query(models.TrackModel).filter(models.TagModel.track == db_track)
        for tag in oldtags:
            self.delete(tag)

        try:
            tags = track.tags
        except TreeError, emsg:
            self.log.debug('ERROR loading {0}: {1}'.format(track.path, emsg))
            return False

        for tag, value in tags.items():
            if isinstance(value, list):
                value = value[0]
            self.add(models.TagModel(track=db_track, tag=tag, value=value))

        self.commit()

        if update_checksum:
            if self.update_track_checksum(track) is None:
                return False

        return True

    def update_track_checksum(self, track):
        db_track = self.get_track(track.path)
        if db_track is not None:
            checksum = track.checksum
            if db_track.checksum != checksum:
                db_track.checksum = checksum
                self.commit()
                return checksum
            else:
                return None
        else:
            return None

class ConfigDBDictionary(dict):
    """Configuration database dictionary

    Generic dictionary like instance of configuration databases

    """
    def __init__(self, db):
        self.log = SoundforestLogger().default_stream
        self.db = db

    def keys(self):
        return sorted(dict.keys(self))

    def items(self):
        return [(k, self[k]) for k in self.keys()]

    def values(self):
        return [self[k] for k in self.keys()]


class SyncConfiguration(ConfigDBDictionary):
    """SyncConfiguration

    Directory synchronization target configuration API

    """

    def __init__(self, db):
        super(SyncConfiguration, self).__init__(db)

        for target in self.db.sync_targets:
            self[target.name] = target.as_dict()

    @property
    def threads(self):
        return int(self.db.get('threads'))

    @property
    def default_targets(self):
        return [k for k in self.keys() if self[k]['defaults']]

    def add_sync_target(self, name, synctype, src, dst, flags=None, defaults=False):
        self[name] = self.db.add_sync_target(name, synctype, src, dst, flags, defaults)


class CodecConfiguration(ConfigDBDictionary):

    """CodecConfiguration

    Audio codec decoder/encoder commands configuration API

    """

    def __init__(self, db):
        super(CodecConfiguration, self).__init__(db)

        self.load()

    def load(self):
        for codec in self.db.codecs:
            self[str(codec.name)] = codec

        for name, settings in DEFAULT_CODECS.items():
            if name  in self.keys():
                continue

            codec = self.db.add_codec(name, **settings)
            self[str(codec.name)] = codec

    def extensions(self, codec):
        if codec in self.keys():
            return [codec] + [e.extension for e in self[codec].extensions]
        return []

    def match(self, path):
        ext = os.path.splitext(path)[1][1:]

        if ext == '':
            ext = path

        if ext in self.keys():
            return self[ext]

        for codec in self.values():
            if ext in [e.extension for e in codec.extensions]:
                return codec

        return None
