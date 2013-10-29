# coding=utf-8
"""Soundforest configuration database

Soundforest configuration database, implementing the database
classes in soundforest.models for cli scripts.

"""

import os

from soundforest import models, SoundforestError
from soundforest.log import SoundforestLogger
from soundforest.defaults import DEFAULT_CODECS, DEFAULT_TREE_TYPES

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
            self.log = SoundforestLogger().default_stream
            models.SoundforestDB.__init__(self, path=path)

            treetypes = self.session.query(models.TreeTypeModel).all()
            if not treetypes:
                treetypes = []
                for name,description in DEFAULT_TREE_TYPES.items():
                    treetypes.append(models.TreeTypeModel(
                        name=name,
                        description=description
                    ))
                self.add(treetypes)
                self.commit()

            self.codecs = CodecConfiguration(db=self)
            self.sync = SyncConfiguration(db=self)

        def get(self, key):
            entry = self.session.query(models.SettingModel).filter(
                models.SettingModel.key==key
            ).first()

            return entry is not None and entry.value or None

        def set(self, key, value):
            existing = self.session.query(models.SettingModel).filter(
                models.SettingModel.key==key
            ).first()
            if existing:
                self.session.delete(existing)

            self.session.add(models.SettingModel(
                key=key,
                value=value
            ))
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
                    raise SoundforestError('Invalid data in configuration for field %s' % key)

            return value

        def has_key(self, key):
            return self.get(key) is not None

        def keys(self):
            return [s.key for s in self.session.query(models.SettingModel).all()]

        def items(self):
            return [(s.key, s.value) for s in self.session.query(models.SettingModel).all()]

        def values(self):
            return [s.value for s in self.session.query(models.SettingModel).all()]

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
        ConfigDBDictionary.__init__(self, db)

        for target in self.db.registered_sync_targets:
            self[target.name] = target.as_dict()

    @property
    def threads(self):
        return int(self.db.get('threads'))

    @property
    def default_targets(self):
        return [k for k in self.keys() if self[k]['defaults']]

    def create_sync_target(self, name, synctype, src, dst, flags=None, defaults=False):
        self[name] = self.db.register_sync_target(name, synctype, src, dst, flags, defaults)


class CodecConfiguration(ConfigDBDictionary):

    """CodecConfiguration

    Audio codec decoder/encoder commands configuration API

    """

    def __init__(self, db):
        ConfigDBDictionary.__init__(self, db)

        self.load()

    def load(self):
        for codec in self.db.registered_codecs:
            self[str(codec.name)] = codec

        for name, settings in DEFAULT_CODECS.items():
            if name  in self.keys():
                continue

            codec = self.db.register_codec(name, **settings)
            self[str(codec.name)] = codec

    def extensions(self, codec):
        if codec in self.keys():
            return [codec] + [e.extension for e in self[codec].extensions]
        return []
