"""
Database backends for soundforest
"""

__all__ = (
    'backend_sqlite',
    'models'
)


DEFAULT_DATABASE_BACKEND = 'sqlite'
DATABASE_BACKENDS = {
    'sqlite':   'soundforest.database.backend_sqlite.SQliteBackend',
}

class SoundForestDBError(Exception):
    """
    Exceptions raised by all database backends
    """
    def __str__(self):
        return self.args[0]
