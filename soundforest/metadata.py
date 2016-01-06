"""
Common metadata file matches seen in music file directories.

Please note these classes are not used to open or process the metadata files:
this module is intended only for detecting the type of files.
"""

import os

# List of filenames we consider as albumart images. Can't be arsed to store this
# to database, it's a static list anyway and we can extend the list if needed!
ALBUMART_FILENAMES = (
    'artwork.jpg',
    'albumart.jpg',
    'coverart.jpg',
    'front.jpg',
    'back.jpg',
)

# Same for cover booklet filenames: rename these consistently in your library!
BOOKLET_FILENAMES = (
    'booklet.pdf',
)

# Apple OS/X system files we wish to ignore
OSX_SYSTEM_FILES = [
    '.com.apple.timemachine.supported',
    '.DS_Store',
]


class MetadataFileType(object):
    """
    Parent class for metadata file description classes.

    Attributes:
    description     Textual description for this metadata file type
    filenames       List of full filenames to match to this type of metadata
    extensions      List of file extensions to match to this type of metadata
    """
    def __init__(self, description, filenames=None, extensions=None):
        self.description = description
        self.filenames = filenames
        self.extensions = extensions

    def __repr__(self):
        """
        Returns the description string
        """
        return self.description

    def match(self, path):
        """
        Match given path to this metadata file type. Override in child class
        if you need more complicated logic.

        Returns true if the filename matches metadata type, False if not.
        """
        path = os.path.realpath(path)
        if self.filenames:
            if os.path.basename(path) in self.filenames:
                return True

        if self.extensions:
            ext = os.path.splitext(path)[1][1:].lower()
            if ext in self.extensions:
                return True

        return False


class OSXSystemFile(MetadataFileType):
    """
    OS/X system metadata files not relevant for audio trees.
    """
    def __init__(self):
        super(OSXSystemFile, self).__init__('OS/X System file', filenames=OSX_SYSTEM_FILES)


class AbletonAnalysisFile(MetadataFileType):
    """
    Ableton track metadata files.
    """
    def __init__(self):
        super(AbletonAnalysisFile, self).__init__('Ableton Live Track Metadata', extensions=['asd'])


class PDFBooklet(MetadataFileType):
    """
    PDF format album booklet, as received from itunes.

    Right now, we expect the file to be renamed to booklet.pdf in same directory
    as the album. Someone else may add parser for PDF files in general if needed.
    """
    def __init__(self):
        super(PDFBooklet, self).__init__('Album Cover Booklet', filenames=BOOKLET_FILENAMES)


class CoverArt(MetadataFileType):
    """
    Coverart files stored to the album directory with music files.

    Static list of albumart filenames we process are defined in module sources.
    """
    def __init__(self):
        super(CoverArt, self).__init__('Album Artwork', filenames=ALBUMART_FILENAMES)


class m3uPlaylist(MetadataFileType):
    """
    Playlist files in m3u format
    """
    def __init__(self):
        super(m3uPlaylist, self).__init__('m3u playlist', extensions=['m3u'])


class Metadata(list):
    """
    Load metadata parsers and match filenames to the parsers
    """
    def __init__(self):
        """
        Register instances of the MetadataFileType classes in the module
        """
        self.add_metadata(CoverArt())
        self.add_metadata(m3uPlaylist())
        self.add_metadata(AbletonAnalysisFile())
        self.add_metadata(PDFBooklet())
        self.add_metadata(OSXSystemFile())

    def add_metadata(self, metadata_class):
        """
        Register instance of a MetadataFileType class
        """
        if not isinstance(metadata_class, MetadataFileType):
            raise ValueError('Not a MetadataFileType instance')

        self.append(metadata_class)

    def match(self, path):
        """
        Match path to registered metadata type parsers.
        Returns matching metadata class or None
        """
        for m in self:
            if m.match(path):
                return m

        return None

