"""
Tag constants, including names and descriptions of
standard tags
"""

# Used for formatting output nicely
STANDARD_TAG_ORDER = [
    'album_artist',
    'composer',
    'artist',
    'conductor',
    'orchestra',
    'performers',
    'album',
    'title',
    'comment',
    'notes',
    'description',
    'location',
    'genre',
    'year',
    'bpm',
    'key',
    'tracknumber',
    'totaltracks',
    'disknumber',
    'totaldisks',
    'label',
    'license',
    'copyright',
    'sort_album_artist',
    'sort_composer',
    'sort_artist',
    'sort_performers',
    'sort_show',
    'sort_album',
    'sort_title',
]

# Tags common to all file formats. All tags in database are
# unicode strings or base64 formatted strings (if base64 flag
# is set)
STANDARD_TAG_MAP = {
    'album_artist': {
        'label': 'Album Artist',
        'description': 'Artist for the album',
    },
    'composer': {
        'label': 'Composer',
        'description': 'Composer of the track',
    },
    'artist': {
        'label': 'Artist',
        'description': 'Performer of the track',
    },
    'conductor': {
        'label': 'Conductor',
        'description': 'Conductor of the performance',
    },
    'orchestra': {
        'label': 'Orchestra',
        'description': 'Orchestra performing the track',
    },
    'performers': {
        'label': 'Performers',
        'description': 'Artists performing the track',
    },
    'album': {
        'label': 'Album',
        'description': 'Album title',
    },
    'title': {
        'label': 'Title',
        'description': 'Track title',
    },
    'genre': {
        'label': 'Genre',
        'description': 'Musical genre of the track',
    },
    'comment': {
        'label': 'Comment',
        'description': 'Comment for track',
    },
    'notes': {
        'label': 'Note',
        'description': 'A generic note for the track',
    },
    'description': {
        'label': 'Description',
        'description': 'Description of the item',
    },
    'location': {
        'label': 'Location',
        'description': 'Recording location of the track',
    },
    'year': {
        'label': 'Year',
        'description': 'Year the track was performed',
    },
    'bpm': {
        'label': 'BPM',
        'description': 'Track BPM value',
    },
    'key': {
        'label': 'Key',
        'description': 'Musical key of the track',
        },
    'tracknumber': {
        'label': 'Track Number',
        'description': 'Track number in album',
        },
    'totaltracks': {
        'label': 'Total Tracks',
        'description': 'Total number of tracks in album',
        },
    'disknumber': {
        'label': 'Disk Number',
        'description': 'Disk number in multi disk albums',
        },
    'totaldisks': {
        'label': 'Total Disks',
        'description': 'Total number of disks in album',
    },
    'label': {
        'label': 'Label',
        'description': 'Recording label for the album',
    },
    'license': {
        'label': 'License',
        'description': 'License for the track',
    },
    'copyright': {
        'label': 'Copyright',
        'description': 'Copyright owner for the track',
    },
    'sort_album_artist': {
        'label': 'Sort Album Artist',
        'description': 'Album artist name used for sorting',
    },
    'sort_artist': {
        'label': 'Sort Artist',
        'description': 'Artist name used for sorting',
    },
    'sort_composer': {
        'label': 'Sort Composer',
        'description': 'Composer name used for sorting',
    },
    'sort_performers': {
        'label': 'Sort Performers',
        'description': 'Performers used for sorting',
    },
    'sort_show': {
        'label': 'Sort Show',
        'description': 'Show name used for sorting',
    },
    'sort_album': {
        'label': 'Sort Album',
        'description': 'Album name used for sorting',
    },
    'sort_title': {
        'label': 'Sort Title',
        'description': 'Title used for sorting',
    },
}