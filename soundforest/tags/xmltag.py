"""
XML schema representation of audio file metadata tags
"""

from lxml import etree as ET
from lxml.etree import XMLSyntaxError
from lxml.builder import E

from soundforest.tags.constants import parsedate

class XMLTagError(Exception):
    pass

XML_EXPORT_FIELDS = [
    'path',
    'album_artist',
    'sort_artist',
    'artist',
    'sort_album',
    'album',
    'sort_title',
    'title',
    'genre',
    'bpm',
    'year',
    'tracknumber',
    'comment',
    'composer',
    'copyright',
    'xid',
]

def XMLTrackNumberField(details):
    if details.has_key('totaltracks'):
        node = E('tracknumber',
            track=details['tracknumber'][0],
            total=details['totaltracks'][0]
        )
    else:
        node = E('tracknumber',
            track=details['tracknumber'][0],
        )
    return node

def XMLTrackYear(details):
    nodes = []
    for value in details['year']:
        value = parsedate(value)

        if value is None:
            continue
        nodes.append(E('year', '{0:d}'.format(value.tm_year)))
    return nodes

XML_FIELD_CLASSES = {
    'tracknumber': XMLTrackNumberField,
    'year': XMLTrackYear,
}

class XMLTags(dict):
    def __init__(self, data):
        self.tree = E('track')
        if not isinstance(data, dict):
            raise XMLTagError('Details must be dictionary')

        for k in XML_EXPORT_FIELDS:
            if k not in data.keys():
                continue

            if k in XML_FIELD_CLASSES.keys():
                nodes = XML_FIELD_CLASSES[k](data)
                if nodes is not None:
                    for node in nodes:
                        self.tree.append(node)

            else:
                for v in data[k]:
                    self.tree.append(E(k, v))

    def toxml(self):
        return ET.tostring(self.tree, pretty_print=True)


class XMLTrackTree(object):
    def __init__(self):
        self.tracks =  E('tracks')
        self.tree = E('soundforest', self.tracks)

    def append(self, xmltags):
        if not isinstance(xmltags, XMLTags):
            raise XMLTagError('xmltags must be XMLTags instance')
        self.tracks.append(xmltags.tree)

    def tostring(self):
        self.tracks.set('total', '{0:d}'.format(len(self.tracks)))
        return ET.tostring(self.tree, pretty_print=True)

