"""Albumart tags

Abstraction for album art image format processing

"""

import os
import requests
from io import BytesIO

from PIL import ImageFile

DEFAULT_ARTWORK_FILENAME = 'artwork.jpg'

PIL_EXTENSION_MAP = {
    'JPEG':     'jpg',
    'PNG':      'png',
}

PIL_MIME_MAP = {
    'JPEG':     'image/jpeg',
    'PNG':      'image/png',
}


class AlbumArtError(Exception):
    pass


class AlbumArt(object):
    """
    Class to parse albumart image files from tags and files
    """

    def __init__(self, path=None):
        self.__image = None
        self.__mimetype = None

        if path is not None:
            self.import_file(path)

    def __repr__(self):
        """
        Returns text description of image type and size
        """

        if not self.is_loaded():
            return 'Uninitialized AlbumArt object.'
        return '%(mime)s %(bytes)d bytes %(width)dx%(height)d' % self.get_info()

    def __unicode__(self):
        """
        Returns file format and size as unicode string
        """

        if not self.is_loaded():
            return u'Uninitialized AlbumArt object'
        return '%(mime)s %(width)dx%(height)dpx' % self.get_info()

    def __len__(self):
        """
        Returns PIL image length as string
        """

        if not self.is_loaded():
            return 0
        return len(self.__image.tostring())

    def __parse_image(self, data):
        """
        Load the image from data with PIL
        """

        try:
            parser = ImageFile.Parser()
            parser.feed(data)
            self.__image = parser.close()
        except IOError:
            raise AlbumArtError('Error parsing albumart image data')

        try:
            self.__mimetype = PIL_MIME_MAP[self.__image.format]
        except KeyError:
            self.__image = None
            raise AlbumArtError('Unsupported PIL image format: {}'.format(
                self.__image.format,
            ))

        if self.__image.mode != 'RGB':
            self.__image = self.__image.convert('RGB')

    def import_data(self, data):
        """
        Import albumart from metadata tag or database as bytes
        """
        self.__parse_image(data)

    def import_file(self, path):
        """
        Import albumart from file
        """
        if not os.path.isfile(path):
            raise AlbumArtError('No such file: {}'.format(path))
        if not os.access(path, os.R_OK):
            raise AlbumArtError('No permissions to read file: {}'.format(
                path,
            ))

        self.__parse_image(open(path, 'r').read())

    def is_loaded(self):
        """
        Boolean test to see if album art image is loaded
        """
        return self.__image is not None

    def get_fileformat(self):
        """
        Return file format of loaded album art image
        """
        if not self.is_loaded():
            raise AlbumArtError('AlbumArt not yet initialized.')
        return self.__image.format

    def get_info(self):
        """
        Return details of loaded album art image
        """
        if not self.is_loaded():
            raise AlbumArtError('AlbumArt not yet initialized.')
        colors = self.__image.getcolors()
        if colors is None:
            colors = 0
        return {
            'type': 3,  # Album cover
            'mime': self.__mimetype,
            'bytes': len(self),
            'width': int(self.__image.size[0]),
            'height': int(self.__image.size[1]),
            'colors': colors,
        }

    def dump(self):
        """
        Returns bytes from the image with BytesIO   read() call
        """
        if not self.is_loaded():
            raise AlbumArtError('AlbumArt not yet initialized.')

        s = BytesIO()
        self.__image.save(s, self.get_fileformat())
        s.seek(0)
        return s.read()

    def save(self, path, fileformat=None):
        """
        Saves the image data to given target file.

        If target filename exists, it is removed before saving.
        """
        if not self.is_loaded():
            raise AlbumArtError('AlbumArt not yet initialized.')

        if fileformat is None:
            fileformat = self.get_fileformat()

        if os.path.isdir(path):
            path = os.path.join(path, DEFAULT_ARTWORK_FILENAME)

        if os.path.isfile(path):
            try:
                os.unlink(path)
            except IOError as e:
                raise AlbumArtError('Error removing existing file {}: {}'.format(
                    path,
                    e,
                ))

        try:
            self.__image.save(path, fileformat)
        except IOError as e:
            raise AlbumArtError('Error saving {}: {}'.format(
                path,
                e,
            ))

    def fetch(self, url):
        res = requests.get(url)
        if res.status_code != 200:
            raise AlbumArtError('Error fetching url {} (returns {})'.format(
                url,
                res.status_code,
            ))

        if 'content-type' not in res.headers:
            raise AlbumArtError('Response did not include content type header')

        try:
            content_type = res.headers.get('content-type', None)
            if not content_type:
                raise AlbumArtError('Response missing content-type header')
            (prefix, extension) = content_type.split('/', 1)
            if prefix != 'image':
                raise AlbumArtError('Content type of data is not supported: {}'.format(
                    content_type,
                ))

        except ValueError:
            raise AlbumArtError('Error parsing content type {}'.format(
                res.headers.get('content-type', None)
            ))

        return self.import_data(res.content)
