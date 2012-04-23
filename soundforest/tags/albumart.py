#!/usr/bin/env python
"""
Abstraction for album art image format processing
""" 

import os,logging,StringIO
from PIL import ImageFile

PIL_EXTENSION_MAP = {
    'JPEG':     'jpg',
    'PNG':      'png',
}

PIL_MIME_MAP = {
    'JPEG':     'image/jpeg',
    'PNG':      'image/png',
}

class AlbumArtError(Exception):
    """
    Exception thrown by errors in file metadata, parameters or 
    file permissiosns.
    """
    def __str__(self):  
        return self.args[0]

class AlbumArt(object):
    """
    Class to parse albumart image files from tags and files
    """
    def __init__(self,path=None):
        self.log = logging.getLogger('modules')
        self.__image = None
        self.__mimetype = None
        if path is not None:
            self.import_file(path)

    def import_data(self,data):
        """
        Import albumart from metadata tag or database as bytes
        """
        self.__parse_image(data)

    def import_file(self,path):
        """
        Import albumart from file
        """
        if not os.path.isfile(path):
            raise AlbumArtError('No such file: %s' % path)
        if not os.access(path,os.R_OK):
            raise AlbumArtError('No permissions to read file: %s' % path)
        self.__parse_image(open(path,'r').read())

    def __parse_image(self,data):
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
            raise AlbumArtError(
                'Unsupported PIL image format: %s' % self.__image.format
            )

        if self.__image.mode != 'RGB':
            self.__image = self.__image.convert('RGB')

    def __repr__(self):
        """
        Returns text description of image type and size
        """
        if self.__image is None:
            return 'Uninitialized AlbumArt object.'

        info = self.info
        return '%s %d bytes %dx%d' % (
            info['mime'],len(info['data']),info['width'],info['height']
        )

    def __getattr__(self,attr):
        """
        Attributes created on the fly and returned:
        image       PIL image
        format      PIL image format
        info         dictinary containing image information:
            type    always 3 (mp3 header type for album cover)
            mime    image mime type
            depth   image bit depth
            width   image width
            height  image height
            colors  always 0
        """
        if attr in ['image','format']:
            if self.__image is None:
                raise AlbumArtError('AlbumArt not yet initialized.')
            if attr == 'image':
                return self.__image
            elif attr == 'format': 
                return self.__image.format
        if attr == 'info':
            if self.__image is None:
                raise AlbumArtError('AlbumArt not yet initialized.')
            colors = self.__image.getcolors()
            if colors is None:
                colors = 0
            if self.path is not None:
                desc = os.path.basename(self.path)
            else:
                desc = 'albumart'
            return {
                'type': 3, # Album cover
                'desc': desc,
                'mime': self.__mimetype,
                'depth': self.__image.bits,
                'width': self.__image.size[0],
                'height': self.__image.size[1],
                'colors': colors,
                'data': self.dump(),
            }
        raise AttributeError('No such AlbumArt attribute: %s' % attr)


    def __unicode__(self):
        """
        Returns file format and size as unicode string
        """
        if self.__image is None:
            return unicode('Uninitialized AlbumArt object')
        return unicode('%s file %d bytes' % (self.format,len(self)))

    def __len__(self):
        """
        Returns PIL image length as string
        """
        return len(self.image.tostring())

    def dump(self):
        """
        Returns bytes from the image with StringIO.StringIO read() call
        """
        if self.__image is None:
            raise AlbumArtError('AlbumArt not yet initialized.')
        s = StringIO.StringIO()
        self.__image.save(s,self.out_format)
        s.seek(0)
        return s.read()

    def save(self,path,out_format=None):
        """
        Saves the image data to given target file.

        If target filename exists, it is removed before saving.
        """
        if out_format is None:
            out_format = self.format
        if os.path.isfile(path):
            try:
                os.unlink(path)
            except IOError,(ecode,emsg):
                raise AlbumArtError(
                    'Error removing existing file %s: %s' % (path,emsg)
                )
        try:
            self.__image.save(path,out_format)
        except IOError,emsg:
            raise AlbumArtError('Error saving %s: %s' % (path,emsg))

