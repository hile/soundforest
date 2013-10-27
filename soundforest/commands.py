"""
Support for various codec programs in soundforest.
"""

import os
import sqlite3
import logging

from subprocess import Popen, PIPE
from systematic.shell import CommandPathCache

# Buffer size for Popen command execution
POPEN_BUFSIZE = 1024

#
# Default codec commands and parameters to register to database.
# NOTE:
#  Changing this dictionary after a codec is registered does NOT
#  register a codec parameters, if the codec was already in DB!
DEFAULT_CODECS = {

  'mp3': {
    'description': 'MPEG-1 or MPEG-2 Audio Layer III',
    'extensions':   ['mp3'],
    'encoders': [
        'lame --quiet -b 320 --vbr-new -ms --replaygain-accurate FILE OUTFILE',
    ],
    'decoders': [
        'lame --quiet --decode FILE OUTFILE',
    ],
  },

  'aac': {
    'description': 'Advanced Audio Coding',
    'extensions': ['aac', 'm4a', 'mp4'],
    'encoders': [
        'neroAacEnc -if FILE -of OUTFILE -br 256000 -2pass',
        'afconvert -b 256000 -v -f m4af -d aac FILE OUTFILE',
    ],
    'decoders': [
        'neroAacDec -if OUTFILE -of FILE',
        'faad -q -o OUTFILE FILE -b1',
    ],
  },

  'vorbis': {
    'description': 'Ogg Vorbis',
    'extensions': ['vorbis','ogg'],
    'encoders': [
        'oggenc --quiet -q 7 -o OUTFILE FILE',
    ],
    'decoders': [
        'oggdec --quiet -o OUTFILE FILE',
    ],
  },

  'flac': {
    'description': 'Free Lossless Audio Codec',
    'extensions': ['flac'],
    'encoders': [
        'flac -f --silent --verify --replay-gain QUALITY -o OUTFILE FILE',
    ],
    'decoders': [
        'flac -f --silent --decode -o OUTFILE FILE',
    ],
  },

  'wavpack': {
    'description': 'WavPack Lossless Audio Codec',
    'extensions': ['wv', 'wavpack'],
    'encoders': [ 'wavpack -yhx FILE -o OUTFILE' ],
    'decoders': [ 'wvunpack -yq FILE -o OUTFILE' ],
  },

  'caf': {
    'description': 'CoreAudio Format audio',
    'extensions':   ['caf'],
    'encoders': [
        'afconvert -f caff -d LEI16 FILE OUTFILE',
    ],
    'decoders': [
        'afconvert -f WAVE -d LEI16 FILE OUTFILE',
    ],
  },

  'aif': {
      'description': 'AIFF audio',
      'extensions':   ['aif', 'aiff'],
      'encoders': [
        'afconvert -f AIFF -d BEI16 FILE OUTFILE',
      ],
      'decoders': [
        'afconvert -f WAVE -d LEI16 FILE OUTFILE',
      ],
  },

  # TODO - Raw audio, what should be decoder/encoder commands?
  'wav': {
      'description': 'RIFF Wave Audio',
      'extensions':   ['wav'],
      'encoders': [],
      'decoders': [],
  },

}

PATH_CACHE = CommandPathCache()
PATH_CACHE.update()

class CodecCommandError(Exception):
    """
    Exceptions raised by CodecCommand in this module
    """
    def __str__(self):
        return self.args[0]

class CodecCommand(object):
    """
    Wrapper to validate and run codec commands from command line.

    A codec command specification must contain special arguments
    FILE and OUTFILE.
    These arguments are replaced with input and output file names
    when run() is called.
    """
    def __init__(self, command):
        self.log = logging.getLogger('modules')
        self.command = command.split()

    def __repr__(self):
        return ' '.join(self.command)

    def validate(self):
        """
        Confirm the codec command contains exactly one FILE and OUTFILE argument
        """
        if self.command.count('FILE')!=1:
            raise CodecCommandError('Command requires exactly one FILE')
        if self.command.count('OUTFILE')!=1:
            raise CodecCommandError('Command requires exactly one OUTFILE')

    def is_available(self):
        """
        Check if the command is available on current path
        """
        return PATH_CACHE.which(self.command[0]) is None and True or False

    def parse_args(self, input_file, output_file):
        """
        Validates and returns the command to execute as list, replacing the
        input_file and output_file fields in command arguments.
        """

        if not self.is_available:
            raise CodecCommandError('Command not found: %s' % self.command[0])
        try:
            self.validate()
        except CodecCommandError, emsg:
            raise CodecCommandError('Error validating codec command: %s' % emsg)

        # Make a copy of self.command, not reference!
        args = [x for x in self.command]
        args[args.index('FILE')] = input_file
        args[args.index('OUTFILE')] = output_file
        return args

    def run(self, input_file, output_file, stdout=None, stderr=None, shell=False):
        """
        Run codec command with given input and output files. Please note
        some command line tools may hang when executed like this!

        If stdout and stderr are not given, the command is executed without
        output. If stdout or stderr is given, the target must have a write()
        method where output lines are written.

        Returns command return code after execution.
        """

        args = self.parse_args(input_file, output_file)
        p = Popen(args, bufsize=POPEN_BUFSIZE, env=os.environ,
            stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=shell
        )

        if stdout is None and stderr is None:
            p.communicate()
            rval = p.returncode
        else:
            rval = None
            while rval is None:
                while True:
                    l = p.stdout.readline()
                    if l=='': break
                    if stdout is not None:
                        stdout.write('%s\n'%l.rstrip())
                while True:
                    l = p.stderr.readline()
                    if l=='': break
                    if stderr is not None:
                        stderr.write('%s\n'%l.rstrip())
                rval = p.poll()

        #noinspection PySimplifyBooleanCheck
        if rval != 0:
            self.log.info('Error executing (returns %d): %s' % (rval, ' '.join(args)))
        return rval
