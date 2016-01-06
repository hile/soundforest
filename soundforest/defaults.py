# coding=utf-8
"""Soundforest default settings

Default settings for configuration database and commands.

"""

import sys
import os

if sys.platform == 'darwin':
    SOUNDFOREST_USER_DIR = os.path.expanduser('~/Library/Application Support/Soundforest')
    SOUNDFOREST_CACHE_DIR = os.path.expanduser('~/Library/Caches/Soundforest')
else:
    SOUNDFOREST_USER_DIR = os.path.expanduser('~/.config/soundforest')
    SOUNDFOREST_CACHE_DIR = os.path.expanduser('~/.cache/soundforest')

DEFAULT_TREE_TYPES = {
  'songs': 'Full song audio files',
  'loops': 'Audio loops',
  'crates': 'Folders of exported audio files',
  'samples': 'Audio samples',
  'scratch': 'Scratch samples',
  'ringtones': 'Phone Ringtones',
  'notifications': 'Phone Notifications',
}

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

  'm4a': {
    'description': 'Advanced Audio Coding',
    'extensions': ['m4a', 'aac', 'mp4'],
    'encoders': [
      'afconvert -b 256000 --soundcheck-generate -f m4af -d aac FILE OUTFILE',
      'neroAacEnc -if FILE -of OUTFILE -br 256000 -2pass',
    ],
    'decoders': [
      'afconvert -f WAVE -d LEI16 FILE OUTFILE',
      'neroAacDec -if OUTFILE -of FILE',
      'faad -q -o OUTFILE FILE -b1',
    ],
    'testers': [
      'afconvert -f WAVE -d LEI16 FILE OUTFILE',
    ]
  },

  'm4r': {
    'description': 'AAC Alert/Ringtone for iOS',
    'extensions': ['m4r'],
    'encoders': [
      'afconvert -b 256000 --soundcheck-generate -f m4af -d aac FILE OUTFILE',
      'neroAacEnc -if FILE -of OUTFILE -br 256000 -2pass',
    ],
    'decoders': [
      'afconvert -f WAVE -d LEI16 FILE OUTFILE',
      'neroAacDec -if OUTFILE -of FILE',
      'faad -q -o OUTFILE FILE -b1',
    ],
  },

  'ogg': {
    'description': 'Ogg Vorbis',
    'extensions': [ 'ogg', 'oga', ],
    'encoders': [
      'oggenc --quiet -q 7 -o OUTFILE FILE',
    ],
    'decoders': [
      'oggdec --quiet -o OUTFILE FILE',
    ],
  },

  'opus': {
    'description': 'Opus in vorbis container',
    'extensions': [ 'opus', ],
    'encoders': [
      'opusenc--quiet FILE OUTFILE',
    ],
    'decoders': [
      'opusdec --quiet FILE OUTFILE',
    ],
  },

  'alac': {
    'description': 'Apple Lossless Codec',
    'extensions': ['alac', 'm4a'],
    'encoders': [
      'afconvert -d alac FILE OUTFILE',
    ],
    'decoders': [
      'afconvert -f WAVE -d LEI16 FILE OUTFILE',
    ],
  },

  'flac': {
    'description': 'Free Lossless Audio Codec',
    'extensions': ['flac'],
    'encoders': [
      'flac -f --silent --verify --replay-gain --best -o OUTFILE FILE',
    ],
    'decoders': [
      'flac -f --silent --decode -o OUTFILE FILE',
    ],
    'testers': [
      'flac --silent --test FILE',
    ]
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

  'wav': {
      'description': 'RIFF Wave Audio',
      'extensions':   ['wav'],
      'encoders': [
        'afconvert -f WAVE -d LEI16 FILE OUTFILE',
      ],
      'decoders': [
        'cp FILE OUTFILE',
      ],
  },

}
