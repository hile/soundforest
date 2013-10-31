# coding=utf-8
"""CLI utilities

Command line utilities for soundforest

"""

import sys
import os
import fnmatch
import time
import logging
import argparse
import tempfile
import signal
import socket
import threading
import subprocess

from setproctitle import setproctitle
from soundforest.config import ConfigDB
from soundforest.log import SoundforestLogger

def xterm_title(value, max_length=74, bypass_term_check=False):
    """
    Set title in xterm titlebar to given value, clip the title text to
    max_length characters.
    """
    TERM=os.getenv('TERM')
    TERM_TITLE_SUPPORTED = [ 'xterm', 'xterm-debian']
    if not bypass_term_check and TERM not in TERM_TITLE_SUPPORTED:
        return
    sys.stderr.write('\033]2;'+value[:max_length]+'',)
    sys.stderr.flush()

class ScriptError(Exception):
    pass

class ScriptThread(threading.Thread):
    """
    Common script thread base class
    """
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.log = SoundforestLogger().default_stream
        self.status = 'not running'
        self.setDaemon(True)
        self.setName(name)

    def execute(self, command):
        p = subprocess.Popen(command, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr)
        return p.wait()

class ScriptThreadManager(list):
    def __init__(self, name, threads=None):
        self.log = SoundforestLogger().default_stream
        self.db = ConfigDB()
        if threads is None:
            threads = self.db.get('threads')
            if threads is None:
                threads = 1
        else:
            threads = int(threads)
        self.threads = threads 

    def get_entry_handler(self, entry):
        raise NotImplementedError('Must be implemented in child class')

    def run(self):
        raise NotImplementedError('Must be implemented in child class')


class Script(object):
    """
    Common CLI tool setup class
    """
    def __init__(self, name=None, description=None, epilog=None, debug_flag=True, subcommands=True):
        self.db = ConfigDB()
        self.name = os.path.basename(sys.argv[0])

        reload(sys)
        sys.setdefaultencoding('utf-8')

        setproctitle('%s %s' % (self.name, ' '.join(sys.argv[1:])))
        signal.signal(signal.SIGINT, self.SIGINT)

        if name is None:
            name = self.name

        self.logger = SoundforestLogger()
        self.log = self.logger.default_stream

        self.parser = argparse.ArgumentParser(
            prog=name,
            description=description,
            epilog=epilog,
            add_help=True,
            conflict_handler='resolve',
        )
        if debug_flag:
            self.parser.add_argument('--debug', action='store_true', help='Show debug messages')

        if subcommands:
            self.commands = {}
            self.command_parsers = self.parser.add_subparsers(
                dest='command',
                help='Please select one command mode below',
                title='Command modes'
            )

    def SIGINT(self, signum, frame):
        """
        Parse SIGINT signal by quitting the program cleanly with exit code 1
        """
        for t in filter(lambda t: t.name!='MainThread', threading.enumerate()):
            t.join()
        self.exit(1)

    def wait(self, poll_interval=1):
        """
        Wait for running threads to finish.
        Poll interval is time to wait between checks for threads
        """
        while True:
            active = filter(lambda t: t.name!='MainThread', threading.enumerate())
            if not len(active):
                break
            time.sleep(poll_interval)

    def exit(self, value=0, message=None):
        """
        Exit the script with given exit value.
        If message is not None, it is printed on screen.
        """
        if message is not None:
            self.message(message)

        while True:
            active = filter(lambda t: t.name!='MainThread', threading.enumerate())
            if not len(active):
                break
            time.sleep(1)
        sys.exit(value)

    def message(self, message):
        sys.stdout.write('%s\n' % message)

    def error(self, message):
        sys.stderr.write('%s\n' % message)

    def register_subcommand(self, command, name, description, epilog=None):
        if name in self.commands:
            raise ScriptError('Duplicate sub command name: %s' % name)
        self.commands[name] = command
        return self.command_parsers.add_parser(
            name,
            help=description,
            description=description,
            epilog=epilog
        )

    def add_argument(self, *args, **kwargs):
        """
        Shortcut to add argument to main argumentparser instance
        """
        self.parser.add_argument(*args, **kwargs)

    def parse_args(self):
        """
        Call parse_args for parser and check for default logging flags
        """
        args = self.parser.parse_args()
        if hasattr(args, 'debug') and getattr(args, 'debug'):
            self.logger.set_level('DEBUG')
        return args

class ScriptCommand(object):
    """
    Parent class for cli subcommands
    """
    def __init__(self, script, name, description, mode_flags=[], epilog=None, debug=True):
        self.name = name
        self.script = script

        self.logger = SoundforestLogger()
        self.log = self.logger.default_stream

        if not isinstance(mode_flags, list):
            raise ScriptError('Mode flags must be a list')
        self.mode_flags = mode_flags
        self.selected_mode_flags = []

        self.parser = script.register_subcommand(self, name, description, epilog)
        if debug:
            self.parser.add_argument('--debug', action='store_true', help='Debug messages')

    @property
    def db(self):
        return self.script.db

    def add_argument(self, *args, **kwargs):
        self.parser.add_argument(*args, **kwargs)

    def exit(self, *args, **kwargs):
        self.script.exit(*args, **kwargs)

    def message(self, *args, **kwargs):
        self.script.message(*args, **kwargs)

    def parse_args(self, args):
        """
        Common argument parsing
        """
        xterm_title('soundforest %s' % (self.name))
        if hasattr(args, 'debug') and getattr(args, 'debug'):
            self.logger.set_level('DEBUG')

        self.selected_mode_flags = filter(lambda x:
            getattr(args, x) not in [None, False, []],
            self.mode_flags
        )
        return args

    def match_path(self, path, matches=[]):
        for m in matches:
            m_realpath = os.path.realpath(m)
            t_realpath = os.path.realpath(path)

            if fnmatch.fnmatch(path, m):
                return True

            if path[:len(m)] == m:
                return True

            if t_realpath[:len(m_realpath)] == m_realpath:
                return True

        return False
