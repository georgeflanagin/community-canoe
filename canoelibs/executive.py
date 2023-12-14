# -*- coding: utf-8 -*-
""" 
Plugin template.
"""

import typing
from   typing import *

# System imports

import argparse
import base64
import math
import os
import os.path
import signal
import sys
import time

# Installed imports

from urutils import getproctitle, setproctitle

# Canoe imports

import canoedb as cdb
import canoestats
from   grammar import *
import importlib as il
import urpacker
import tombstone as tomb
import urutils as uu

if uu.in_production():
    from urdecorators import show_exceptions_and_frames as trap
else:
    from urdecorators import null_decorator as trap

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2019, University of Richmond'
__credits__ = None
__version__ = '0.9'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'testable'

__license__ = 'MIT'
import license

# Use the Real Time signals as our set of waitable signals.
waitable_signals = set(range(signal.SIGRTMIN+1, signal.NSIG))
executive_mod_on = os.stat(__file__).st_mtime

class Executive:
    """ 
    Execute the recipe. 
    """

    @trap
    def __init__(self, recipe:object, sn:int = -1) -> None:
        """
        Execute the program we are given.

        recipe -- a collection of opcodes to execute.
        sn     -- if it has already been assigned, othewise we 
                    will get a new one.
        """

        self.this_commit = uu.version(full=False)

        self.proc       = getproctitle()
        self.sn         = sn
        self.db         = None
        self.r          = recipe
        self.wait_until = None
        self.verbose    = os.isatty(0)
        self.start_time = time.time()

        self.nag_map = {
            ERROR_ACTION.stop       :0,
            ERROR_ACTION.proceed    :0,
            ERROR_ACTION.notify     :1,
            ERROR_ACTION.skip       :2,
            ERROR_ACTION.cleanup    :0,
            ERROR_ACTION.retry      :2,
            ERROR_ACTION.test_empty :0
            }

        self.jump_table = {
            ERROR_ACTION.stop       :[self.stop],
            ERROR_ACTION.proceed    :[self.proceed],
            ERROR_ACTION.notify     :[self.notify],
            ERROR_ACTION.skip       :[self.skip],
            ERROR_ACTION.cleanup    :[self.cleanup],
            ERROR_ACTION.retry      :[self.notify],
            ERROR_ACTION.test_empty :[self.test_empty]
            }

        recipe_commit = self.r.compiler_info[0]

        try:
            os.environ['recipe'] = self.r.name
            os.environ[self.r.name] = self.r.this_dir
            os.environ['sn'] = str(self.sn) if self.sn != -1 else uu.new_serial_number()
            self.sn = os.environ['sn']
            self.db = cdb.default()
            tomb.tombstone('database is open')

        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            raise


    def __bool__(self) -> bool:
        """
        Answer the question "Is a recipe attached?"
        """
        return self.r != uu.SloppyDict()


    @trap
    def plugin_name(self, section_name:str) -> str:
        """
        Return the section_name without the numerical suffix on the end.
        """
        try:
            location = section_name.rindex('_')
            plugin_name = section_name[:location]
            return plugin_name if plugin_name in ITERABLE_SECTIONS else section_name

        except ValueError as e:
            return section_name
    


    @trap
    def exec(self, strict:bool=True) -> None:
        """ execute the attached program, and return

        exec() 
         - changes the name of the process to match the task
         - gets a serial number
         - writes a begin-job record to the DB
         - executes the task
         - writes an end-job record to the DB
        """

        global waitable_signals
        global executive_mod_on
        signal.pthread_sigmask(signal.SIG_BLOCK, waitable_signals)

        # Get me a serial number, and let's go for it.
        setproctitle("{}:{}".format(self.r.name, self.sn))
        tomb.tombstone(f"{self.r.name} is s/n {self.sn}")

        tomb.tombstone('begin-job')

        # new_integration() contains an INSERT OR IGNORE statement.
        stats = canoestats.default()
        stats.new_integration(self.r.name, self.r.get('frequency', '?'))
        stats.new_execution(self.r.name, self.sn)
        uu.make_dir_or_die(self.r.this_dir)    

        self._log("{} begins".format(getproctitle()))

        ###
        # A little sanity checking.
        #
        #  [1] if the recipe's source code is available, and has been modified
        #      after the most recent compilation, it should be recompiled to 
        #      pick up the most recent changes.
        #  [2] if the executive (this module) itself has been modified, then 
        #      the recipe should be recompiled because of possible incompatibility.
        # 
        # The test program at the bottom sets strict=False so that you can test
        #  and tinker without messing up things. In production, we stop. We always
        #  check and print the message, regardless.
        ###

        compiled_on = self.r.compiled_time[0]
        try:
            source_mod_on = os.stat(self.r.origin).st_mtime
        except:
            source_mod_on = 0

        result = math.nan

        ###
        # This is the main event loop for the IJKL interpreter. An IJKL
        # program consists of 
        ###
        try:    
            for name in self.r.roster:
                if self.verbose: tomb.tombstone(f"Found {name=} instruction.")
                self._log("{}:{}".format(getproctitle(), name))
                time.sleep(0.1)
                if (self.wait_until is not None and name != self.wait_until): 
                    if self.verbose: tomb.tombstone("skipping because of previous return code.")
                    continue   

                opcodes = self.r[name]
                tomb.tombstone_comments(opcodes)
                plugin_type = self.plugin_name(name)
                uu.tombstone(f"{name=} -> {plugin_type=}")
                plugin_module = il.import_module(plugin_type)
                tomb.tombstone(f"{name} begins.")
                
                if self.verbose: tomb.tombstone(f"invoking {plugin_type=} plugin")
                result = getattr(plugin_module, f"{plugin_type}_main")(opcodes)
                try:
                    # NOTE: This is the name of the value in the enumeration of return codes, so
                    # something like 'proceed', 'cleanup', etc.
                    code = result.name
                except:
                    code = ERROR_ACTION.cleanup
                self._log(f"{getproctitle()}:{name} returned {code}")

                # Figure out what to do next, and bail out on ERROR_ACTION.stop
                for foo in self.jump_table.get(result, [self.runtime_error]):
                    if foo() is ERROR_ACTION.stop: break

        finally:
            
            if self.r.affirm and result is not ERROR_ACTION.notify:
                try:
                    uu.dorunrun(self.r.affirmation, timeout=5)
                    uu.tombstone("Affirmation email sent.")

                except Exception as e:
                    uu.tombstone("Affirmation email failed to send.")    

            else:
                uu.tombstone("Affirmation email not requested.")

            self._log(f'{self.sn} {self.r.name} completed.')
            self._nag(self.nag_map.get(result, 3))
            tomb.tombstone('end-job')

        return 


    @trap
    def _log(self, msg:str) -> bool:
        """
        Put a message in the logging queue.

        msg -- anything you want to say.

        returns -- True if the operation succeeded; False otherwise.
        """
        tomb.tombstone(msg)


    @trap
    def _nag(self, code:int, msg:str="") -> bool:
        """
        Let Nagios know what happened.

        code -- one of Nagios's allowed values.

        returns -- True if the operation succeeded; False otherwise.
        """
        return True
        # opts = {'CODE':'nag', 'PRIORITY':code, 'SHORT_EVENT':self.r.name,
        #    'LONG_EVENT':" ".join([getproctitle(), str(self.sn), msg])}
        #msgid = self.db.q_insert(self.nag_queue, **opts)
        # if self.verbose and msgid is not None: 
        #    print('msg id {} to nagging queue'.format(msgid))
        #return msgid is not None
        

    @trap
    def runtime_error(self) -> None:
        """
        This fuction should not be callable.
        """
        raise Exception('Runtime exception')


    def stop(self) -> ERROR_ACTION:
        """
        This function is the non-error, stop.
        """
        tomb.tombstone('{}, s/n {} is complete.'.format(self.r.name, self.sn))
        sys.exit(os.EX_OK)


    def proceed(self) -> ERROR_ACTION:
        return ERROR_ACTION.proceed


    def notify(self) -> ERROR_ACTION:
        self.wait_until = 'notify'
        self.r.mailto = self.r.mailto.format(self.sn)
        try:
            uu.dorunrun(self.r.mailto, timeout=5)
            tomb.tombstone(f"notification succeeded.")

        except Exception as e:
            tomb.tombstone(f"notification failed {self.r.mailto}")

        return ERROR_ACTION.proceed


    def skip(self) -> ERROR_ACTION:
        tomb.tombstone('Logic error. {} should not be returned.'.format(ERROR_ACTION.skip.name))
        return ERROR_ACTION.stop


    def cleanup(self) -> ERROR_ACTION:
        self.wait_until = 'cleanup'
        return ERROR_ACTION.proceed


    def test_empty(self) -> ERROR_ACTION:
        tomb.tombstone('Logic error. {} should not be returned.'.format(ERROR_ACTION.test_empty.name))
        return ERROR_ACTION.proceed


if __name__ == '__main__':
    """
    Universal test program for all Canøe plugins.
    """
    this_commit = uu.version(full=False)
    mod_time = os.stat(__file__).st_mtime

    if len(sys.argv) < 2: 
        tomb.tombstone("\nUsage: executive.py {compiledrecipe[.jsc]}")
        sys.exit(os.EX_DATAERR)

    if not sys.argv[-1].endswith('.jsc'): sys.argv[-1]+='.jsc'

    f = ( sys.argv[-1] if sys.argv[-1].startswith(os.sep) else 
          os.path.join(os.environ.get('COMPILEDRECIPES', '/sw/canoe/compiledrecipes'),
            sys.argv[-1])
        )

    loader = urpacker.URpacker()
    loader.attachIO(f, s_mode='read')
    opcodes = loader.read()

    uu.tombstone(f"{sys.argv[-1]} loaded.")

    tomb.tombstone("\n")
    tomb.tombstone("Executive version {}".format(this_commit))
    tomb.tombstone("Compiled on       {}".format(mod_time))
    tomb.tombstone("\n")
    tomb.tombstone("Compiler version {}".format(uu.compiler_info(opcodes)))
    tomb.tombstone("Compiled on      {}".format(uu.compiled_time(opcodes)))
    tomb.tombstone(80*'-')

    e = Executive(opcodes)
    e.exec(strict='--strict' in sys.argv)
