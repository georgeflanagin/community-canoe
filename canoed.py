#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from typing import *

""" 
This is the main routine of canoed. It is intended to be launched 
with nohup.
"""

# Credits
__author__ = 'George Flanagin, Douglas Broome'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.4'
__maintainer__ = 'George Flanagin, Douglas Broome'
__email__ = 'gflanagin@richmond.edu, dbroome@richmond.edu'
__status__ = 'Working Prototype'
__required_version__ = (3, 6)

__license__ = 'MIT'
import license

# Standard imports

import argparse
import cx_Oracle
import datetime
import errno
import hashlib
import os
import sys

# Canøe imports

import canoedb as cdb
import canoeconfig as cc
import canoecrypter as cxx
import canoeenv
import executive
import fifo
import importlib
import jparse as jp
import loader
from   reanimator import ReAnimator
from   urutils import setproctitle, getproctitle
import signal
import time
import tombstone as tomb
from   urdecorators import show_exceptions_and_frames as trap
import urutils as uu

#####################################
############# BEGIN #################
#####################################

canoe_packages = [ 'canoelibs', 'urlib', 'plugins' ]
code = ReAnimator(canoe_packages)
code.load_code()

caught_signals = [  signal.SIGINT, signal.SIGQUIT, signal.SIGHUP,
                    signal.SIGUSR1, signal.SIGUSR2, signal.SIGRTMIN+1 ]

@trap
def handler(signum:int, stack:object=None) -> None:
    """
    Universal signal handler.
    """

    if signum in [ signal.SIGHUP, signal.SIGUSR1 ]: 
        tomb.tombstone('Rereading all configuration files.')
        canoed()

    elif signum in [ signal.SIGUSR2, signal.SIGQUIT, signal.SIGINT ]:
        tomb.tombstone('Closing up.')
        uu.fclose_all()
        sys.exit(os.EX_OK)

    elif signum == signal.SIGRTMIN+1:
        tomb.tombstone('Reloading code modules.')
        i, j = code.reload_code()
        tomb.tombstone('{} modules reloaded; {} new modules loaded.'.format(j, i))
        canoed()        

    else:
        tomb.tombstone(
            "ignoring signal {}. Check list of handled signals.".format(signum)
            )


### Globals to preserve state on a restart. 

pipe_name = None
known_events = ['run','alert']
available_cpus = len(os.sched_getaffinity(0))
retry_counter = 0
max_retries = 0

@trap
def canoed(pipe_affinity:str='') -> None: 
    """ 
    This is the daemon's main routine. 
    """

    import executive
    """
    We are going nohup, so there are no demonic preliminaries to get
    out of the way. This function is the "main" for the daemon that
    polls the event pipe and then executes any instructions it finds.

    pipe_affinity -- the name of the pipe to use. If omitted, then 
        we will use the most recent pipe created. This is the correct
        action for production.
    init -- recompile all the recipes.
    """

    global pipe_name, known_events
    global available_cpus, retry_counter, max_retries

    if pipe_name is None:
        # First time through, only.
        pipe_name = pipe_affinity

    try:
        pipe = fifo.FIFO(pipe_name, 'non_block')
    except Exception as e:
        uu.tombstone(uu.type_and_text(e))
        tomb.tombstone(f'unable to use {pipe_name}')
        sys.exit(os.EX_NOINPUT)

    tomb.tombstone('THIS IS CANOE 20!')
    tomb.tombstone(f'Reading events from the pipe named {pipe_name}.')

    # Our name will reflect the pipe from which we are reading.
    setproctitle('canoed:' + pipe_name)

    sn = uu.new_serial_number()
    tomb.tombstone("Serial number {} issued to the canoed process.".format(sn))

    COMPILEDRECIPES = os.environ.get('COMPILEDRECIPES', '/sw/canoe/compiledrecipes')
    count, programs = loader.loader(COMPILEDRECIPES)
    if count < 0:
        tomb.tombstone(f"There was a problem loading the programs: {-count}")
        sys.exit(os.EX_DATAERR)

    elif count == 0:
        tomb.tombstone(f"No programs found in {COMPILEDRECIPES} to run.")
        sys.exit(os.EX_CONFIG)

    else:
        tomb.tombstone(f"Loaded {count} compiled programs to run.")

    if count <= 0: sys.exit(os.EX_CONFIG)

    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    tomb.tombstone("Child signals will be ignored.")

    # We put the main event loop in a try block so that we can execute
    # the finally block at the end. To ensure that we do not raise 
    # additional exceptions, we introduce all the names used inside
    # the try block.
    db = cdb.default()
    events = []
    event = ''
    pid = 0
    sn = 0
    guido = None
    db_timeout = 7200
    db_connected = time.time()

    # zero is the least nice a non-priv process can be. The function returns
    # the current niceness.
    my_niceness = os.nice(0)

    stop_after = False
    try:
        parent_exit = os.EX_OK # I try to be optimistic.
        while not stop_after:
            # This is the beginning of the main event loop. The only way
            # to leave is via an Exception.

            try:
                # Get anything in the pipe.
                events = pipe(60*60*24) # All day, if necessary.
                if len(events): uu.tombstone(f'read {events} from pipe.')

                for name in [_.strip() for _ in events]:
                    stop_after = name == 'stop'
                    if stop_after: 
                        tomb.tombstone("Read shutdown instruction. Stopping after pipe is emptied.")

                    # Put the brakes on a run away process that has loaded up the 
                    # pipe. For example, if we have been offline for a while, then
                    # the pipe could have several hundred events in it.
                    while os.getloadavg()[0] > available_cpus:
                        uu.tombstone('too busy {}.'.format(os.getloadavg()))
                        time.sleep(10)

                    # Run each integration in its own process. According to the 
                    # multiprocessing module's documentation, an OSError is
                    # raised if fork fails -- a slight difference from 
                    # `man 2 fork`.
                    try:
                        time.sleep(4)
                        pid = os.fork()
                    except OSError as e:
                        raise Exception("fork failed!") from e

                    if not pid:  
                        uu.tombstone(f'Child process {os.getpid()} begun.')
                        ###
                        # Behold, a child is born. It needs its own try-block
                        # to handle its exceptions because we need to keep it
                        # from bubbling up and having an indefinite life span
                        # as a copy of canoed that reads the pipe.
                        ###
                        # Each niceness level is 10%, so this makes our child 
                        # twice as nice as the parent.
                        os.nice(my_niceness+6)
                        child_exit = os.EX_OK

                        try:
                            # Change our name so that we show up correctly in ps
                            setproctitle(getproctitle() + ':' + name)

                            # The accumulator counts the steps of execution in 
                            # the process. If the parent has been running a while
                            # it is up in the many thousands; we want zero. 
                            uu.Accumulator.reset()

                            # Let any canoetop procs know that we are running.
                            # for _ in uu.pids_of('canoetop', True): 
                            #     os.kill(_, signal.SIGUSR1)

                            # An executive is a module that executes a recipe. So we
                            # build one, get the recipe, resolve any bindings, and
                            # then attach and run it. Straightforward.
                            # Canøe will attempt to repurpose an already open database
                            # connection.

                            sn = uu.new_serial_number()

                            tomb.tombstone(f'Child s/n: {sn} assigned to task {name}')

                            opcodes = programs.get(name)
                            if opcodes is None:
                                tomb.tombstone(f'No opcodes found for {name}')
                                os._exit(os.EX_NOINPUT)

                            guido = executive.Executive(programs[name], sn)
                            guido.exec()


                        except Exception as e:

                            child_exit = os.EX_OSERR
                            tomb.tombstone('Terminal error in child process.')
                            uu.fatalerror()
                
                        finally:
                            uu.tombstone(f'Child process {os.getpid()} ended.')
                            # Always hang up your boots.
                            os._exit(child_exit)

                    elif pid > 0: 
                        ###
                        # Parent code.
                        ###
                        # Write down what we did. 
                        uu.tombstone(f"Forked: {name}")
                        continue

                    elif pid < 0:
                        ###
                        # This is not really the child's pid; it is the negative of
                        # the signal number that killed the child process.
                        ###
                        e = errno.errorcode
                        tomb.tombstone("child terminated by signal {}.".format(-1*pid))

                ########## ACHTUNG! ####################################
                # This continue statement jumps up to the beginning    #
                # of the >while True< loop. It is outside the 'for'.   #
                ########################################################
                # tomb.tombstone('continuing w/o catching an exception.')
                continue

            except OSError as e:
                tomb.tombstone(uu.type_and_text(e))
                parent_exit = os.EX_OSERR
                raise

            except Exception as e:
                parent_exit = os.EX_OSERR
                tomb.tombstone(str(e))
                raise 

            else:
                # Again, this is back to >while True<
                tomb.tombstone('continuing via else')
                continue 

        # End of WHILE
        if stop_after:
            tomb.tombstone('stopping as instructed.')

    except Exception as e:
        tomb.tombstone(f"Exception in outer block: {str(e)}")

    finally:
        tomb.tombstone('exiting via finally.')
        sys.exit(parent_exit)


if __name__ == '__main__':
    # If we are running the daemon from the console, then we 
    # want control-C from the keyboard to work.
    if os.isatty(0):
        caught_signals.remove(signal.SIGINT) 
        caught_signals.remove(signal.SIGHUP) 
    for _ in caught_signals:
        try:
            signal.signal(_, handler)
        except OSError as e:
            uu.tombstone('cannot reassign signal {}'.format(_))
        else:
            uu.tombstone('signal {} is being handled.'.format(_))

    parser = argparse.ArgumentParser()
    parser.add_argument('--pipe', type=str, required=True)
    my_args = parser.parse_args()
    try:
        canoed(my_args.pipe)

    except KeyboardInterrupt:
        ###
        # Obviously (I hope) this only works when you are running this
        # program in test mode, interactively. If SIGINT is sent to 
        # the handler, then this block of code is never executed.
        ###
        print("Whoa! You asked to exit.")
        sys.exit(os.EX_OK)

else:
    pass
