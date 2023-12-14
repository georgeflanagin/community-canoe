# -*- coding: utf-8 -*-
""" 
Plugin support routines to deal with return codes.
"""

import typing
from   typing import *

# System imports

import collections
from   collections.abc import Iterable
import datetime
import json
import os
import os.path
import signal
import sys
import time

# Installed imports

# Canoe imports

from   canoestats import Blinker
from   canoestats import LED
import fname
from   grammar import *
import hop
import tombstone as tomb
import urbox as ux
import urdb
import urpacker
import urutils as uu

debug = not uu.in_production()
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


@trap
def all_files_found(fileset:Union[list, str], expected_number:Iterable) -> bool:
    """
    Trivial function to check to see if we got enough files. This 
    might be in need of fleshing out down the road, and that is the
    reason that it is encapsulated in this function.
    """
    if isinstance(fileset, Iterable):
        num = len(fileset)
    elif isinstance(fileset, str) and os.path.isdir(fileset):
        num = len([name for name in os.listdir(fileset) if os.path.isfile(name)])
    else:
        raise Exception(f"Bad argument type {type(fileset)} for {fileset}")

    return num in expected_number


@trap
def fail_ok(e:ERROR_ACTION) -> bool:
    return e in [
        ERROR_ACTION.proceed,
        ERROR_ACTION.skip,
        ERROR_ACTION.stop,
        ERROR_ACTION.test_empty
        ]

###
# Why have this handler? Why not just sleep? During a long sleep, an hour or
# more in many cases, the process that is waiting might receive a signal
# other than SIGALRM. Many times, we can discard the stray signal and wait
# for the alarm to go off.
#
# We are not using realtime signals.
###
realtime_signals = set(range(signal.SIGRTMIN+1, signal.NSIG)) 

# And we don't care about user defined and child signals.
handle_these_signals = set([
    signal.SIGUSR1, signal.SIGUSR2, 
    signal.SIGCHLD, signal.SIGCONT, 
    signal.SIGQUIT ]) | realtime_signals 

# And if we are running non-interactively, we don't care about 
# SIGINT and SIGHUP.
if not os.isatty(0):
    handle_these_signals.add(signal.SIGINT)
    handle_these_signals.add(signal.SIGHUP) 

# These variables persist during the execution. 
nap_begun = time.time()
sleep_for = 0


@trap
def handler(signal_number:int, frame:object) -> None:
    """
    This is our handler to discard the signals that might wake us
    that are not the alarm going off. There are several ways this
    can happen in a long sleep, and also a common way this can 
    take place in a planned nap. 

    Consider the difference between time.sleep(5) and running the
    Linux program $(sleep 5) as a subprocess. In the first case, 
    the OS sends Python a SIGALRM when the five seconds are up. In
    the second case, the `sleep` program receives a SIGALRM, but 
    sends its parent a SIGCHLD. Wooops.
    """
    global sleep_for, nap_begun
    now = time.time()
    
    uu.tombstone(f"Received signal {uu.signal_name(signal_number)}")

    # If it really is the alarm, that's great. No operations
    # required.
    if signal_number == signal.SIGALRM: return

    # SIGQUIT will stop all Canøe processes by writing 'stop' to the pipe.
    if signal_number == signal.SIGQUIT:
        current_pipe = max(all_files_in(os.environ.get('PIPEDIR'), key=os.path.getmtime))
        with open(current_pipe, 'wb') as f:
            f.write('stop')
        uu.tombstone('I QUIT')
        sys.exit(os.EX_PROTOCOL)

    if signal_number in handle_these_signals:
        if (sleep_for := sleep_for - (now - nap_begun)) <= 0: 
            return
        nap_begun = now
        uu.tombstone(f"Sleeping {sleep_for} more seconds.")
        time.sleep(sleep_for)
        return

    raise Exception("How did we get here?")

# Trap them.
for i in handle_these_signals: signal.signal(i, handler)


@trap
def meets_expectations(opcodes:uu.SloppyDict, blinker:Blinker) -> ERROR_ACTION:
    """
    Based on the information in the opcodes, decided if the
    most recent operation to retrieve data worked as intended.

    opcodes   -- IJKL code.
    returns   -- a returnable ERROR_ACTION
    """
    if not all_files_found(opcodes.local_dir, opcodes.required.count):
        blinker.blink(LED.REQUIRED_FAIL)
        return ( ERROR_ACTION.proceed 
            if fail_ok(opcodes.on_error) else 
            ERROR_ACTION.notify )

    if not test_empty(opcodes.local_dir, opcodes.on_error,
        opcodes.empty.lines, opcodes.empty.bytes, opcodes.empty.whitespace):
        blinker.blink(LED.EMPTY_FAIL)
        return ERROR_ACTION.proceed

    return opcodes.on_error


@trap
def now_as_minutes_of_the_day() -> int:
    """
    NOTE: using datetime.now because time.time gives UTC and we
    want a relevant offset for local time. 
    """
    now = datetime.datetime.now()
    return now.hour*60 + now.minute


# Test empty can be a little confusing.
is_empty_file = True
non_empty_file = not is_empty_file

@trap
def test_empty(
        filename:Union[str, List[str]], 
        e:ERROR_ACTION, 
        num_lines:int=1,
        byte_len:int=10,
        all_whitespace:bool=True) -> bool:
    """
    Look at the file and the action.

    filename -- is what it says it is, or maybe it is a list.
    e        -- the action to take on error.

    returns  -- True: if the filename points to something "empty" or absent 
        when the action is to test_empty
                False: otherwise
    """

    if ERROR_ACTION(e) is not ERROR_ACTION.test_empty: 
        return non_empty_file

    if filename is None: 
        return is_empty_file

    if isinstance(filename, str) and os.path.isdir(filename):
        filename = [name for name in os.listdir(filename) if os.path.isfile(name)]
        return test_empty(filename, e, num_lines, byte_len, all_whitespace)

    elif isinstance(filename, str):
        f = fname.Fname(filename)
        if len(f) > byte_len: 
            return non_empty_file
        data = f()
        if all_whitespace and data:
            return non_empty_file

        return collections.Counter(data)['\n'] <= num_lines

    elif isinstance(filename, list):
        for f in filename:
            if isinstance(f, fname.Fname): f = f.fqn
            if not test_empty(f, e, num_lines, byte_len, all_whitespace): 
                return non_empty_file
        return is_empty_file

    raise Exception(f'Bad arguments {filename}, {e}')
        
    
@trap
def wait_or_give_up(
    blinker:Blinker,
    interval:int=0,
    until:int=-1,
    use:int=0) -> tuple:
    """
    Causes process to suspend for a while so that it can try again.

    interval -- number of minutes to pause. SIGALRM is set for this 
        interval. 

    until -- end time as minutes into the day, [0 .. 1439]. SIGALRM
        is set for this time if now+interval > until. Special values
        are -1, meaning just return.
        in chunks of /interval/ minutes.

    use -- on the initial call, this is used to calculate the value 
        of /until/ if use is zero. After that, it is set to zero. 

    returns -- modified parameters as tuple(interval, until, use)
        if the operation has reached its end, return None.
    """
    global nap_begun, sleep_for

    now = now_as_minutes_of_the_day()
    one_day = 1440

    ###
    # Check to see if use or until is the operational clause.
    # If use > 0, then until is calculated.
    # If use == 0, then until is the relevant boundary.
    ###
    if use > 0:
        until = now + use
        use = 0
    
    if until < 0:
        raise Exception(f"Not sure how this happened {time, until, use}")
    
        
    # See if we are (or have been) waiting across midnight.
    if now > until: 
        until += one_day
    elif until - now > one_day:
        until -= one_day
    else:
        interval = min(interval, until-now)

    blinker.blink(LED.WAITING)
    uu.tombstone(f"Process {os.getpid()} is sleeping for {interval} minutes.")
    nap_begun = time.time()
    sleep_for = interval*60

    time.sleep(sleep_for)
    uu.tombstone(f"Process {os.getpid()} has awakened.")
    then = now
    now = now_as_minutes_of_the_day()
    
    if now >= until:
        blinker.blink(LED.WAIT_EXPIRED)
        return None, None, None
    else:
        blinker.blink(LED.ON)
        return interval, until, use
