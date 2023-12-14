# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from typing import *

""" 
Monitor signals.
"""

# Credits
__author__ = 'George Flanagin, Douglas Broome, Virginia Griffith, Ray Cargill'
__copyright__ = 'Copyright 2018, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin, Douglas Broome'
__email__ = 'canoe@richmond.edu'
__status__ = 'Working Prototype'
__required_version__ = (3, 6)

__license__ = 'MIT'
import license

# Standard imports

import os
import signal
import sys
import time

import tombstone as tomb
import urutils as uu
import whocalled

def handler(signum:int, stack:object=None) -> None:
    """
    This is a signal handler. A well behaved Canøe daemon
    should handle signals in the following way:

    --- name ------- value ----- description -------

    signal.SIGHUP       1   ignore it.
    signal.SIGINT       2   keyboard control-C pressed
    signal.SIGUSR1      10  if appropriate, restart or reread config info.
    signal.SIGUSR2      12  a controlled shutdown. 
    signal.SIGTERM      15  a controlled shutdown.
    signal.SIGCHLD      17  either ignore it, or shutdown the sender.
    """
    child_signal = True

    tomb.tombstone('Received signal {}'.format(signum))
    try:
        # We want the bottom of the stack, not the top.
        stack_info = whocalled.whocalledme()[-1]
        restarter = stack_info.function
            
    except Exception as e:
        child_signal = False
        tomb.tombstone('cannot retrieve stack frame: {}'.format(str(e)))
        tomb.tombstone('signaler was outside my process family.')

    # We are looking to do the right thing when there is a control-C from
    # a terminal window.
    if signum == signal.SIGINT and os.isatty(0):
        os.kill(os.getpid(), signal.SIGUSR2)

    elif signum == signal.SIGINT:
        tomb.tombstone('returning from SIGINT')
        return

    elif signum == signal.SIGUSR1: 
        if child_signal:
            tomb.tombstone('restarting')
            restarter(None)
        else:
            tomb.tombstone('I would like to restart, but I cannot.')

    elif signum in [signal.SIGUSR2, signal.SIGTERM]: 
        tomb.tombstone('killing me softly')
        uu.fclose_all()
        sys.exit(os.EX_OK)

    elif signum == signal.SIGTSTP:
        tomb.tombstone('Received a terminal stop.')
        if os.isatty(0): 
            tomb.tombstone('isatty() returned True so we will wait for a SIGCONT.')
            os.kill(os.getpid(), signal.SIGSTOP)
            tomb.tombstone('resuming after SIGCONT received.')
            return
        else:
            tomb.tombstone('we are not hooked up to a terminal.')
            return

    elif signum > signal.SIGRTMIN:
        tomb.tombstone('Received a realtime signal.')
        if child_signal: restarter(None)

    else:
        tomb.tombstone('nothing to do for signal {}'.format(signum))
        pass


if __name__ == "__main__":
    print("My PID is {}".format(os.getpid()))

    print("Mapping signals {} to handler()".format(signal.NSIG))
    all_signals = range(1, signal.NSIG)
    for i in all_signals:
        try:
            signal.signal(i, handler)
        except Exception as e:
            print("Cannot handle signal {}".format(i))

    try:
        while True:
            print("Send me a signal, send me a sign.")
            time.sleep(20)
            # print("Signal is {}".format(signal.sigwait(set(all_signals))))

    except KeyboardInterrupt as e:
        print("you pressed control-C")

    except Exception as e:
        print(str(e))

