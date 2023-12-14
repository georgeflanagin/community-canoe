#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from typing import *

""" 
This is the scheduler that feeds canoed. It is intended to be launched 
with nohup.
"""

# Credits
__author__ = 'George Flanagin, Douglas Broome'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.4'
__license__ = 'https://www.gnu.org/licenses/gpl.html'
__maintainer__ = 'George Flanagin, Douglas Broome'
__email__ = 'gflanagin@richmond.edu, dbroome@richmond.edu'
__status__ = 'Working Prototype'
__required_version__ = (3, 6)


__license__ = 'MIT'
import license

# Standard imports

import argparse
import os
import signal
import sys
import time

if sys.version_info < __required_version__:
    print("This software requires Python " + str(__required_version__))
    sys.exit(os.EX_SOFTWARE)

# Installed imports
from   urutils import setproctitle, getproctitle

# Canøe imports
import fifo
import loader
import tombstone as tomb
from   urdecorators import show_exceptions_and_frames as trap
import urutils as uu

############# BEGIN #################
def handler(signum:int, stack:object=None) -> None:
    tomb.tombstone('Received signal {}'.format(signum))
    if signum == signal.SIGUSR1: canoeschedd()
    elif signum == signal.SIGUSR2: 
        tomb.tombstone('killing me softly')
        save_last_time(time.time() // 60)
        uu.fclose_all()
        sys.exit(os.EX_OK)
    else:
        tomb.tombstone('no handler for signal {}'.format(signum))
        pass

def get_last_time() -> int:
    moment = -1
    try:
        with open(uu.expandall('~/.lasttime.dat'), 'r') as f:
            x = f.read()
            moment=int(x)

    except Exception as e:
        moment = time.time() // 60
        save_last_time(moment)

    finally:
        return int(moment)


def save_last_time(t:int) -> None:
    with open(uu.expandall('~/.lasttime.dat'), 'w') as f:
        f.write("{}\n".format(int(t)))


pipe_name = None

@trap
def canoeschedd(pipe_affinity:str=None, cold:bool=False):
    """
    We are going nohup, so there are no demonic preliminaries to get
    out of the way. 
    """
    global pipe_name
    tomb.tombstone("THIS IS CANOE 20's SCHEDULER STARTING.")
    if pipe_name is None: pipe_name = pipe_affinity

    # It doesn't hurt to start it if it is already started.
    try:
        my_pipe = fifo.FIFO(pipe_name, 'w')
    except Exception as e:
        tomb.tombstone(str(e))
        tomb.tombstone("Start canoed first. The scheduler cannot write without a reader.")
        sys.exit(os.EX_CANTCREAT)
    else:
        tomb.tombstone(f'Posting events to {pipe_name}')
        

    # Our name will reflect to which queue we are writing.
    myname = 'canoeschedd:' + pipe_name
    setproctitle(myname)

    COMPILEDRECIPES = os.environ.get('COMPILEDRECIPES', '/sw/canoe/compiledrecipes')

    # Do this with a generator because we don't need all the recipes
    # in memory -- we just need the schedule from each one, and the remainder
    # of the data can be left off.
    gen = loader.load(COMPILEDRECIPES)
    todo_list = {}
    while True:
        try:
            prog = next(gen)
            for i, line_item in enumerate(prog.schedule):
                for j, element in enumerate(line_item):
                    if not element: prog.schedule[i][j] = uu.Universal()
            todo_list[prog.name] = prog.schedule
        except StopIteration as e:
            break
        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            raise

    print("{} programs loaded.".format(len(todo_list)))

    # I suppose this could be combined with the above dictionary
    # comprehension, but I think we might get to "unreadable" at
    # that point.
    tomb.tombstone("+"*80)
    tomb.tombstone("begin schedules")
    for _ in sorted(todo_list.keys()):
        tomb.tombstone("{} @ {}".format(_, todo_list[_]))

    tomb.tombstone("end schedules")
    tomb.tombstone("+"*80)

    start_time = stop_time = time.time()
    run_priority = 1.0
    max_nap = 58
    # tomb.tombstone("The alarm goes off every {} seconds.".format(max_nap))

    # Set this to zero if we are outside the while-loop starting below.
    # If inside the loop last_time is zero, then we are starting up cold.
    last_minute = 0 if cold else get_last_time()
    try:
        tomb.tombstone("last_minute initialized to {}".format(last_minute))
        while True:
            start_time = time.time()
            this_minute = int(start_time // 60)

            # Once upon a time we had clock failure (22 April 2019), and every
            # year we have daylight saving time. Chill out until things clear.
            if this_minute < last_minute:
                time.sleep(58)
                continue

            save_last_time(this_minute)
            # Note that the following range() might have no elements.
            minutes_to_consider = [this_minute] if last_minute == 0 else list(range(last_minute+1, this_minute+1))
            # tomb.tombstone("Evaluating minutes {}".format(minutes_to_consider))
            last_minute = this_minute

            try:
                count = 0
                jobs_to_do = set()
                # Very likely there will be only one minute to consider.
                for minute in minutes_to_consider:
                    for _ in sorted(todo_list.keys()): # for each job ....
                        for schedule in todo_list[_]: # for each schedule it has ....
                            if uu.this_is_the_time(minute, schedule):
                                jobs_to_do.add(_)
                                count += 1
                if count:
                    jobs_to_do = list(jobs_to_do)
                    my_pipe(jobs_to_do)
                    tomb.tombstone('Added {} events.'.format(len(jobs_to_do)))
                else:
                    tomb.tombstone('Nothing new to schedule.')    

            except Exception as e:
                tomb.tombstone(str(e))

            stop_time = time.time()
            sleep_interval = max_nap - (stop_time - start_time)
            # tomb.tombstone('sleeping for {} seconds.'.format(sleep_interval))
            time.sleep(sleep_interval)

    except Exception as e:
        tomb.tombstone(uu.type_and_text(e))

    finally:
        save_last_time(this_minute)
        tomb.tombstone('Closing up.')
        uu.fclose_all()


if __name__ == '__main__':
    signal.signal(signal.SIGUSR1, handler)
    signal.signal(signal.SIGUSR2, handler)

    parser = argparse.ArgumentParser()
    parser.add_argument('--cold', action='store_true') 
    parser.add_argument('--pipe', type=str, required=True)
    my_args = parser.parse_args()
    canoeschedd(my_args.pipe, my_args.cold)
else:
    pass
