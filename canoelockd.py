# -*- coding: utf-8 -*-
import typing
from typing import *

""" 
This is program creates a file on one or more remote mount
points, and rewrites the pid to that file at regular 
intervals. This is known as the "lock file." Each lock
file is associated with a unique process, and that
process's pid is the content of the lock file.
"""

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2018, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Working Prototype'
__required_version__ = (3, 6)

__license__ = 'MIT'

# Built-ins.
import os
import signal
import sys
import time

# Python modules we wrote.
import tombstone as tomb
import urutils as uu
import urutils as setproctitle

trappable_signals = frozenset([1, 2, 3, 10, 12, 15])
lock_file_name = 'canoe.lock'
my_kids = {}
parent_process = -1
default_lock_interval = 60

def handler(signum:int, stack:object=None) -> None:
    """
    Uncomplicated signal handler. When we receive one of these signals
    we close our files and quietly stop or die.
    """
    tomb.tombstone('Received signal {}.'.format(signum))
    global my_kids
    global parent_process

    if signum in [ signal.SIGHUP, signal.SIGINT, signal.SIGQUIT,
            signal.SIGUSR1, signal.SIGUSR2, signal.SIGTERM ]: 
        tomb.tombstone('Closing up.')

        # If we are the parent, and we have received a request to stop,
        # then we should pass along the signal to our offspring. The
        # kids will be signaling back and we don't have waitpid() here,
        # so we need to ignore SIGCHLD lest we have zombies.
        if os.getpid() == parent_process:
            signal.signal(signal.SIGCHLD, signal.SIG_IGN)
            for kid in my_kids:
                os.kill(int(kid), signum)

        uu.fclose_all()
        sys.exit(os.EX_OK)

    else:
        tomb.tombstone("ignoring signal {}. What are you doing to me?".format(signum))
        pass


def maintain_lock_file(mount_point:str, rewrite_interval:int) -> None:
    """
    This function is only called in the child process. It creates 
    a lock file on the given mountpoint.

    mount_point -- the name of something that starts with /mnt/
    rewrite_interval -- seconds between rewriting.

    returns -- None. This should run until it gets a signal to stop.
    """
    global lock_file_name
    pid_string = bytes(str(os.getpid()) + '\n', 'utf-8')
    filename = os.path.join(mount_point, lock_file_name)

    # if there is an old one, let's remove it.
    try:
        os.unlink(filename)
    except Exception as e:
        pass

    try:
        f = os.open(filename, os.O_RDWR|os.O_EXCL|os.O_CREAT, mode=0o600)

    except PermissionError as e:
        # There is no way we can create a file.
        tomb.tombstone('cannot create lock file on {}'.format(mount_point))
        return

    except OSError as e:
        # This is generally 'host down.'
        tomb.tombstone(str(e))
        return

    except Exception as e:
        # Something truly terrible.
        tomb.tombstone(uu.type_and_text(e))
        return

    else:
        tomb.tombstone('{} created.'.format(filename))

    try:
        while True:
            # we are in business.
            os.pwrite(f, pid_string, 0)
            os.fsync(f)
            time.sleep(rewrite_interval)

    except Exception as e:
        # The moment we are trying to capture: the mount point disappeared.
        tomb.tombstone(uu.type_and_text(e))
        raise e from None


def canoelock_main(my_args:List[str]) -> int:
    """
    Usage:
        canoelock_main(interval, mnt1, [mnt2, [ mnt3 .. ]]) 

    This function tries to create a process that will then create
        a lock file on the designated mountpoint. 

    If this function is called with *no* arguments, it will retrieve
    a list of mounted file systems of type cifs. Whether these can 
    be monitored is anyone's guess. If you supply arguments beyond
    the monitoring interval, those directories will be added to
    the mount list.

    interval -- number of seconds between rewrites.
    mnt1, etc. -- names of mount points.
    """
    mounts = None

    if '-?' in my_args:
        tomb.tombstone(canoelock_main.__doc__)
        return os.EX_OK

    try:
        interval = int(my_args[0])

    except IndexError as e:
        tomb.tombstone('Using default lock interval of {}'.format(default_lock_interval))
        interval = default_lock_interval

    except ValueError as e:
        tomb.tombstone('The first argument in {} must be an integer'.format(my_args))
        return os.EX_DATAERR


    my_args = my_args[1:] # If my_args is empty, this slice op still works.

    mounts = []
    with open('/proc/mounts') as f:
        data = f.read().split('\n')
    mounts = [ _.split()[1] for _ in data if 'cifs' in _ ]
    if my_args: mounts.extend(my_args)
    mounts = sorted(list(set(mounts)))

    # We have to make these global so that we can reference them
    # in the handler.
    global my_kids
    global parent_process
    parent_process = os.getpid()
    setproctitle.setproctitle('canoelockd:+')

    # Create each lock file in a separate process. Kick 'em
    # off with a happy fork().
    for mount_point in mounts:
        pid = os.fork()
        if pid < 0: 
            tomb.tombstone('fork failed for {}'.format(mount_point))
            continue

        elif pid > 0:
            # keep track of our kids.
            my_kids[str(pid)] = mount_point
            tomb.tombstone('Launched pid {} to monitor {}'.format(pid, mount_point))
            continue

        else: 
            # this code is executed in the forked (child) process. If the function
            # returns without raising an exception, it could not create/maintain
            # the required lock file.
            try:
                setproctitle.setproctitle('canoelockd:{}'.format(os.path.split(mount_point)[-1]))
                maintain_lock_file(mount_point, 60)
                tomb.tombstone('unable to create lock file on {}'.format(mount_point))

            except Exception as e:
                raise Exception('adminsys@richmond.edu', 
                    'mount point {} is down.'.format(mount_point)) from None

            finally:
                sys.exit(os.EX_UNAVAILABLE)

    # The child code never gets to here.
    while len(my_kids):
        tomb.tombstone('Waiting ...')

        #########################################################
        # os.wait() returns a tuple. exit info is laid out this way:
        #
        # +--------+v-------+
        # |FEDCBA98|76543210|
        # +--------+^-------+
        # |  code  | signal |
        # +--------+^-------+
        #           |
        #           +-- If this bit is on, there is a core dump.
        #########################################################

        pid, exit_info, usage = os.wait3(os.P_ALL)
        unmount_point = my_kids.pop(str(pid)) 
        kill_signal = exit_info & 0xFF
        exit_status = (exit_info & 0xFF00) // 0xFF

        if kill_signal & 0x80:
            tomb.tombstone('corefile produced for pid {} monitoring {}'.format(pid, unmount_point))
        tomb.tombstone('{} killed by signal {}, exit code = {}'.format(
            pid, kill_signal, exit_status & 0x7F
            ))

    return os.EX_OK              


if __name__ == '__main__':
    for signal_number in trappable_signals:
        try:
            signal.signal(signal_number, handler)
        except OSError as e:
            tomb.tombstone('cannot reassign signal {}'.format(signal_number))
        else:
            tomb.tombstone('signal {} is being handled.'.format(signal_number))

    sys.exit(canoelock_main(sys.argv[1:]))
else:
    pass

