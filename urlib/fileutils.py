# -*- coding: utf-8 -*-
""" Generic, bare functions, not a part of any object or service. """

# Added for Python 3.5+
import typing
from typing import *

import argparse
import atexit
import base64
import binascii
import calendar
import collections
from   collections.abc import Iterable
import contextlib
import copy
from   ctypes import cdll, byref, create_string_buffer
import datetime
import dateutil
from   dateutil import parser
import enum
import errno
import fcntl
import fnmatch
import functools
from   functools import reduce
import getpass
import glob
import hashlib
import inspect
import ipaddress
import io
import json
import locale
import operator
import os
import paramiko
import pickle
import pprint as pp
import psutil
import random
import resource
import re
import shlex
import shutil
import signal
import socket
import stat
import string
import sortedcontainers
import subprocess
import sys
import tempfile
import threading
import time
import traceback
try:
    libc = cdll.LoadLibrary('libc.so.6')
except OSError as e:
    print("libc.so.6 has not been loaded.")
    libc = None
    
import croniter

# Credits
__longname__ = "University of Richmond"
__acronym__ = " UR "
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Prototype'

__license__ = 'MIT'

LIGHT_BLUE="\033[1;34m"
BLUE = '\033[94m'
RED = '\033[91m'
YELLOW = '\033[1;33m'
REVERSE = "\033[7m"
REVERT = "\033[0m"
GREEN="\033[0;32m"

LOCK_NONE = 0

pig = [
    "                         _",
    " _._ _..._ .-',     _.._(`))",
    "'-. `     '  /-._.-'    ',/",
    "   ) " + __acronym__ + "    \            '.",
    "  / -    -    | The Safety  \\",
    " |  O    O    /  Pig         |",
    " \   .-.                     ;  ",
    "  '-('' ).-'       ,'       ;",
    "     '-;           |      .'",
    "        \           \    /",
    "        | 7  .__  _.-\   \\",
    "        | |  |  ``/  /`  /",
    "       /,_|  |   /,_/   /",
    "          /,_/      '`-'",
    " "]

# Cheap hack so that "*" means "every{minute|hour|etc}"
class Universal(set):
    """
    Universal set - match everything. No matter the value
    of item, s.o.b., Mr. Wizard, it's there!
    """
    def __contains__(self, item): return True

class OuterLoop(Exception):
    pass

# And an instance.
star = Universal() 

class SloppyDict: pass
class SloppyTree: pass


# Cheap hack to get sequence numbers for tombstones.

class Accumulator(object):
    """
    This only works in a multi-processing environment because
    we care about the monotonic increasing property of the 
    numbers, not their values or whether the set of values is
    duplicated in a forked process.

    Syntax:

        i = Sequence()
    """
    ax = 0

    @classmethod
    def reset(cls):
        Accumulator.ax = 0


    def __init__(self):
        pass

    def __call__(self):
        Accumulator.ax += 1
        return Accumulator.ax

    def __int__(self):
        return ax

# And here is the accumulator itself.
AX=Accumulator()

####
# A
####

def all_ASCII(s: str) -> bool:
    """ 
    Ensure ASCII-ness.
    s -- a string.

    returns: -- True only if all characters in the string are ASCII.
        Empty strings are construed to be all ASCII, as they do not
        change the ASCII-ness of another string when concatenated.
    """
    if not len(s): return True
    return reduce(operator.mul, [int(ascii(x)) and int(ascii(x)) < 128 for x in list(s)], 1) == 1


def all_files_in(s:str) -> str:
    """
    A generator to cough up the full file names for every
    file in a directory.
    """
    s = expandall(s)
    for c, d, files in os.walk(s):
        for f in files:
            yield os.path.join(c, f)


def all_files_like(s:str) -> str:
    """
    A list of all files that match the argument
    """
    s = expandall(s)
    return [ f for f in all_files_in(os.path.dirname(s)) 
        if fnmatch.fnmatch(os.path.basename(f), os.path.basename(s)) ]


def args_to_str(n:argparse.Namespace) -> str:
    opt_string = ""
    for _ in sorted(vars(n).items()):
        opt_string += "    --"+ _[0].replace("_","-") + " " + str(_[1]) + "\n"

    return opt_string


def asciify(s: str) -> str:
    """ Convert chars in string to ascii-only by 'flattening'

    s - string to be transformed.
    returns: - a possibly empty transformation of s.
    """
    return str(s).encode('ascii', 'ignore')



def ask(obj:Any, host:str, port:int, EOM:str="$$$") -> str:
    """ This function is used to transmit JSON over a socket.

    obj -- a snippet of data, generally a true object, that needs
        to be rewritten as a JSON string.
    host -- the name of some machine on the interwebs.
    port -- the host's listen port.
    EOM -- a software marking indicating the end-of-message.

    returns: -- The reply as a string without EOM on the end, 
        or None on error.
    """

    reply = ""
    buf = ""
    if not isinstance(obj, str): obj = json.dumps(obj) + str(EOM)
    if not obj: return None

    try:
        the_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        the_sock.setblocking(1)
        the_sock.connect((socket.gethostbyname(host), port))
        the_sock.sendall(bytes(obj, 'utf-8'))
    except socket.error:
        print ('socket error ' + socket.error)
        return None
    while True:
        try:
            buf = the_sock.recv(4096)
        except socket.error: 
            pass
        except KeyboardInterrupt:
            tombstone("Aborting request. crtl-C detected on keyboard.")
            return
        finally:
            reply = reply + buf
        if len(buf) < 4096 or reply.endswith(EOM): break

    if reply.endswith(EOM): reply = reply[:-3]

    return reply


####
# B
####

def bad_exit() -> None:
    tombstone("Halted by signal at " + time.ctime())
    fclose_all()
    os._exit(0)    


def blind(s:str) -> str:
    """
    Produce /blinding/ reverse video around the display of a string.
    """
    global REVERSE
    global REVERT
    return " " + REVERSE + s + REVERT + " " 


def boolify(x:Any) -> bool:
    """
    Work around all the various ways people represent the truth.
    """
    if isinstance(x, bool): # already a boolean
        return x

    elif isinstance(x, (int, float)): # test for nonzero-ness
        return x != 0

    elif isinstance(x, complex):
        return x != 0j

    elif isinstance(x, str): # test for these common strings.
        return x.lower() in ['yes', 'true', 'ok']

    elif isinstance(x, Iterable) and len(x) > 0: # if it is a container, test all elements.
        return all([boolify(_) for _ in x]) 

    try: # For testing files and empty containers.
        return len(x) > 0
    except:
        return False


def brine(o:object, force_bytes=False) -> str:
    """
    Return a base64 encoded pickle of the argument as a str or bytes.
    """
    try:
        s = base64.b64encode(pickle.dumps(o, pickle.DEFAULT_PROTOCOL))
    except pickle.PicklingError as e:
        tombstone("Pickling error: {}".format(str(e)))
    except binascii.Error as e:
        tombstone("Base64 encoding error: {}".format(str(e)))

    return s if force_bytes else s.decode('utf-8')


def unbrine(s:str) -> object:
    """
    Reverse the brining.
    """
    try:
        return pickle.loads(base64.b64decode(s))
    except UnpicklingError as e:
        tombstone("Unpickling error: {}".format(str(e)))
    except binascii.Error as e:
        tombstone("Base64 decoding error: {}".format(str(e)))


def build_file_list(f:str) -> List[str]:
    """
    Resolve all the symbolic names that might be embedded in the filespec,
    and return a list of all the files that match it **at the time the
    function is called.**

    f -- a file name "spec."

    returns -- a possibly empty list of file names.
    """
    return glob.glob(file_name_filter(f))
    

####
# C
####

def canoe_schedule(s:str) -> List[List[Set]]:
    """
    Deal with special cases...

    @adhoc -- create a schedule that never matches any date.
    @=     -- a date string, written in free form.
    @random=stuff. See explanation in the code below.

    returns what it finds.
    """
    never = [ {61}, {25}, {32}, {13}, {8} ]
    items = []

    if s.startswith('@='):
        lhs, rhs = s.split('=')
        try:
            t = dateutil.parser.parse(rhs).timetuple()[1:5]
            s = " ".join([str(e) for e in t[::-1]]) + " *"
            items.append(parse_schedule(s))
        except ValueError as e:
            items.append(parse_schedule(_))

    elif s.startswith('@random='):
        """
        This is a regular schedule in every way except one,
        namely that the minute term is written as 
        [0-9][0-9]?[xX]. This term tells how many times
        per hour you wish to run. 
        """
        terms = s[8:].strip().split()
        try:
            tries = int(terms[0][:-1])
        except:
            tries = 0
            tombstone('ignoring invalid schedule ' + s)
        terms[0] = "0"
        schedule = parse_schedule(" ".join(terms))
        schedule[0] = set([ 
            random.randrange(60) for i in range(0, tries) 
            ])
        return schedule    

    elif s == '@adhoc': 
        return never

    else:
        raise Exception('Unknown schedule: ' + str(s))

    return [ item for item in items if item ]


def canoe_version() -> bytes:
    git = "/opt/rh/rh-git218/root/usr/bin/git"
    result = subprocess.run(f"{git} rev-parse --abbrev-ref HEAD", shell=True, stdout=subprocess.PIPE)
    return bytes(result.stdout[:8]) + b'00'


def columns() -> int:
    """
    If we are in a console window, return the number of columns. 
    Return zero if we cannot figure it out, or the request fails.
    """
    try:
        return int(subprocess.check_output(['tput','cols']).decode('utf-8').strip())
    except:
        return 0


def compiler_info(opcodes:SloppyDict) -> str:
    return opcodes.compiler_info


def compiled_time(opcodes:SloppyDict) -> float:
    return opcodes.compiled_time


def contains_metachars(s:str) -> bool:
    if not s: return False
    for c in s:
        if c in '?*[]': return True
    return False


def cron_to_str(cron:Tuple[Set]) -> Dict:
    """
    Return an English explanation of a crontab entry.
    """

    if len(cron) != 5: return "This does not appear to be a cron schedule"
    
    keynames=["a_minutes","b_hours","c_days","d_months","e_dows"]
    explanation = dict.fromkeys(keynames)

    for time_unit, sched in zip(keynames, cron):

        # This is self explanatory, right?
        if sched == star:
            explanation[time_unit] = 'all ' + time_unit[2:]
            continue
        
        # Test for the exact value (often the case for min, hr, dow)
        valid = sorted(list(sched))
        if len(valid) == 1:
            explanation[time_unit] = time_unit[2:] + " " + str(valid[0])
            continue

        if valid == list(range(valid[0], valid[-1]+1)):
            explanation[time_unit] = (time_unit[2:] + " " + str(valid[0]) + 
                " to " + str(valid[-1]))
            continue

        # Test for every fifth minute, third month, etc. Maybe some 
        # explanation is required ... zip() stops when the first target
        # of the pair is empty. We subtract the neighbors (remember, it
        # already sorted), and make a set. If the set only has one
        # element, then the neighbors are equally spaced apart.

        diffs = set([ j - i for i, j in zip(valid, valid[1:]) ])
        if len(diffs) == 1:
            explanation[time_unit] = (time_unit[2:] + 
                " every " + str(diffs.pop()) + 
                " from " + str(valid[0]) + " to " + str(valid[-1]))
            continue

        # TODO: tune this up a bit.

        explanation[time_unit] = time_unit[2:] + " in " + str(valid)

    return explanation


# Some point in the distant past, namely 14 May 2014
arbitrarily_long_ago = 1400000000
def crontuple_now(t:Any=None) -> datetime.datetime:
    """
    Return t (or "now") as a datetime object. t should be in whole 
    minutes.
    """
    global arbitrarily_long_ago

    # This branch gives us compatibility with existing code.
    if t is None:
        moment = datetime.datetime.now()

    # This branch allows us to distinguish between minutes and seconds of the epoch.
    elif isinstance(t, (int, float)):
        t = t*60 if t < arbitrarily_long_ago else t
        moment = datetime.datetime.fromtimestamp(t) 

    # Maybe the caller has already converted?
    elif isinstance(t, datetime.datetime):
        moment = t

    # Or perhaps the caller did not know what he was doing?
    else:
        raise Exception('bad arg type {} to crontuple_now()'.format(type(t)))

    # Get us the crontab relevant parts.
    return datetime.datetime(*moment.timetuple()[:6])


####
# D
####

def date_filter(filename:str, *, 
    year:str="YYYY", 
    year_contracted:str="Y?",
    month:str="MM", 
    month_contracted:str="M?",
    month_name:str="bbb",
    week_number:str="WW",
    day:str="DD",
    day_contracted:str="D?",
    hour:str="hh",
    minute:str="mm",
    second:str="ss",
    date_offset:int=0) -> str:
    """
    Remove placeholders from a filename and use today's date (with
    an optional offset).

    NOTE: all the placeholders are non-numeric, and all the replacements 
        are digits. Thus the function works because the two are disjoint
        sets. Break that .. and the function doesn't work.
    """
    if not isinstance(filename, str): return filename

    #Return unmodified file name if there isn't at least one set of format delimiters "{" and "}"
    if not re.match(".*?\{.*?\}.*?", filename):
        return filename

    today = crontuple_now() + datetime.timedelta(days=date_offset)

    # And now ... for Petrarch's Sonnet 47
    this_year = str(today.year)
    this_year_contracted = this_year[2:]
    this_month_name = calendar.month_abbr[today.month].upper()
    this_month = str('%02d' % today.month)
    this_month_contracted = this_month if this_month[0] == '1' else this_month[1]
    this_week = str('%02d' % datetime.date.today().isocalendar()[1])
    this_day =  str('%02d' % today.day)
    this_day_contracted = this_day if this_day[0] != '0' else this_day[1]
    this_hour = str('%02d' % today.hour)
    this_minute = str('%02d' % today.minute)
    this_second = str('%02d' % today.second)

    #Initialize new_filename so we can use it later
    new_filename = filename
    
    #Iterate through each pair of "{" and "}" in filename and replace placeholder values
    #with date literals
    for date_exp in [ m.group(0) for m in re.finditer("\{.*?\}",filename) ]:
        #Start with the sliced substring excluding the "{" and "}" charactes and
        #begin replacing pattern date strings with literals
        new_name = date_exp[1:-1].replace(year, this_year)
        new_name = new_name.replace(year_contracted, this_year_contracted)
        new_name = new_name.replace(month_name, this_month_name)
        new_name = new_name.replace(month, this_month)
        new_name = new_name.replace(month_contracted, this_month_contracted)
        new_name = new_name.replace(week_number, this_week)
        new_name = new_name.replace(day, this_day)
        new_name = new_name.replace(day_contracted, this_day_contracted)
        new_name = new_name.replace(hour, this_hour)
        new_name = new_name.replace(minute, this_minute)
        new_name = new_name.replace(second, this_second)
        #Now replace the original string including the "{" and "}" with the translated string
        new_filename = new_filename.replace(date_exp,new_name)

    #Return result and strip { and } format containers
    return new_filename


def datetime_encoder(obj:Any) -> str:
    """
    If Oracle DATETIME objects come back to the program via cx_Oracle,
    they are a non-serializable type. This class is a hook that 
    spots them, and returns the YYYY-MM-DD part of the ISO 8601 
    formatted string.

    If the argument is /not/ a DATETIME type, then we pass it along
    to the bog standard encoder.
    """
    try:
        return obj.isoformat()[:10]
    except:
        return obj


def deliver_file_box(handle:object, 
    filename:str, filename2:str, cloud_folder_id:int, klobber:bool) -> bool:

    num_tries = 1
    while not handle.put(filename, filename2, cloud_folder_id, klobber): 
        num_tries += 1
        OK = snooze(num_tries)
        if not OK: return False
    return True


def deliver_file_host(handle:object,
    local_filename:str, remote_directory:str, remote_filename:str, overwrite:bool) -> bool:

    num_tries = 1
    while not handle.send_one_file(local_filename, remote_filename, overwrite, remote_directory):
        num_tries += 1
        OK = snooze(num_tries)
        if not OK: return False
    return True


def deliver_file_s3(handle:object,
    local_name:str, remote_name:str, overwrite:bool) -> bool:

    num_tries = 1
    while not handle.send_to_bucket(local_name, remote_name, overwrite):
        num_tries += 1
        OK = snooze(num_tries)
        if not OK: return False
    return True


def dictify(args:str) -> Dict[str, str]:
    """
    We take commonsense narratives like:

       " subject= report88, header, sender =system@starrez.com"

    and change them into implicit Python keyword arguments
    that resemble this tidy collection of keys and values.

        {"subject":"report88", "header":True, "sender":"system@starrez.com"}
    """
    the_dict = collections.defaultdict(lambda:True)

    args = args.strip().split(',')
    for arg in args:
        try:
            x, y = arg.strip().split('=')
            the_dict[x.strip()] = y.strip()
        except:
            the_dict[arg.strip()]

    return dict(the_dict)
            

def dict_walk(d:Dict[Hashable, Any]) -> Dict[Hashable, Any]:
    """
    Blow down the keys and values of a nested data structure.
    """
    for k, v in d.items():
        if type(v) is dict:
            yield from dict_walk(v)
        else:
            yield (k, v)


def do_not_run_twice(name:str) -> None:
    """
    Prevents multiple executions at startup. Note that you shouldn't
    call this function from a program after it may have forked into
    multiple processes.
    """
    pids = pids_of(name, True)
    if len(pids):
        tombstone(name + " appears to be already running, and has these PIDs: " + str(pids))
        sys.exit(os.EX_OSERR)


def dorunrun(command:Union[str, list],
    timeout:int=None,
    verbose:bool=False,
    quiet:bool=False,
    return_exit_code:bool=False,
    ) -> Union[bool, int]:
    """
    A wrapper around (almost) all the complexities of running child 
        processes.
    command -- a string, or a list of strings, that constitute the
        commonsense definition of the command to be attemped. 
    timeout -- generally, we don't
    verbose -- do we want some narrative to stderr?
    quiet -- overrides verbose, shell, etc. 
    return_exit_code -- return the actual exit code rather than
        implicitly converting to boolean True for 0.

    returns -- True if the child process returns a zero, or the code.
    """

    if verbose: tombstone(f"{command=}")

    if isinstance(command, (list, tuple)):
        command = [str(_) for _ in command]
        shell = False

    elif isinstance(command, str):
        shell = True

    else:
        raise Exception(f"Bad argument type to dorunrun: {command}")

    r = None
    try:
        result = subprocess.run(command, 
            timeout=timeout, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            shell=shell)

        r = result.returncode

        # Always show errors even if verbose is False.
        if not r:
            verbose and tombstone("Child process terminated without error.")
        elif r < 0:
            tombstone(f"Child process terminated by signal {-r}")
        else:
            verbose and tombstone(f"Child process returned an error: {r}")

        if not quiet:
            if r or shell or verbose:
                tombstone(f"stdout: {result.stdout}")
                tombstone(f"stderr: {result.stderr}")

    except subprocess.TimeoutExpired as e:
        tombstone(f"Process exceeded time limit at {e.timeout} seconds.")    

    except Exception as e:
        tombstone(f"Unexpected error: {str(e)}")
    
    return result.returncode if return_exit_code else (r == 0)


def dump_cmdline(args:argparse.ArgumentParser, return_it:bool=False) -> str:
    """
    Print the command line arguments as they would have been if the user
    had specified every possible one (including optionals and defaults).
    """
    if not return_it: print("")
    opt_string = ""
    for _ in sorted(vars(args).items()):
        opt_string += " --"+ _[0].replace("_","-") + " " + str(_[1])
    if not return_it: print(opt_string + "\n")
    
    return opt_string if return_it else ""


def dump_exception(e:Exception, line:int=0, fcn:str=None, module:str=None) -> str:
    """ Tell us what we really [don't] want to know. """
    cf = inspect.stack()[1]
    f = cf[0]
    i = inspect.getframeinfo(f)

    line = str(i.lineno) if line == 0 else line
    module = i.filename if not module else module
    junk, exception_type = str(type(e)).split()
    exception_type = exception_type.strip("'>")

    msg = []
    msg.append("Caught @ line " + line + " in module " + module + 
            ".\n" + str(e) + " :: raised by " + exception_type)
    # squeal(msg)k
    msg.extend(formatted_stack_trace())
    return "\n".join(msg)


####
# E
####

def empty(o:Any) -> bool:
    """
    Roughly equivalent to PHP's empty(), but goes farther. Oddly formed
    JSON constructions like {{}} and {[]} need to be considered empty.
    """

    # See if it is "False" by the usual means. 
    if not o: return True

    # Determine if it might be a collection of Falsies.
    try:
        r = functools.reduce(operator.and_, [empty(oo) for oo in o])
    except:
        return False
    
    return r


def empty_to_null_literal(s:str) -> str:
    """ For creating database statements with a literal NULL

    s -- string to assess
    returns: -- s or 'NULL'

    >>> empty_to_null_literal('')    
    'NULL'
    >>> empty_to_null_literal('NULL')
    'NULL'
    >>> empty_to_null_literal(5)
    '5'
    >>> empty_to_null_literal('george')
    'george'
    """

    return (str(s) if (not isinstance(s, str) or len(s) > 0) else 'NULL')


def expandall(s:str) -> str:
    """
    Expand all the user vars into an absolute path name. If the 
    argument happens to be None, it is OK.
    """
    return s if s is None else os.path.abspath(os.path.expandvars(os.path.expanduser(s)))
    

def explain(code:int) -> str:
    """
    Lookup the os.EX_* codes.
    """
    codes = { _:getattr(os, _) for _ in dir(os) if _.startswith('EX_') }
    names = {v:k for k, v in codes.items()}    
    return names.get(code, 'No explanation for {}'.format(code))


####
# F
####

def fatalerror() -> None:

    mypid = os.getpid()
    __wrapper_marker_local__ = None
    # Create a dump file.
    # "$CANOE_LOG/today's-date" as the dir name.
    new_dir = os.path.join(os.environ.get('CANOE_LOG', '.'), now_as_string()[:10])
    make_dir_or_die(new_dir)

    # The file name will be the "pid.dump"
    dump_file = os.path.join(new_dir, "{}.dump".format(mypid))
    
    tombstone("writing dump to file {}".format(blind(dump_file)))

    with open(dump_file, 'a') as f:
        with contextlib.redirect_stdout(f):
            # Protect against further failure -- log the exception.
            try:
                e_type, e_val, e_trace = sys.exc_info()
            except Exception as e:
                tombstone(type_and_text(e))

            if e_type is None or e_val is None:
                print('{} Not called because of an exception.'.format(now_as_string()))
            else:
                print('{} Exception raised {}: "{}"'.format(now_as_string(), e_type, e_val))
            
            # iterate through the frames in reverse order so we print the
            # most recent frame first
            for frame_info in inspect.getinnerframes(e_trace):
                f_locals = frame_info[0].f_locals
        
                # If there is a local variable named __wrapper_marker_local__, we assume
                # the frame is from a call of this function. Nothing to see here.
                if '__wrapper_marker_local__' in f_locals: continue

                # log the frame information
                print('\n{}\n**File <{}>, line {}, in function {}()\n    {}'.format(
                    80*'-',
                    frame_info[1], 
                    frame_info[2], 
                    frame_info[3], 
                    frame_info[4][0].lstrip()
                    ))

                # log every local variable of the frame
                for k, v in f_locals.items():
                    try:
                        print('    {} <=> {}'.format(k, v))
                    except:
                        pass

            print('{}\n'.format(80*'='))


def fclose_all() -> None:
    for i in range (0, 1024):
        try:
            os.close(i)
        except:
            continue


def fcn_signature(*args) -> str:
    """
    provide a string for debugging that resembles a function's actual
    arguments; i.e., how it was called. The zero-th element of args 
    should be the name of the function, and then the arguments follow.
    """
    if not args: return "()"

    s = args[0] + "("
    s += ", ".join([str(_) for _ in args[1:]])
    s += ")"
    return s


def file_name_filter(filename:str, env:str='.') -> str:
    """
    Modify the filename in the following ways, and in this order:

    1. Apply the date filtering.
    2. Expand any environment variables or directory shorthand.
    3. Join the environment if the name does not start with an
        absolute path.
    """
    filename = expandall(date_filter(filename))

    if not filename.startswith(os.sep): 
        filename = os.path.join(env, filename)

    return filename


def fmove_safe(file1:str, file2:str, lock_strategy:int=0) -> int:
    """
    Call fcopy_safe to move a file, and then unlink the original
    if this is successful. 

    file1 -- the source file name.

    file2 -- the destination file name.

    lock_strategy -- applies to file1. The default is to attempt
        a flying read, and hope for the best.
    """
    
    result_of_copy = fcopy_safe(file1, file2, lock_strategy=lock_strategy) 

    xtime = Stopwatch()
    if result_of_copy in [ os.EX_OK, os.EX_CONFIG ]: os.unlink(file1)
    xtime.lap('unlinked')
    xtime.stop()
    tombstone(str(xtime))

    return result_of_copy


def fcopy_safe(file1:str, file2:str, *, 
        mode:int=stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP,
        num_attempts:int=2, 
        min_len:int=5, 
        lock_strategy:int=0,
        wait:float=0,
        overwrite:bool=True) -> int:

    """
    Carefully copy from something that looks like a file to another file,
    ensuring that the contents are really there.

    file1 -- file object associated with an open file.

    file2 -- either a filename (str) or something we have open-ed from
        elsewhere in the Python program.

    mode -- stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP is the equivalent
        of (octal) 0o640. You can, of course, pass in whatever you like.
        You may not have your request granted, but fcopy_safe() will do
        the best it can.

    num_attempts -- how many times do you want to try to os.stat the
        destination file?

    min_len -- to avoid copying empty and almost empty files, we have
        discovered that a value of 5 (bytes) is good.

    lock_strategy -- one of these:
        ----------------------
        no lock at all  : 0
        LOCK_SH         : 1
        LOCK_EX         : 2
        LOCK_NB         : 4
        LOCK_UN         : 8
        LOCK_MAND       : 32
        LOCK_READ       : 64
        LOCK_WRITE      : 128
        LOCK_RW         : 192

    overwrite -- do we klobber a file that is already there?

    returns -- one of the os.EX_* entries. Returns os.EX_OK if and only if
        the destination file was successfully closed.

        EX_OK - All went well.
        EX_NOINPUT - Unable to open the "from" file.
        EX_CANTCREAT - Says it all.
        EX_NOPERM - Cannot open the directory(s) atop one or the other files.
        
    """
    fd1 = fd2 = None
    locked = False
    can_lock_it = True
    # read_mode = 'wb' if lock_strategy else 'rb'
    read_mode = 'rb'
    write_mode = 'wb' if overwrite else 'xb'

    xtime = Stopwatch()


    """ 
    Check for copying a file onto itself. It happens. Often. 
    Note that we forgive this transgression, and we do it first
    because we are only checking the names of the files.
    """

    if os.path.realpath(file1) == os.path.realpath(file2):
        tombstone("{} and {} are the same pathname.".format(file1, file2))
        return os.EX_OK
    # xtime.lap('checked same file')

    """ 
    Check for an empty or almost empty source file. It happens. 
    We also forgive this. This check also does not involve opening 
    the file.
    """

    file_len = os.stat(file1).st_size
    if file_len < min_len:
        tombstone("not copying almost empty file {} of length {}.".format(file1, file_len))
        return os.EX_OK
    # xtime.lap('checked empty')

    """ Open the source file. """

    try:
        fd1 = open(file1, read_mode)
    except PermissionError as e:
        tombstone(type_and_text(e))

    except OSError as e:
        tombstone(type_and_text(e))
        return os.EX_NOINPUT

    # xtime.lap('open read')

    """ See if we need to lock it. """

    while lock_strategy and not locked and can_lock_it:
        try:
            fcntl.flock(fd1, lock_strategy)
        except OSError as e:
            can_lock_it = e.errno in [ errno.EAGAIN, errno.EACCES ]
        else:
            locked = True
            # xtime.lap('locked in mode {}'.format(lock_strategy))
            if wait > 0: 
                tombstone('waiting for {} seconds'.format(wait))
                time.sleep(wait)
                # xtime.lap('wait finished')
            break
    else:
        # xtime.lap('not locked' if not lock_strategy else 'lock failed')
        pass

    """ Open the output file. """

    try:
        fd2 = open(file2, write_mode)

    except IsADirectoryError as e:
        _, f = os.path.split(file1)
        fd2 = open(os.path.join(file2, f), write_mode)

    except FileNotFoundError as e:
        tombstone(type_and_text(e))
        tombstone('ACHTUNG! Checking to see if directories exist for {}'.format(file2))
        pieces = file2.split(os.sep)[1:-1]
        tombstone(str(pieces))
        top = os.sep
        for piece in pieces:
            top = (os.sep).join([top, piece])
            if os.path.isdir(top): 
                tombstone("Checking ... {}".format(top))
                continue
            else: 
                tombstone('Problem is with {}.'.format(top))   
                return os.EX_NOPERM
        else:
            tombstone('Likely problems with relative directory names.')
            return os.EX_CANTCREAT
        
    except Exception as e:
        tombstone(type_and_text(e))
        return os.EX_CANTCREAT

    # xtime.lap('open write')

    """ At long last we have fd1 and fd2. """
    try:
        shutil.copyfileobj(fd1, fd2) 
    except Exception as e:
        tombstone(type_and_text(e))
        return os.EX_IOERR

    xtime.lap('copied')

    for _ in range(0, num_attempts):
        try:
            os.stat(file2)
        except Exception as e:
            tombstone(type_and_text(e))
        else:
            break
    else:
        return os.EX_UNAVAILABLE

    xtime.lap('verified closed')

    try:
        os.chmod(file2, mode)
    except Exception as e:
        tombstone(type_and_text(e))
        return os.EX_CONFIG
    else:
        xtime.lap('perms reset')
    
    xtime.stop()
    tombstone(str(xtime))

    return os.EX_OK


def formatted_stack_trace(as_string: bool = False) -> str:
    """ Easy to read, tabular output. """

    exc_type, exc_value, exc_traceback = sys.exc_info()
    this_trace = traceback.extract_tb(exc_traceback)
    r = []
    r.append("Stack trace" + "\n" + "-"*80)
    for _ in this_trace:
        r.append(", line ".join([str(_[0]), str(_[1])]) +
            ", fcn <" + str(_[2]) + ">, context=>> " + str(_[3]))
    r.append("="*30 + " End of Stack Trace " + "="*30)
    return "\n".join(r) if as_string else r


def fcn_signature(*args) -> str:
    """
    provide a string for debugging that resembles a function's actual
    arguments; i.e., how it was called. The zero-th element of args 
    should be the name of the function, and then the arguments follow.
    """
    if not args: return "()"

    s = args[0] + "("
    s += ", ".join([str(_) for _ in args[1:]])
    s += ")"
    return s


####
# G
####

def getproctitle() -> str:
    global libc
    try:
        buff = create_string_buffer(128)
        libc.prctl(16, byref(buff), 0, 0, 0)
        return buff.value.decode('utf-8')

    except Exception as e:
        return ""


def get_ssh_host_info(host_name:str=None, config_file:str=None) -> List[Dict]:
    """ Utility function to get all the ssh config info, or just that
    for one host.

    host_name -- if given, it should be something that matches an entry
        in the ssh config file that gets parsed.
    config_file -- if not given (as it usually is not) the usual default
        config file is used.
    """

    if config_file is None:
        config_file = expandall("~") + "/.ssh/config"

    ssh_conf = paramiko.SSHConfig()
    ssh_conf.parse(open(config_file))

    if not host_name: return ssh_conf
    if host_name == 'all': return ssh_conf.get_hostnames()
    return None if host_name not in ssh_conf.get_hostnames() else SloppyDict(ssh_conf.lookup(host_name))

def get_file_page(path:str,num_bytes:int=resource.getpagesize()) -> str: 
    """
    Returns the first num_bytes of a file as a tuple of hex strings

    path -- path to file
    num_bytes -- number of bytes from position 0 to return
    """
    with open(path,'rb') as z:
        return z.read(num_bytes)

def got_data(filenames:str) -> bool:
    """
    Return True if the file or files all are non-empty, False otherwise.
    """
    if filenames is None or not len(filenames): return False

    filenames = listify(filenames)
    result = True
    for _ in filenames:
        result = result and bool(os.path.isfile(_)) and bool(os.stat(_).st_size)
    return result

####
# H
####

def hexify(msg_id:bytes) -> str:
    return binascii.hexlify(msg_id).decode('utf-8')


def hexxxify(msg_id:bytes) -> str:
    s = hexify(msg_id)
    return "{} {} {} {} {}".format(s[:4], s[4:12], s[12:16], s[16:28], s[28:])


class HostLocation: pass
class HostLocation:
    """
    Simple class to provide the answers to whether an IP address is
    local, a part of richmond.edu, or remote.
    """

    always_localhost = ['localhost', 
                    'localhost.localdomain', 
                    socket.gethostname(),
                    socket.getfqdn()]

    richmond_edu = [ '141.166', '192.67.49' ]

    def __init__(self, addr:Any):
        """
        addr -- anything you like, integer or string.
        """
        try:
            if addr in HostLocation.always_localhost: addr = '127.0.0.1'
            self.ip = ipaddress.IPv4Address(addr)

        except AddressValueError as e:
            self.ip = None
        

    def __bool__(self) -> bool:
        """
        returns -- True if the object is associated with a valid IPv4 address,
            and false otherwise.
        """
        return self.ip is not None


    def __str__(self) -> str:
        """
        returns -- dotted decimal format.
        """
        return self.ip.exploded


    @property
    def is_here(self) -> bool:
        """
        returns -- whether we are talking about this machine.
        """
        return bool(self) and self.ip.is_loopback

    
    @property
    def is_richmond(self) -> bool:
        """
        returns -- whether the address is a part of richmond.edu, including
            private IP addresses.
        """
        return bool(self) and any(
            [self.ip.is_private] +
            [self.ip.exploded.startswith(_) for _ in HostLocation.richmond_edu] 
            )


    @property
    def is_remote(self) -> bool:
        """
        returns -- True if this is a routable, public address, not part of richmond.edu.
            Note that the underlying is_global property excludes things like link-local
            addresses.
        """
        return self.ip.is_global and not self.is_richmond()
 

####
# I
####

def in_production(g:Dict=None) -> bool:
    """ 
    This function should be used to determine if the caller is a part of a 
    process that is running in the environment we designate as PRODUCTION.
    The rules for running in production are tighter, and how this is determined
    may change in the future. Therefore, do not make a simple hard-coded
    comparison of values.
    """
    if g is not None:
        return g.env.get('this_env', 'test').upper() == 'PROD'
    return False


def is_canoe_code(s:bytes) -> Tuple[bool, bytes]:
    """
    In keeping with the way ELF program headers are done, compiled
    Canøe object code will also have a well-known header. 

    Bytes 0-4: Canoe 43 61 6e 6f 65 
    Bytes 5-6: The major number as characters (usually the "year"): 32 30 
    Bytes 7:   The minor number (usually a lower case letter, early in the alphabet): 65
    Bytes 8-9: Reserved for future use, and set to zeros at this time: 30 30

    Note that the purpose is not to conceal Canøe's object code, but to make
    sure that anything handed to Canøe's executive to execute is the real thing.
    Passing Canøe's object code to some other program is not a security risk.
    """

    has_canoe   = s[0:5] == b'canoe'
    has_year    = s[5] in b'23456789' and s[6] in b'0123456789'
    has_release = s[7] in b'abcdefghijklmnopqrstuvwxyz'
    has_padding = all(_ == 48 for _ in s[8:9])

    if all((has_canoe, has_year, has_release, has_padding)):
        return True, b'BZh91AY&SY' + s[10:]
    else:
        tombstone(f"{has_canoe=} {has_year=} {has_release=} {has_padding=} ::: {s[:20]}")
        return False, s
        


def is_PDF(o:Union[bytes,str]) -> bool:
    """
    Determine if a file is a PDF file or something else.

    o -- as a str, it is interpreted to be a filename; if bytes,
        we assume it is the first part of some file-like data
        object.

    returns True if the file or data start with %PDF-1.
    """

    shred = None
    if isinstance(o, str):
        with open(o) as f:
            shred = bytes(f.readline()[:7])
    else:
        shred = o[:7]
    return shred == b'%PDF-1.'


def is_phone_number(s:str) -> bool:
    """ Determines if s could be a USA phone number. """

    return len(s) == 10 and s.isdigit()


def iso_time(seconds:int) -> str:
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(seconds))


def iso_seconds(timestring:str) -> int:
    dt = datetime.datetime.strptime(timestring, '%Y-%m-%dT%H:%M')
    return dt.strftime("%s")

###
# K
###
class Konstants(enum.IntEnum):
    YEAR_IN_SECONDS = 31556952


####
# L
####

def lines_in_file(filename:str) -> int:
    """
    Count the number of lines in a file by a consistent means.
    """
    if not os.path.isfile(filename): return 0

    try:
        count = int(subprocess.check_output([
            "/bin/grep", "-c", os.linesep, filename
            ], universal_newlines=True).strip())
    except subprocess.CalledProcessError as e:
        tombstone(str(e))
        return 0
    except ValueError as e:
        tombstone(str(e))
        return -2
    else:
        return count
    

def listify(x:object) -> List[object]:
    """ 
    change a single element into a list containing that element, but
    otherwise just leave it alone. 
    """
    try:
        if not x: return[]
    except NameError as e:
        return []
    return x if isinstance(x, list) else [x]


####
# M
####

def make_dir_or_die(dirname:str, mode:int=0o700) -> None:
    """
    Do our best to make the given directory (and any required 
    directories upstream). If we cannot, then die trying.
    """

    dirname = expandall(dirname)

    try:
        os.makedirs(dirname, mode)

    except FileExistsError as e:
        # It's already there.
        if not os.path.isdir(dirname): 
            raise NotADirectoryError('{} is not a directory.'.format(dirname)) from None
            sys.exit(os.EX_IOERR)

    except PermissionError as e:
        # This is bad.
        tombstone()
        tombstone("Permissions error creating/using " + dirname)
        sys.exit(os.EX_NOPERM)

    if (os.stat(dirname).st_mode & 0o777) < mode:
        tombstone("Permissions on " + dirname + " less than requested.")


def make_IN_clause(a:Union[List,str]) -> str:
    """ Changes the argument, often a list, into an 'IN (e, e, e)' clause

    a -- a value, or a list of more than one value. The type of a is
        irrelevant.
    returns: -- An SQL fragment suitable for inclusion in an SQL statement.

    >>> make_IN_clause('george')
    " IN ('george') "

    >>> make_IN_clause(['george', 'flanagin'])
    " IN ('george','flanagin') "
    """

    if not isinstance(a, list): a = [a]
    return " IN (" + (",".join([q(_) for _ in a])) + ") "


def me() -> str:
    return getpass.getuser()


class MessageTable(dict):

    def __missing__(self, k:object) -> str:
        return f'Unknown key {k}'  

    def __setitem__(self, k:object, v:object=None) -> None:
        raise Exception("This object is immutable.")


def mdays(urcal:dict) -> sortedcontainers.SortedList:
    """
    The urcal is a dict with two lists attached to the keys bizdays and 
    holidays. We need to build up a sorted list of the non-holiday bizdays
    involved.
    """
    start = urdate()
    urcal['holidays'] = [ urdate(dateutil.parser.parse(_)) for _ in urcal['holidays']]

    """
    Let's start ten days before now, and go a year and change out
    into the future. We have to start a little before today to take care 
    of the recipes that might be starting now, and have a date modification
    that takes them to yesterday, or last Friday, etc.
    """
    return sortedcontainers.SortedList([ _ for _ in range(start-10, start+400) 
        if _ % 7 in urcal['bizdays']
        and _ not in urcal['holidays']])


def memavail() -> float:
    """
    Return a fraction representing the available memory to run
    new processes.
    """
    with open('/proc/meminfo') as m:
        info = [ _.split() for _ in m.read().split('\n') ]
    return float(info[2][1])/float(info[0][1])


def mymem() -> int:
    info = psutil.Process(os.getpid())
    with info.oneshot():
        return info.memory_full_info().uss


####
# N
####

def name_from_dirname(s:str) -> str:
    return os.path.basename(os.path.normpath(s))


def new_serial_number() -> str:
    """
    return a new serial number in the form yyyymmdd:pid:hash, where
    pid is this process's PID, and the hash is the hash of the current 
    microtime.
    """
    ahora  = datetime.datetime.today()
    hasher = hashlib.sha1()
    hasher.update(bytes(f"{time.time()}", 'utf-8'))
    suffix = hasher.hexdigest()[:4]
    return "{}{:0>2}{:0>2}.{:0>5}.{}".format(ahora.year, ahora.month, ahora.day, os.getpid(), suffix)


def normalize_phone_number(s: str) -> str:
    """ Remove the non-digits from the string. """
    t = []
    for c in s:
        if c in list(string.digits): t.append(c)
    return ''.join(t)


def notlikeanyof(search_term:str, sock_drawer:Iterable) -> bool:
    """ 
    Look for something in the sock_drawer that is like the sock you have. 
    """
    try:
        return not any(_sock in search_term for _sock in sock_drawer)
    except TypeError as e:
        tombstone(f"{sock_drawer} is of type {type(sock_drawer)}, and not iterable.")
        raise NotImplementedError from None


def nothinglikeit(search_term:str, sock_drawer:Iterable) -> bool:
    """ 
    A crude implementation of "not any". This function decides if your sock
    is sorta-like any of the socks.  
    """
    try:
        return not any(_sock.startswith(search_term) for _sock in sock_drawer)
    except TypeError as e:
        tombstone(f"{sock_drawer} is of type {type(sock_drawer)}, and not iterable.")
        raise NotImplementedError from None


def now_as_seconds() -> int:
    return time.clock_gettime(0)


def now_as_string() -> str:
    """ Return full timestamp for printing. """
    return datetime.datetime.now().isoformat()[:21].replace('T',' ')


####
# P
####

def path_join(dir_part:str, file_part:str) -> str:
    """
    Like os.path.join(), but trapping the None-s and replacing
    them with appropriate structures.
    """
    if dir_part is None:
        tombstone("trapped a None in directory name")
        dir_part = ""

    if file_part is None:
        tombstone("trapped a None in filename")
        file_part = ""

    dir_part = os.path.expandvars(os.path.expanduser(dir_part))
    file_part = os.path.expandvars(os.path.expanduser(file_part))
    return os.path.join(dir_part, file_part)
 

def parse_proc(pid:int) -> dict:
    """
    Parse the proc file for a given PID and return the values
    as a dict with keys set to lower without the "vm" in front,
    and the values converted to ints.
    """
    lines = []
    proc_file = '/proc/'+str(pid)+"/status"
    with open(proc_file, 'r') as f:
        rows = f.read().split("\n")

    if not len(rows): return None

    interesting_keys = ['VmSize', 'VmLck', 'VmHWM', 
            'VmRSS', 'VmData', 'VmStk', 'VmExe', 'VmSwap' ]

    kv = {}
    for row in rows:
        if ":" in row:
            k, v = row.split(":", 1)
        else:
            continue
        if k in interesting_keys: 
            kv[k.lower()[2:]] = int(v.split()[0])

    return kv


def parse_schedule(s:str) -> List[Set]:
    """
    Changes a crontab type schedule descriptor into a list of sets,
    where each set contains the matching values for each schedule
    element.

    Big change on 16 March 2017. We now handle @canoe directives.
    """
    s = s.strip()
    if not s: return []

    try:
        cron_parser = croniter.croniter(s)
        return [ setify(_) for _ in cron_parser.expanded ]
    except Exception as e:
        return canoe_schedule(s)


def parse_schedules(scheds:List[str]) -> List[List[Set]]:
    """
    Iteratively call parse_schedule, and return a list of
    lists of sets.  i.e., 
    
        [
            [ {0}, {0}, {1}, {1}, {1} ],
            [ {0,30}, ... ]
        ]
    """
    parsed = []
    if not scheds: return parsed

    for sched in listify(scheds): 
        result = parse_schedule(sched)
        if all(isinstance(_, set) for _ in result):
            parsed.append(result)
        else:
            parsed.extend([ _ for _ in result if _ ])
        
    return parsed


def parse_user_input(s:str, transformation:int=0) -> Tuple[List[str], bool]:
    """
    Do a whitespace parse on the input, and return whether there was
    anything other than one string.

    s -- a string, presumably typed in by the user.
    transformation -- a bit mask, prescribing appropriate changes
        1 -> to lower case
        2 -> to ASCII
    
    returns -- tuple(list, bool) 
                The list is a (possibly empty) collection of tokens,
                _nand the bool has these meanings:
                True -- there was exactly one token
                False -- there was more than one token
                None -- there was nothing, or an empty string.
    """
    tokens = []
    nothing_worth_doing = None
    try:
        if s is None: return tokens, nothing_worth_doing
        s = s.strip()
        if not len(s): return tokens, nothing_worth_doing
        s = s.lower() if transformation & 1 else s
        s = s.decode('utf-8').encode('ascii') if transformation & 2 else s
        if not s: tokens, nothing_worth_doing

        tokens = shlex.split(s)
        nothing_worth_doing = len(tokens) == 1 and tokens[0] == s
    except Exception as e:
        tombstone(type_and_text(e))
    finally:
        return tokens, nothing_worth_doing


def parse_JSON_file(filename: str):
    """ strip bash style comments from an otherwise JSON file. """
    json_string = []
    for line in open(filename, 'r').readlines().strip():
        if not line or line[0] == '#': continue
        json_string.append(line)

    return json.loads(''.join(json_string))


def pids_of(process_name:str, anywhere:Any=None) -> list:
    """
    Canøe is likely to have more than one background process running, 
    and we will only know the first bit of the name, i.e., "canoed".
    This function gets a list of matching process IDs.

    process_name -- a text shred containing the bit you want 
        to find.

    anywhere -- unused argument, maintained for backward compatibility.

    returns -- a possibly empty list of ints containing the pids 
        whose names match the text shred.
    """
    results = subprocess.run(['pgrep','-u', 'canoe'], stdout=subprocess.PIPE)
    return [ int(_) for _ in results.stdout.decode('utf-8').split('\n') if _ ]


###
# Q
###

Q1="'"
Q2='"'
Q3="`"
BACKSLASH = "\\"

def q0(ins:str, esc=None) -> str:
    """
    Do nothing.
    """
    return ins

def q1(ins:str, esc=None) -> str:
    """
    ANSI SQL 99 single quoting. No escaping.
    """
    return Q1 + re.sub(Q1,  Q1+Q1, ins) + Q1


def q2(ins:str, esc:str=BACKSLASH) -> str:
    """
    Ordinary double quotes, escaped within.
    """
    return Q2 + ins.replace(Q2, esc+Q2) + Q2


def q3(ins:str, esc:str=BACKSLASH) -> str:
    """
    Back tick quoting (think bash evaluations)
    """
    return Q3 + ins.replace(Q3, esc+Q3) + Q3


def q4(ins:str, esc=None) -> str:
    """
    Microsoft PowerShell quoting. It is rather Byzantine,
    and there is no escaping.
    """
    return NotImplemented


def q5(ins:str, esc:str=BACKSLASH) -> str:
    """
    Ordinary single quotes, escaped.
    """
    return Q1 + ins.replace(Q1, esc+Q1) + Q1


quote_strategies = [ q0, q1, q2, q3, q4, q5 ]

def q_(s:str, strategy:int=1, esc:str=BACKSLASH) -> str:
    try:
        return quote_strategies[strategy](s, esc)
    except:
        message = 'unknown quote strategy {} for string (({}))'.format(strategy, s)
        raise Exception(message)


def q(ins:str, quoteType:int=1) -> str:
    """A general purpose string quoter and Houdini (mainly for SQL)

    ins -- an input string
    quote_type -- an integer between 0 and 5. Meanings:
        0 : do nothing
        1 : ordinary single quotes.
        2 : ordinary double quotes.
        3 : Linux/UNIX backquotes.
        4 : PowerShell escape and quoting.
        5 : SQL99 escaping only.
    returns: -- some version of 's'.
    """

    quote = "'"
    if quoteType == 1:
        return quote + ins.replace("'", "''") + quote
    elif quoteType == 2:
        quote = '"'
        return quote + ins.replace('"', '\\"') + quote
    elif quoteType == 3:
        quote = '`'
        return quote + ins + quote
    elif quoteType == 4: # Powershell
        ins = re.sub('^', '^^', ins)
        ins = re.sub('&', '^&', ins)
        ins = re.sub('>', '^>', ins)
        ins = re.sub('<', '^<', ins)
        ins = re.sub('|', '^|', ins)
        ins = re.sub("'", "''", ins)
        return quote + ins + quote
    elif quoteType == 5: # SQL 99 .. quotes only.
        return quote + re.sub("'",  "''", ins) + quote
    else:
        pass
    return ins


def q64(s:str, quote_type:int=1) -> bytes:
    """ Convert to Base64 before quoting.

    s -- a string to convert to Base64.
    returns: -- same thing as q()
    """
    return b"'" + encodebytes(s.encode('utf-8')) + b"'"


def q_like(s:str) -> str:
    """ Prepend and append a %

    s -- a string
    returns: -- %s%
    """
    return q("%" + s + "%")


def q_like_pre(s:str) -> str:
    """ append a %

    s -- a string
    returns: -- s%
    """

    return q("%" + s)


def q_like_post(s:str) -> str:
    """ Append a %

    s -- a string
    returns: -- s%
    """

    return q(s + "%")


###
# O
###

class Outerloop(Exception): 
    """
    raising Outerloop indicates that you want to jump
    out of the loop you are in, and move up a level.
    """
    pass


def oracle_type_to_python(oracle_type:str, precision:int=0) -> type:
    """
    Convert an Oracle 10+ type name into something Pythonic.
    """
    oracle_type = oracle_type.upper()
    if oracle_type[:3] in ['NUM']: 
        return int if precision == 0 else float
    if oracle_type[:3] in ['BIN']: return float
    if oracle_type[:3] in ['DAT', 'TIM', 'INT']: return datetime.datetime
    if oracle_type[:3] in ['CHA', 'NCH', 'NVA', 'VAR', 'LON', 'RAW']: return str
    if oracle_type[:3] in ['ROW']: return str
    return object


####
# R
####

def random_file(name_prefix:str, *, length:int=None, break_on:str=None) -> tuple:
    """
    Generate a new file, with random contents, consisting of printable
    characters.

    name_prefix -- In case you want to isolate them later.
    length -- if None, then a random length <= 1MB
    break_on -- For some testing, perhaps you want a file of "lines."

    returns -- a tuple of file_name and size.
    """    
    f_name = None
    num_written = -1

    file_size = length if length is not None else random.choice(range(0, 1<<20))
    fcn_signature('random_string', file_size)
    s = random_string(file_size, True)

    if break_on is not None:
        if isinstance(break_on, str): break_on = break_on.encode('utf-8')
        s = s.replace(break_on, b'\n')    

    try:
        f_no, f_name = tempfile.mkstemp(suffix='.txt', prefix=name_prefix)
        num_written = os.write(f_no, s)
        os.close(f_no)
    except Exception as e:
        tombstone(str(e))
    
    return f_name, num_written
    


def random_string(length:int=10, want_bytes:bool=False, all_alpha:bool=True) -> str:
    """
    
    """
    
    s = base64.b64encode(os.urandom(length*2))
    if want_bytes: return s[:length]

    s = s.decode('utf-8')
    if not all_alpha: return s[:length]

    t = "".join([ _ for _ in s if _.isalpha() ])[:length]
    return t


def remove_empty_items(x:list) -> list:
    """ 
    Remove any empty strings and None-s, but not the zero 
    or False values. 
    """
    return list(filter(len, remove_none_items(x)))


def remove_none_items(x:list) -> list:
    """ Scrub None items only. """
    return list(filter(None.__ne__, x))


decimal_point = locale.localeconv()['decimal_point']
digits_set = frozenset("0123456789"+decimal_point)
set_zero = frozenset('0')

def remove_zeros(s:str, doit:bool, max_removals:int=1) -> str:
    """
    When performing extraction, we sometimes wind up with integers being
    converted to strings with ".0" appended. In general, this is caused by
    pandas, but there are other causes. 

    s -- the string to examine

    doit -- whether or not to scrub them.

    max_removals -- how many to remove. For example 24.0 was probably an 
        integer, but 24.0000 was probably a float. The default is 1, because
        that is the reformatting we get from pandas.
    """
    global digits_set, decimal_point, set_zero

    if not doit: return s

    # This operation only makes sense if there are exactly two strings of
    # digits separated by a decimal point. We can stop after the second 
    # split because even two is too many. 
    try:
        lhs, rhs = s.split(decimal_point, 2)
        set_lhs, set_rhs = set(lhs), set(rhs)
    except:
        return s

    # See if everything is OK
    if ( set(s) - digits_set or     # check for non-digits
         not set_rhs or             # the decimal point was not at the end.  
         len(rhs) > max_removals or # the rhs is not too long.
         set_rhs != set('0') ):     # the rhs is not all zeros.
        return s

    return lhs
    

####
# S
####

def schedule_match(t1:tuple, t2:tuple) -> bool:
    return ((t1.tm_min in t2[0]) and
            (t1.tm_hour in t2[1]) and
            (t1.tm_mday in t2[2]) and
            (t1.tm_mon in t2[3]) and
            (((t1.tm_wday+1) % 7) in t2[4]))


def set_encoder(obj:Any) -> str:
    """
    The python set type is not serializable in JSON. Convert it to 
    list first.

    If the argument is /not/ a set type, then we pass it along
    to the bog standard encoder.
    """
    if isinstance(obj, set):
        return [ _ for _ in obj ]
    return obj


def setify(obj):
    """
    If it is not a set going in, it will be coming out.
    """
    global star
    if str(obj) == '*':
        return star
    if isinstance(obj, int):
        return set([obj])  # Single item
    if isinstance(obj, list) and obj[0] == '*':
        return star
    if not isinstance(obj, set):
        obj = set(obj)
    return obj


def setproctitle(s:str) -> str:
    """
    Change the name of the current process, and return the previous
    name for the convenience of setting it back the way it was.
    """
    global libc
    old_name = getproctitle()
    if libc is not None:
        try:
            buff = create_string_buffer(len(s)+1)
            buff.value = s.encode('utf-8')
            libc.prctl(15, byref(buff), 0, 0, 0)

        except Exception as e:
            print(f"Process name not changed: {str(e)}")

    return old_name.encode('utf-8')


def signal_name(i:int) -> str:
    """
    Improve readability of signal processing. 
    """
    try:
        return f"{signal.Signals(i).name} ({signal.strsignal(i)})"
    except:
        return f"unnamed signal {i}"


def sloppy(o:object) -> SloppyDict:
    return o if isinstance(o, SloppyDict) else SloppyDict(o)


class SloppyDict(dict):
    """
    Make a dict into an object for notational convenience.
    """
    def __getattr__(self, k:str) -> object:
        if k in self: return self[k]
        raise AttributeError("No element named {}".format(k))

    def __setattr__(self, k:str, v:object) -> None:
        self[k] = v

    def __delattr__(self, k:str) -> None:
        if k in self: del self[k]
        else: raise AttributeError("No element named {}".format(k))

    def reorder(self, some_keys:list=[], self_assign:bool=True) -> SloppyDict:
        new_data = SloppyDict()
        unmoved_keys = sorted(list(self.keys()))

        for k in some_keys:
            try:
                new_data[k] = self[k]
                unmoved_keys.remove(k)
            except KeyError as e:
                pass

        for k in unmoved_keys:
            new_data[k] = self[k]

        if self_assign: 
            self = new_data
            return self
        else:
            return copy.deepcopy(new_data)       


def deepsloppy(o:dict) -> Union[SloppyDict, object]:
    """
    Multi level slop.
    """
    if isinstance(o, dict): 
        o = SloppyDict(o)
        for k, v in o.items():
            o[k] = deepsloppy(v)

    elif isinstance(o, list):
        for i, _ in enumerate(o):
            o[i] = deepsloppy(_)

    else:
        pass

    return o


class SloppyTree(dict):
    """
    Like SloppyDict(), only worse -- much worse.
    """
    def __missing__(self, k:str) -> object:
        self[k] = SloppyTree()
        return self[k]

    def __getattr__(self, k:str) -> object:
        return self[k]

    def __setattr__(self, k:str, v:object) -> None:
        self[k] = v

    def __delattr__(self, k:str) -> None:
        if k in self: del self[k]


def snooze(n:int) -> int:
    """
    Calculate the delay. The formula is arbitrary, and can
    be changed.

    n -- how many times we have tried so far.

    returns -- a number of seconds to delay
    """
    num_retries = 10
    delay = 10
    scaling = 1.2

    if n == num_retries: return None
    nap = delay * scaling ** n
    tombstone('Waiting {} seconds to try again.'.format(nap))
    time.sleep(nap)
    return nap


def squeal(s: str=None, rectus: bool=True, source=None) -> str:
    """ The safety pig will appear when there is trouble. """
    tombstone(str)
    return

    for raster in pig:
        if not rectus:
            print(raster.replace(RED, "").replace(LIGHT_BLUE, "").replace(REVERT, ""))
        else:
            print(raster)

    if s:
        postfix = " from " + source if source else ''
        s = (now_as_string() +
             " Eeeek! It is my job to give you the following urgent message" + postfix + ": \n\n<<< " +
            str(s) + " >>>\n")
    tombstone(s)
    return s


def stalk_and_kill(process_name:str) -> bool:
    """
    This function finds other processes who are named canoed ... and
    kills them by sending them a SIGTERM.

    returns True or False based on whether we assassinated our 
        ancestral impostors. If there are none, we return True because
        in the logical meaning of "we got them all," we did.
    """

    tombstone('Attempting to remove processes beginning with ' + process_name)
    # Assume all will go well.
    got_em = True

    for pid in pids_of(process_name, True):
        
        # Be nice about it.
        try:
            os.kill(pid, signal.SIGTERM)
        except:
            tombstone("Process " + str(pid) + " may have terminated before SIGTERM was sent.")
            continue

        # wait two seconds
        time.sleep(2)
        try:
            # kill 0 will fail if the process is gone
            os.kill(pid, 0) 
        except:
            tombstone("Process " + str(pid) + " has been terminated.")
            continue
        
        # Darn! It's still running. Let's get serious.
        os.kill(pid, signal.SIGKILL)
        time.sleep(2)
        try:
            # As above, kill 0 will fail if the process is gone
            os.kill(pid, 0)
            tombstone("Process " + str(pid) + " has been killed.")
        except:
            continue
        tombstone(str(pid) + " is obdurate, and will not die.")
        got_em = False
    
    return got_em


class Stopwatch:
    """
    Note that the laps are an OrderedDict, so you can name them
    as you like, and they will still be regurgitated in order
    later on.
    """
    conversions = {
        "minutes":(1/60),
        "seconds":1,
        "tenths":10,
        "deci":10,
        "centi":100,
        "hundredths":100,
        "milli":1000,
        "micro":1000000
        }

    def __init__(self, *, units:Any='milli'):
        """
        Build the Stopwatch object, and click the start button. For ease of
        use, you can use the text literals 'seconds', 'tenths', 'hundredths',
        'milli', 'micro', 'deci', 'centi' or any integer as the units. 

        'minutes' is also provided if you think this is going to take a while.

        The default is milliseconds, which makes a certain utilitarian sense.
        """
        try:
            self.units = units if isinstance(units, int) else Stopwatch.conversions[units]
        except:
            self.units = 1000

        self.laps = collections.OrderedDict()
        self.laps['start'] = time.time()    


    def start(self) -> float:
        """
        For convenience, in case you want to print the time when
        you started.

        returns -- the time you began.
        """

        return self.laps['start']


    def lap(self, event:object=None) -> float:
        """
        Click the lap button. If you do not supply a name, then we
        call this event 'start+n", where n is the number of events 
        already recorded including start. 

        returns -- the time you clicked the lap counter.
        """
        if event:
            self.laps[event] = time.time()
        else:
            event = 'start+{}'.format(len(self.laps))
            self.laps[event] = time.time()

        return self.laps[event]
    

    def stop(self) -> float:
        """
        This function is a little different than the others, because
        it is here that we apply the scaling factor, and calc the
        differences between our laps and the start. 

        returns -- the time you declared stop.
        """
        return_value = self.laps['stop'] = time.time()
        diff = self.laps['start']
        for k in self.laps:
            self.laps[k] -= diff
            self.laps[k] *= self.units
            
        return return_value


    def __str__(self) -> str:
        """
        Facilitate printing nicely.

        returns -- a nicely formatted list of events and time
            offsets from the beginning:

        Units are in sec/1000
        ------------------
        start     :  0.000000
        lap one   :  10191.912651
        start+2   :  15940.931320
        last lap  :  27337.829828
        stop      :  31454.867363

        """
        # w is the length of the longest event name.
        w = max(len(k) for k in self.laps)

        # A clever print statement is required.
        s = "{:" + "<{}".format(w) + "}  : {: f}"
        header = "Units are in sec/{}".format(self.units) + "\n" + "-"*(w+20) + "\n"

        return header + "\n".join([ s.format(k, self.laps[k]) for k in self.laps ])


####
# T
####

def this_function():
    """ Takes the place of __function__ in other languages. """

    return inspect.stack()[1][3]


def this_is_the_time(current_minute:int, schedule:list) -> bool:
    """
    returns True if *now* is in the schedule, False otherwise.
    """
    
    t = crontuple_now(current_minute)
    return ((t.minute in schedule[0]) and
            (t.hour in schedule[1]) and
            (t.day in schedule[2]) and
            (t.month in schedule[3]) and
            (t.isoweekday() % 7 in schedule[4]))


def this_line(level: int=1, invert: bool=True) -> int:
    """ returns the line from which this function was called.

    level -- generally, this value is one, meaning that we
    want to use the stack frame that is one-down from where we
    are. In some cases, the value "2" makes sense. Take a look
    at CanoeObject.set_error() for an example.

    invert -- Given that the most common use of this function
    is to generate unique error codes, and that error codes are
    conventionally negative integers, the default is to return
    not thisline, but -(thisline)
    """
    cf = inspect.stack()[level]
    f = cf[0]
    i = inspect.getframeinfo(f)
    return i.lineno if not invert else (0 - i.lineno)


def time_match(t, set_of_times:list) -> bool:
    """
    Determines if the datetime object's parts are all in the corresponding
    sets of minutes, hours, etc.
    """
    return   ((t.minute in set_of_times[0]) and
              (t.hour in set_of_times[1]) and
              (t.day in set_of_times[2]) and
              (t.month in set_of_times[3]) and
              (t.weekday() in set_of_times[4]))


def tombstone(args:Any=None, silent:bool=False) -> Tuple[int, str]:
    """
    Print out a message, data, whatever you pass in, along with
    a timestamp and the PID of the process making the call. 
    Along with printing it out, it returns it.

    if silent, we return the formatted string, but do not print.
    """

    i = str(AX()).rjust(4,'0')
    a = i + " " + now_as_string() + " :: " + str(os.getpid()) + " :: "

    if not silent: sys.stderr.write(a)
    if isinstance(args, str) and not silent:
        sys.stderr.write(args + "\n")
    elif isinstance(args, list) or isinstance(args, dict) and not silent:
        sys.stderr.write("\n")
        for _ in args:
            sys.stderr.write(str(_) + "\n")
        sys.stderr.write("\n")
    else:
        pass
        # p = pp.PrettyPrinter(indent=4, width=512, stream=sys.stderr)
        # p.pprint(formatted_stack_trace())

    if not silent: sys.stderr.flush()

    # Return the info for use by CanoeDB.tombstone()
    return i, a+str(args)
    

std_ignore = [ signal.SIGCHLD, signal.SIGHUP, signal.SIGINT, signal.SIGPIPE, signal.SIGUSR1, signal.SIGUSR2 ]
allow_control_c = [ signal.SIGCHLD, signal.SIGPIPE, signal.SIGUSR1, signal.SIGUSR2 ]
std_die = [ signal.SIGQUIT, signal.SIGABRT ]
def trap_signals(ignore_list:list=std_ignore,
                 die_list:list=std_die):
    """
    There is no particular reason for these operations to be in a function,
    except that if this code moves to Windows it makes sense to isolate
    them so that they may better recieve the attention of an expert.
    """
    global bad_exit
    atexit.register(bad_exit)
    for _ in std_ignore: signal.signal(_, signal.SIG_IGN)
    for _ in std_die: signal.signal(_, bad_exit)

    tombstone("signals hooked.")



def type_and_text(e:Exception) -> str:
    """
    This is not the most effecient code, but by the time this function
    is called, something has gone wrong and performance is unlikely
    to be a relevant point of discussion.
    """
    exc_type, exc_value, exc_traceback = sys.exc_info()
    a = traceback.extract_tb(exc_traceback)
    
    s = []
    s.append("Raised " + str(type(e)) + " :: " + str(e))
    for _ in a:
        s.append(" at file/line " + 
            str(_[0]) + "/" + str(_[1]) + 
            ", in fcn " + str(_[2]))

    return s


####
# U
####

def unwhite(s: str) -> str:
    """ Remove all non-print chars from string. """
    t = []
    for c in s.strip():
        if c in string.printable:
            t.append(c)
    return ''.join(t)


UR_ZERO_DAY = datetime.datetime(1830, 8, 1)
def urdate(dt:datetime.datetime = None) -> int:
    """
    This is something of a pharse. Instead of calculating days from 
    1 Jan 4713 BCE, I decided to create a truly UR calendar starting
    from 1 August 1830 CE. After all, no dates before then could be 
    of any importance to us. 

    Why August? August 1 1830 was a Sunday, so we don't have to do
    anything fancy to get day of the week. For any urdate, urdate%7 
    is the weekday where Sunday is a zero.
    """
    if dt is None: dt = datetime.datetime.today()
    return (dt - UR_ZERO_DAY).days
    

####
# V
####

def valid_item_name(s:str) -> bool:
    """ Determines if s is a valid item name (according to Oracle)

    s -- the string to test. s gets trimmed of white space.
    returns: - True if this is a valid item name, False otherwise.
    """
    return re.match("^[A-Za-z_.]+$", s.strip()) != None


def version(full:bool = True) -> str:
    """
    Do our best to determine the git commit ID ....
    """
    try:
        v = subprocess.check_output(
            ["/opt/rh/rh-git218/root/usr/bin/git", "rev-parse", "--short", "HEAD"],
            universal_newlines=True
            ).strip()
        if not full: return v
    except:
        v = 'unknown'
    else:
        mods = subprocess.check_output(
            ["/opt/rh/rh-git218/root/usr/bin/git", "status", "--short"],
            universal_newlines=True
            ) 
        if mods.strip() != mods: 
            v += (", with these files modified: \n" + str(mods))
    finally:
        return v
        

####
# W
####

def wall(s: str):
    """ Send out a notification on the system. """
    return subprocess.call(['wall "' + s + '"'])

def whoami() -> None:
    """
    Prints the thread id, and the PID of the console you are currently running.

    The purpose of this function is to make it clear which process is sending
    which message in this multi-threaded and multi-processing environment.
    """
    tombstone("Thread id is {} and the PID is {}.".format(threading.get_ident(), os.getpid()))
    tombstone("User is {}, and this CPU is known as {}.".format(me(), socket.gethostname().replace('_','.')))
    tombstone("$CANOE_HOME is {}.".format(os.environ.get('CANOE_HOME', 'not set')))
    tombstone("The git commit ID is {}".format(version()))


def urutils_main(): return 'hello world'


if __name__ == "__main__":
    assert(is_phone_number("8043992699") == True)
    assert(is_phone_number("80IBURNEXA") == False)
    assert(normalize_phone_number("+1.804.399.2699") == "18043992699")

    try:
        raise Exception("yo! this is a test of dump_exception.", this_line())
    except Exception as e:
        print(dump_exception(e))

    try:
        x = get_ssh_host_info()
    except Exception as e:
        print(dump_exception(e))
    else:
        for k in x.get_hostnames():
            print(k + "=>" + str(x.lookup(k)))


    print(str(get_ssh_host_info('mate')))

    #Lots of datetime tests with date_filter

    #Utility function used for following date filter tests
    def text_is(t,s):
        f = date_filter(t)
        print(f)
        return f == s

    print("\n--- Begin Date Filter Tests ---")
    today = datetime.datetime.today()
    YYYY = today.strftime("%Y")
    Yq = today.strftime("%-y")
    MM = today.strftime("%m")
    Mq = today.strftime("%-m")
    bbb = today.strftime("%b").upper()
    DD = today.strftime("%d")
    Dq = today.strftime("%-d")
    hh = today.strftime("%H")
    mm = today.strftime("%M")
    ss = today.strftime("%S")

    assert(text_is("File{YYYY}","File{}".format(YYYY)))
    assert(text_is("File{YYYY}.txt", "File{}.txt".format(YYYY)))
    assert(text_is("{YYYY}File","{}File".format(YYYY)))
    assert(text_is("File{Y?}","File{}".format(Yq)))
    assert(text_is("File{Y?}.txt", "File{}.txt".format(Yq)))
    assert(text_is("{Y?}File","{}File".format(Yq)))
    assert(text_is("File{MM}","File{}".format(MM)))
    assert(text_is("File{MM}.txt", "File{}.txt".format(MM)))
    assert(text_is("{MM}File","{}File".format(MM)))
    assert(text_is("File{M?}","File{}".format(Mq)))
    assert(text_is("File{M?}.txt", "File{}.txt".format(Mq)))
    assert(text_is("{M?}File","{}File".format(Mq)))
    assert(text_is("File{bbb}","File{}".format(bbb)))
    assert(text_is("File{bbb}.txt", "File{}.txt".format(bbb)))
    assert(text_is("{bbb}File","{}File".format(bbb)))
    assert(text_is("File{DD}","File{}".format(DD)))
    assert(text_is("File{DD}.txt", "File{}.txt".format(DD)))
    assert(text_is("{DD}File","{}File".format(DD)))
    assert(text_is("File{D?}","File{}".format(Dq)))
    assert(text_is("File{D?}.txt", "File{}.txt".format(Dq)))
    assert(text_is("{D?}File","{}File".format(Dq)))
    assert(text_is("File{hh}","File{}".format(hh)))
    assert(text_is("File{mm}.txt", "File{}.txt".format(mm)))
    assert(text_is("{mm}File","{}File".format(mm)))
    assert(text_is("File{mm}","File{}".format(mm)))
    assert(text_is("File{ss}.txt", "File{}.txt".format(ss)))
    assert(text_is("{ss}File","{}File".format(ss)))
    assert(text_is("File{ss}","File{}".format(ss)))

    print("")
    print("Sanity checks passed")

