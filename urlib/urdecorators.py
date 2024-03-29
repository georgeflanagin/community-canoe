# -*- coding: utf-8 -*-
import typing
from typing import *

"""
These decorators are designed to handle hard errors in operation
and provide a stack unwind and dump. The resulting file will be
named $CANOE_LOG/YYYY-MM-DD/pid. If the same pid dies more than
once the generator "increment()" will append a letter to the name.

Most of the code during development will have a line that looks
something like this:

from urdecorators import show_exceptions_and_frames as trap
# from urdecorators import null_decorator as trap

In production, we can swap the commented line for the one 
preceding it. 

"""

# System imports
import contextlib
from   functools import wraps
import inspect
import os
import sys
import types

# Canoe imports
import urutils as uu

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = 'Based on Github Gist 1148066 by diosmosis'
__version__ = '0.1'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Prototype'

__license__ = 'MIT'
import license

def increment(s:str) -> str:
    return s+'A'
    

def null_decorator(o:object) -> object:
    """
    The big nothing.
    """
    return o


def show_exceptions(func:object) -> object:
    """
    A **simple** decorator function to anticipate errors that might
    be created by the compilation process.
    """

    @wraps
    def func_wrapper(*args, **kwargs):
        error_message = ""
        try:
           return func(*args, **kwargs)

        except Exception as e:
            uu.tombstone(error_message)
            uu.tombstone(uu.type_and_text(e))
            return None

    return func_wrapper



def show_exceptions_and_frames(func:object) -> None:
    """
    An amplified version of the show_exceptions decorator.
    """

    @wraps(func)
    def wrapper(*args, **kwds):
        __wrapper_marker_local__ = None
    
        try:
            # If you uncomment the next line, you can see a flow trace.
            # uu.tombstone(uu.fcn_signature(func.__name__))
            return func(*args, **kwds)

        except Exception as e:
            print("{}".format(e))
            # Who am I?
            pid = 'pid{}'.format(os.getpid())

            # First order of business: create a dump file. The file will be under
            # $CANOE_LOG with today's ISO date string as the dir name.
            new_dir = uu.path_join(os.environ.get('CANOE_LOG'), uu.now_as_string()[:10])
            uu.make_dir_or_die(new_dir)

            # The file name will be the pid (possibly plus something like "A" if this
            # is the second time today this pid has failed).
            candidate_name = uu.path_join(new_dir, pid)
            
            uu.tombstone("writing dump to file {}".format(uu.blind(candidate_name)))

            with open(candidate_name, 'a') as f:
                with contextlib.redirect_stdout(f):
                    # Protect against further failure -- log the exception.
                    try:
                        e_type, e_val, e_trace = sys.exc_info()
                    except Exception as e:
                        uu.tombstone(uu.type_and_text(e))

                    print('Exception raised {}: "{}"'.format(e_type, e_val))
                    
                    # iterate through the frames in reverse order so we print the
                    # most recent frame first
                    for frame_info in inspect.getinnerframes(e_trace):
                        f_locals = frame_info[0].f_locals
                
                        # if there's a local variable named __wrapper_marker_local__, we assume
                        # the frame is from a call of this function, 'wrapper', and we skip
                        # it. The problem happened before the dumping function was called.
                        if '__wrapper_marker_local__' in f_locals: continue

                        # log the frame information
                        print('\n**File <{}>, line {}, in function {}()\n    {}'.format(
                            frame_info[1], frame_info[2], frame_info[3], frame_info[4][0].lstrip()
                            ))

                        # log every local variable of the frame
                        for k, v in f_locals.items():
                            try:
                                print('    {} = {}'.format(k, v))
                            except:
                                pass

                    print('\n')
            sys.exit(-1)

    return wrapper

