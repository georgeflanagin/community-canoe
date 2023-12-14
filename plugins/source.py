# -*- coding: utf-8 -*-
"""
Canøe VM component to handle 'file' transfers to destinations (including
localhost.
"""

import typing
from   typing import *

# System imports

import glob
import os
import os.path
import sys

# Installed imports

# Canoe imports

import canoestats
from   canoestats import Blinker
from   canoestats import LED
import fname
from   grammar import *
import hop
from   pluginlib import *
import tombstone as tomb
import urbox as ux
import urpacker
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

debugging = True

@trap
def source_main(opcodes:list) -> ERROR_ACTION:
    """
    transfer data from one of the endpoints by executing opcodes.

    opcodes -- a collection of Canøe opcodes.

    returns -- ERROR_ACTION.proceed if everything goes according to plan, and
        another ERROR_ACTION object if things go awry. Note: ERROR_ACTION.skip is
        always consumed -- never returned -- because it results in the raise of
        the OuterLoop exception which is converted to a continue.
    """
    global debugging
    mytype = 'files_in'
    tomb.tombstone(">>>>>> SOURCE")

    blinker = None
    for i, subroutine in enumerate([uu.deepsloppy(_) for _ in opcodes], start=1):
        debugging = subroutine.debug
        myname = uu.name_from_dirname(subroutine.local_dir)
        if i == 1: blinker = Blinker(myname, mytype)

        subroutine.on_error = ERROR_ACTION(subroutine.on_error)

        if os.isatty(0) or debugging: tomb.tombstone('source step #{}'.format(i))
        f = uu.date_filter(subroutine.file)
        try:
            ####### BOX #######
            if 'box' in subroutine:
                handle = ux.URBoxHOP(subroutine.box)
                debugging and uu.tombstone(uu.fcn_signature(
                    'handle.get', f, subroutine.directory, 
                    subroutine.local_dir, subroutine.overwrite
                    ))

                waiting = True
                interval = subroutine.wait.time
                until = subroutine.wait.until
                use = subroutine.wait.use
                action = None
                got_something = None

                while waiting:
                    blinker.blink(LED.WAITING)
                    # debugging and uu.tombstone("entering while loop")
                    try:
                        uu.tombstone(f"Attempting to retrieve {f} from Box")
                        num_files = handle.get(f, subroutine.directory, 
                            subroutine.local_dir, subroutine.overwrite)
                        if num_files > 0:
                            tomb.tombstone(f"Retrieved {num_files} from Box.")
                            break

                    except Exception as e:
                        tomb.tombstone(f"Runtime error {e} white attempting to retrieve {f}")
                        return subroutine.on_error

                    # uu.tombstone(f"{got_something} and {need_something}")
                    if not got_something:
                        interval, until, use = wait_or_give_up(blinker, interval, until, use)
                        waiting = until is not None
                        continue

                uu.tombstone("Waiting complete.")
                blinker.blink(LED.GREEN)

                if num_files: continue
                elif subroutine.on_error is ERROR_ACTION.proceed: continue
                elif subroutine.on_error is ERROR_ACTION.skip: raise uu.OuterLoop()
                elif test_empty(f, subroutine.on_error): return ERROR_ACTION.stop
                elif subroutine.on_error is ERROR_ACTION.crash: 
                    blinker.blink(LED.RED)
                    raise Exception().with_traceback(sys.exc_info()[2])
                return subroutine.on_error
                
            ####### S3 #######
            elif 's3' in subroutine:
                debugging and uu.tombstone(f"Attempting {subroutine.ops}")
                if uu.dorunrun(subroutine.ops, verbose=True):
                    # Have to check empty twice, once for success and once
                    # (below) for failure.
                    if test_empty(glob.glob(subroutine.local_dir + "/*"), subroutine.on_error):
                        blinker.blink(LED.GREEN)
                        return ERROR_ACTION.stop
                    else:
                        continue

                if subroutine.on_error is ERROR_ACTION.skip: raise uu.OuterLoop()
                elif subroutine.on_error is ERROR_ACTION.crash: 
                    raise Exception().with_traceback(sys.exc_info()[2])
                elif subroutine.on_error is ERROR_ACTION.proceed: continue

                # Second check of empty.
                elif test_empty(glob.glob(subroutine.local_dir + "/*"), subroutine.on_error): 
                    blinker.blink(LED.GREEN)
                    return ERROR_ACTION.stop
                return subroutine.on_error

            ####### HOST #######
            elif 'host' in subroutine:
                handle = hop.HOP(subroutine.host)
                debugging and uu.tombstone(
                    uu.fcn_signature('hop.get_file', f, subroutine.local_dir, True))

                ###
                # In canoe20d and forward, we must pay a little more attention to 
                # the result. The checks have a logical hierarchy:
                #   required -- did we get the required number of files
                #   empty    -- lets check to see if there is anything in
                #               the files.
                #   wait     -- are we being asked to retry.
                # The values are calculated "up front" and then evaluated.
                ###
                waiting = True
                interval = subroutine.wait.time
                until = subroutine.wait.until
                use = subroutine.wait.use
                action = None
                got_something = None

                while waiting:
                    blinker.blink(LED.WAITING)
                    # debugging and uu.tombstone("entering while loop")
                    try:
                        got_something = handle.get_file(f, subroutine.local_dir, True) 
                        tomb.tombstone(f"Completed operation to retrieve {f}")
                        waiting = False

                    except Exception as e:
                        tomb.tombstone(f"Runtime error {e} white attempting to retrieve {f}")
                        return subroutine.on_error

                    # One wait if we came back empty handed.
                    need_something = 0 not in subroutine.required.count
                    # uu.tombstone(f"{got_something} and {need_something}")
                    if need_something and not got_something:
                        interval, until, use = wait_or_give_up(blinker, interval, until, use)
                        waiting = until is not None
                        continue

                uu.tombstone("Waiting complete.")
                blinker.blink(LED.ON)
                action = meets_expectations(subroutine, blinker)
                if action is ERROR_ACTION.proceed:
                    # NOTE: we already checked "waiting", so we will 
                    # leave this inner while loop, and bounce on up 
                    # a level.
                    tomb.tombstone(f"Moving to next operation.")
                    continue

                elif action is ERROR_ACTION.skip: 
                    tomb.tombstone(f"No files matching {f} found. Skipping to next opcode.")
                    raise uu.OuterLoop()

                elif action is ERROR_ACTION.crash: 
                    tomb.tombstone(f"Unrecoverable error retrieving {f}.")
                    raise Exception().with_traceback(sys.exc_info()[2])

                elif test_empty(glob.glob(subroutine.local_dir + "/*"), subroutine.on_error): 
                    return ERROR_ACTION.stop
        
                else:
                    return subroutine.on_error

            else:
                tomb.tombstone('unreachable point of origin{}'.format(list(subroutine.keys())))
                return subroutine.on_error

        except uu.OuterLoop as e:
            continue

        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            blinker.blink(LED.RED)
            return ERROR_ACTION.cleanup

    blinker.blink(LED.GREEN)
    return ERROR_ACTION.proceed


if __name__ == '__main__':
    """
    Universal test program for all Canøe plugins.
    """
    if len(sys.argv) < 2: sys.exit(os.EX_DATAERR)

    loader = urpacker.URpacker()
    loader.attachIO(sys.argv[-1], s_mode='read')
    opcodes = uu.deepsloppy(loader.read())
    if not opcodes:
        tomb.tombstone("{} was not a Canøe program.".format(sys.argv[-1]))
        sys.exit(os.EX_DATAERR)

    print("\n")
    print("Compiler version {}".format(uu.compiler_info(opcodes)))
    print("Compiled on      {}".format(uu.compiled_time(opcodes)))
    print(80*'-')

    # The following awkwardness allows us to use the same test program for every
    # plugin in the virtual machine.
    # 
    # Basic name of this file, which corresponds to the name of the plugin/executive.
    vm_function = os.path.basename(__file__)[:-3]

    # The _main function's name
    vm_callable = "{}_main".format(vm_function)

    # Get our opcodes from the compiled program because these opcodes have
    # the same name as the plugin, and execute them.
    # 
    try:
        error_action = globals()[vm_callable](opcodes[vm_function])
        tomb.tombstone("ERROR_ACTION is {}".format(ERROR_ACTION(error_action).name))
        sys.exit(os.EX_OK if error_action is ERROR_ACTION.proceed else os.EX_DATAERR)

    except KeyError as e:
        print("The program {} has no opcodes for the {} operation.".format(sys.argv[-1], vm_function))

    except Exception as e:
        print(uu.type_and_text(e))

    finally:
        print(80*'=')
    
