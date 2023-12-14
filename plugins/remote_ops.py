# -*- coding: utf-8 -*-
""" 
Canøe VM component to handle operations on other machines.
"""

import typing
from   typing import *

# System imports

import json
import os
import os.path
import sys

# Installed imports

# Canoe imports

import canoestats
from   canoestats import LED
import fname
from   grammar import *
import hop
from   pluginlib import *
import tombstone as tomb
import urbox as ux
import urdb
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

global debugging

@trap
def remote_ops_main(opcodes:list) -> ERROR_ACTION:
    """
    execute commands on this machine or others. 

    opcodes -- a collection of Canøe opcodes.

    returns -- ERROR_ACTION.proceed if everything goes according to plan, and
        another ERROR_ACTION object if things go awry. Note: ERROR_ACTION.skip is
        always consumed -- never returned -- because it results in the raise of
        the OuterLoop exception which is converted to a continue.    
    """
    context = uu.SloppyDict(dict.fromkeys(['temp_fs', 'selected_columns', 'result', 'rows']))
    stats = canoestats.default()
    mytype = ''
    tomb.tombstone(">>>>>>>> REMOTE_OPS")

    for subroutine in (uu.deepsloppy(_) for _ in opcodes):
        debugging = subroutine.debug
        myname = uu.name_from_dirname(subroutine.local_dir)
        subroutine.on_error = ERROR_ACTION(subroutine.on_error)
        try:
            if 'db' in subroutine:
                rdb = urdb.URdb(subroutine.db)
                debugging and uu.tombstone("database opened.")
                for op in subroutine.ops:
                    is_select = op.upper().startswith('SELECT')
                    mytype = 'db_reads' if is_select else 'db_writes'
                    stats.update(myname, mytype, LED.ON)
                    try:
                        tomb.tombstone(op)
                        result = rdb.execute_SQL(uu.date_filter(op))
                        tomb.tombstone('DB operation completed.')
                        if result is None:
                            debugging and uu.tombstone("Error in DB operation.")
                            stats.update(myname, mytype, LED.RED)
                            return ERROR_ACTION.cleanup

                        if not result and test_empty(result, subroutine.on_error):
                            debugging and uu.tombstone("No data. Stopping.")
                            stats.update(myname, mytype, LED.GREEN)
                            return ERROR_ACTION.stop

                        selected_columns = rdb.selected_columns

                        if is_select:
                            packer = urpacker.URpacker()
                            t = uu.path_join(subroutine.local_dir, 'tempfile')
                            packer.attachIO(t)
                            message = f'Query returned {len(result)} rows.'
                            stats.update(myname, mytype, LED.GREEN)
                            packer.write(result)
                            tcols = uu.path_join(subroutine.local_dir, 'tempfile.columns')
                            trows = uu.path_join(subroutine.local_dir, 'tempfile.rows')
                            with open(tcols, 'w+') as tc:
                                tc.write("|".join(selected_columns))
                            with open(trows, 'w+') as tr:
                                tr.write(f"{len(result)}")
                            if (subroutine.on_error is ERROR_ACTION.test_empty and
                                not len(result)): return ERROR_ACTION.stop

                        else:
                            message = f'{result} rows affected.'
                            stats.update(myname, mytype, LED.GREEN)

                        tomb.tombstone(message)

                    except Exception as e:
                        tomb.tombstone(uu.type_and_text(e))
                        stats.update(myname, mytype, LED.RED)
                        if subroutine.on_error is ERROR_ACTION.skip: 
                            debugging and uu.tombstone("Skipping to next opcode.")
                            raise OuterLoop()            

                        if subroutine.on_error in [ ERROR_ACTION.cleanup, ERROR_ACTION.stop ]: 
                            debugging and uu.tombstone("Exiting this section of the integration.")
                            return subroutine.on_error

                        if subroutine.on_error is ERROR_ACTION.crash: 
                            raise Exception().with_traceback(sys.exc_info()[2])

                    continue
                        
            elif 'host' in subroutine:
                h = hop.HOP(subroutine.host)
                mytype = 'ops_local' if h.local_hop else 'ops_remote'
                for op in subroutine.ops:
                    debugging and uu.tombstone(f"Executing {op}")
                    try:
                        if 'curl' in op:
                            result = uu.dorunrun(op, verbose=True)
                        else:
                            result = h.remote_exec(op, fail_ok(subroutine.on_error))
                        tomb.tombstone(f'result {result} from {op}')
                        stats.update(myname, mytype, LED.GREEN)

                    except Exception as e:
                        stats.update(myname, mytype, LED.RED)
                        tomb.tombstone(uu.type_and_text(e))
                        if subroutine.on_error is ERROR_ACTION.skip: raise OuterLoop()            
                        if subroutine.on_error in [ ERROR_ACTION.cleanup, ERROR_ACTION.stop ]: return subroutine.on_error
                        if subroutine.on_error is ERROR_ACTION.crash: 
                            raise Exception().with_traceback(sys.exc_info()[2])
                        continue

            else:
                uu.tombstone(f'[NOP] instruction; nothing to do.')
                return ERROR_ACTION.proceed

        except OuterLoop as e:
            continue    

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
    
