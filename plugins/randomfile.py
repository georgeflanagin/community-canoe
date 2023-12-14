# -*- coding: utf-8 -*-
""" 
Canøe VM component to handle 'file' transfers to destinations (including
localhost.
"""

import typing
from   typing import *

# System imports

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
import shutil
import tombstone as tomb
import urbox as ux
import urpacker
import urutils as uu

if not uu.in_production():
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
def randomfile_main(opcodes:list) -> ERROR_ACTION:
    """
    Create a random file as specified. 

    opcodes -- a collection of Canøe opcodes.

    returns -- ERROR_ACTION.proceed if everything goes according to plan, and
        another ERROR_ACTION object if things go awry.
    """
    stats = canoestats.default()
    mytype = 'xforms_custom'
    for i, subroutine in enumerate(opcodes):
        myname = uu.name_from_dirname(opcodes.local_dir)
        if i == 0: stats.update(myname, mytype, LED.ON)
        subroutine.on_error = ERROR_ACTION(subroutine.on_error)
        try:
            filename, bytes_written = uu.random_file(subroutine.prefix)
            shutil.copy2(filename, 
                os.path.join(
                    subroutine.local_dir, 
                    uu.date_filter(subroutine.output))
                    )
            os.unlink(filename)
            stats.inc(myname, mytype)

        except Exception as e:
            if subroutine.on_error is ERROR_ACTION.proceed: 
                stats.update(myname, mytype, LED.YELLOW)
                continue

            elif subroutine.on_error is ERROR_ACTION.skip: 
                stats.update(myname, mytype, LED.YELLOW)
                return ERROR_ACTION.proceed

            else: 
                stats.update(myname, mytype, LED.RED)
                return subroutine.on_error
    
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
    
    
