# -*- coding: utf-8 -*-
""" 
Description of the plugin goes here.
"""


# Standard library imports go here.

import os
import os.path
import sys
from   typing import *

# Third party imports go here.

# Canoe project imports go here. These are pre-stocked for your use
# because almost every plugin will need them. 

import canoestats
from   canoestats import LED
import fname
from   grammar import *
import pluginlib
import tombstone as tomb
import urdb
import urpacker
import urutils as uu
from   urdecorators import show_exceptions_and_frames as trap

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2021, University of Richmond'
__credits__ = None
__version__ = '0.9'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'testable'

__license__ = 'MIT'
import license

@trap
def renamepics_main(opcodes:list) -> ERROR_ACTION:

    # It is easier to always work from a list, even if the list
    # has only one thing in it.
    opcodes = uu.listify(uu.deepsloppy(opcodes))
    stats = canoestats.default()
    mytype = 'xforms_custom'
    try:
        myname = uu.name_from_dirname(opcodes[0].local_dir)
    except Exception as e:
        uu.tombstone('FATAL: Cannot determine the name of the current integration.')
        return ERROR_ACTION.notify
    
    # OK. Let's begin execution.
    stats.update(myname, mytype, LED.ON)

    for i, subroutine in enumerate(opcodes):
        p = urpacker.URpacker()
        p.attachIO(subroutine.input, s_mode='read')
        frame = p.read('pandas')
        for row in frame.iter_rows():
            present_name, new_name = subroutine.columns
            try:
                os.link(uu.path_join(subroutine.directory, present_name), 
                        uu.path_join(subroutine.output, new_name))
                os.unlink(uu.path_join(subroutine.directory, present_name))
            except Exception as e:
                stats.update(myname, mytype, LED.RED)
                uu.tombstone(f'Unable to create link for {photo_ID}')
                return ERROR_ACTION.notify

    stats.update(myname, mytype, LED.GREEN)
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
    
