# -*- coding: utf-8 -*-
""" 
Plugin template.
"""

import typing
from   typing import *

# System imports

import json
import os
import os.path
import shutil
import sys

# Installed imports

import pandas

# Canoe imports

import canoestats
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

file_header = """Transaction Unique ID|Transaction Date|Person Unique ID|Name|Amount Spent|Currency Spent|Location Spent|Expense Code|Matter Number|Business Purpose|Description|Append to Description|Extra Text|Is Firm Paid|Is Amount Disabled|Is Currency Disabled|Is Delete Disabled|Is Receipt Needed|VAT %|VAT Amount|Has VAT Receipt|User Defined Fields|User Defined Fields""".split('|')

default_column_order = """Transaction Unique ID|Transaction Date|Person Unique ID|Name|Amount Spent|Currency Spent|Location Spent|Expense Code|Matter Number|Business Purpose|Description|Append to Description|Extra Text|Is Firm Paid|Is Amount Disabled|Is Currency Disabled|Is Delete Disabled|Is Receipt Needed|VAT %|VAT Amount|Has VAT Receipt|User Defined Fields|User Defined Fields2"""

@trap
def chromefilter_main(opcodes:list) -> ERROR_ACTION:
    """
    Split a file into subfiles based on a group-by operation on some 
    column in the data.
    """
    global default_column_order
    global file_header
    stats = canoestats.default()
    mytype = 'xforms_custom'
    myname = uu.name_from_dirname(opcodes.local_dir)
    stats = Blinker(myname, mytype)
    
    if opcodes.debug: tomb.tombstone('Entering {}'.format(uu.this_function()))

    # If the file to separate starts with a /, then that's the whole name.
    # If not, we want to prepend the name of the recipe that is running
    # this task.
    original_file = ( opcodes.input 
        if opcodes.input.startswith(os.sep) else 
        uu.path_join(opcodes.local_dir, opcodes.input)
        )

    if opcodes.debug: tomb.tombstone('original_file =>> {}'.format(original_file))

    # As always, life begins with putting the data into pandas.
    p = urpacker.URpacker()
    p.attachIO(original_file, s_mode='read')
    chrome_frame = p.read(format='pandas')

    # Sort it, and set the named index column as the index.
    chrome_frame.sort_values(by=[opcodes.index], inplace=True)
    chrome_frame.set_index(keys=[opcodes.index], inplace=True, drop=False)

    # And make a list of all the index values that are present.
    index_values = chrome_frame[opcodes.index].unique()

    if opcodes.debug: tomb.tombstone('index_values =>> {}'.format(index_values))

    tomb.tombstone("index values are : {}".format(index_values))

    # Let the filtering begin. The column order will be the same in all the
    # subfiles, so we will set it before we begin.
    column_order = default_column_order
    column_order = column_order.split('|')
    # print("column order is : {}".format(column_order))
    for v in index_values:
        # print("v is : {}".format(v))
        subframe = chrome_frame.loc[chrome_frame[opcodes.index] == v]
        # print(subframe)
        # Reorder the columns if requested.
        # subframe = subframe[column_order]
        tomb.tombstone("writing {}".format(uu.path_join(opcodes.local_dir, v)))
        tomb.tombstone("columns are {}".format(column_order))
        tomb.tombstone("header is {}".format(file_header))
        subframe.to_csv(uu.path_join(opcodes.local_dir, v), 
            sep=opcodes.sep, 
            header=file_header,
            columns=column_order, 
            index=opcodes.keepindex)

    else:
        # NOTE: this branch is only executed if the for loop runs to completion.
        if opcodes.original == 'move':
            tomb.tombstone('moving file')
            shutil.move(original_file, uu.path_join('/tmp', opcodes.input))
            stats.blink(LED.GREEN)
        elif opcodes.original == 'remove':
            tomb.tombstone('removing file.')
            os.unlink(original_file)
            stats.blink(LED.GREEN)
        else:
            tomb.tombstone('original file retained.')
            stats.blink(LED.GREEN)
        return ERROR_ACTION.proceed

    # Something bad happened.
    stats.blink(LED.RED)
    return opcodes.on_error


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
    
