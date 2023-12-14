#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from typing import *

""" 
Lee Parker's Canøe dashboard, in ASCII format. This is a 
debugging tool for the web version.
"""

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2020, University of Richmond'
__credits__ = None
__version__ = '1.0'
__license__ = 'https://www.gnu.org/licenses/gpl.html'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Working Prototype'
__required_version__ = (3, 6)


__license__ = 'MIT'
import license

# Standard imports

import argparse
import contextlib
import os
import os.path
import pdb
import signal
import sys

if sys.version_info < __required_version__:
    print("This software requires Python " + str(__required_version__))
    sys.exit(os.EX_SOFTWARE)

# Installed imports

# Canøe imports
import canoestats
from   canoestats import LED
from   canoestats import dash_devices
import grammar
from   grammar import ERROR_ACTION
import tombstone as tomb
import urpacker
from   urdecorators import show_exceptions_and_frames as trap
import urutils as uu

file_header = """### Dashboard generated `{}`\n
For additional information see the document `dashboard.explanation.md` in this folder."""

dash_columns = uu.SloppyDict({
    "Job Name":"The name Canøe uses for the integration.",
    "RD":"DataBase Read operations.",
    "WR":"DataBase Write operations.",
    "LOC":"Operations that are LOCal to Canøe.",
    "REM":"Operations executed on REMote hosts.",
    "IN":"INbound files collected.",
    "OUT":"OUTbound files delivered.",
    "STD":"Data Transforms that are Standard (csv, xml, encrypt, decrypt).",
    "PLG":"Data Transforms that are Custom (i.e., plugins).",
    "Last Exec":"Local time that the last operation was completed.",
    "S/N":"Unique Serial Number of the most recent run.",
    "FRQ":"""Frequency of operation symbols: 
    5 -> weekday        7 -> daily          H -> hourly         M -> monthly 
    & -> often          ! -> manually       ? -> unknown        W -> weekly""",
    " ":"""Operation indicators:
    . -> success        * -> minor error    # -> major error
    @ -> waiting        w -> wait timeout   r -> required file missing  
    e -> emptyfile found """
    })

format_str = """{:<28} {: ^3} {: ^3} {: ^3} {: ^3} {: ^3} {: ^3} {: ^3} {: ^3} {:^16} {:^10}  {:^3}"""

"""
          1         2         3         4         5         6         7         8         9         
0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
====================================================================================================
Job Name                       RD  WR  LOC REM IN  OUT STD PLG      Last Run        S/N     FRQ
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ a
alma                            .   .   .   .   .   .   .   .  2020-10-07 05:01  32365.75da  7 
"""

debugging = False

@trap
def dashboard_main(my_args) -> int:
    """
    Select the current database contents, and display them. 
    """
    global file_header
    global debugging

    my_args = uu.SloppyDict(my_args)   
    db = canoestats.CanoeStats(my_args.db)
    data = db.get_jobs(my_args.order)
    debugging = my_args.debug

    myname = uu.name_from_dirname(my_args.local_dir)
    mytype = 'xforms_custom'
    db.update(myname, mytype, LED.ON)    

    output = os.path.join(my_args.local_dir, my_args.output)
    with open(output, 'w+') as f:
        with contextlib.redirect_stdout(f):
            print("# The Canøe Dashboard")
            print("\n```\n")
            for k, v in dash_columns.items():
                print(f"{k} <=> {v}")
            print("```\n")

            # Print the top of the table, labels and leading.
            print(file_header.format(uu.now_as_string()))
            print("```")
            print((my_args.width+6)*"=") 
            print(format_str.format(*dash_columns.keys()))

            alpha_order = my_args.order == 'alpha'
            c = ' a' if alpha_order else ''
            print((my_args.width+4)*"~" + c) 

            # From a to z
            for i, line in enumerate(data, start=1):

                # Init the tokens that contain each row of data (11 columns).
                tokens = [None]*12

                # See if we have switched to a new initial letter.
                if alpha_order and not line[0].startswith(c[1]):
                    c = " " + line[0][0]
                    print((my_args.width+4)*'~' + c)

                # Correctly format each column in the line.
                for j, it in enumerate(line):
                    # uu.tombstone(f"{j} {it}")
                    # Select a letter to print instead of the value.
                    tokens[j] = str(dash_devices.get(it, it))
                    if j ==  9: tokens[j] = tokens[j][:16]
                    if j == 10: tokens[j] = tokens[j][-10:]

                print(format_str.format(*tokens))
                    
            print((my_args.width+6)*"=") 
            print("```")

    db.update(myname, mytype, LED.GREEN)
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
    
