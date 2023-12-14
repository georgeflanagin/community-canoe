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
import sys

# Installed imports

# Canoe imports


import canoestats
from   canoestats import LED
import fname
from   grammar import *
import hop
import pluginlib
import shlex
import subprocess
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

@trap
def encryptpics_main(opcodes:uu.SloppyDict) -> ERROR_ACTION:

    stats = canoestats.default()
    mytype = 'xforms_custom'
    myname = uu.name_from_dirname(opcodes.local_dir)
    stats.update(myname, mytype, LED.ON)

    # The parameters are passed inside the opcodes. 
    command_segments = [
        f"{opcodes.exe} --encrypt --sign --force-mdc --trust-model always --yes --batch --quiet -u {opcodes.signature[0]}",
        "--recipient {}",
        f"-o {opcodes.local_dir}/{{}}.{opcodes.ext} {{}}"
        ]

    # Build the center segment now, outside the loop. It will not change
    # during execution.
    command_segments[1] = " ".join([ 
        command_segments[1].format(k) for k in opcodes.publickey 
        ])

    # first, get the list of pic-names.
    loader = urpacker.URpacker()
    if not opcodes.input.startswith(os.sep):
        opcodes.input = uu.path_join(opcodes.local_dir, opcodes.input)

    attached = loader.attachIO(opcodes.input, s_mode='read')
    if not attached: 
        uu.tombstone(f"Failed to attach {opcodes.input} for input.")
        return ERROR_ACTION.stop

    names = loader.read('pandas')[opcodes.column]
    successes = 0
    failures = 0
    missing = 0
    for i, name in enumerate(names):
        f = fname.Fname(uu.path_join(opcodes.images, name) + ".jpg")
        if not f:
            missing += 1
            uu.tombstone(f"No file named {str(f)} found.")
            continue
        
        # We should have files named {UR}i.{gpg} or similar, that represent
        # the encrypted versions of the pictures.
        seg = command_segments[2].format(i, str(f))

        ##########
        #  HERE  #
        ##########
    
        cmd = " ".join([command_segments[0], command_segments[1], seg])
        result = uu.dorunrun(cmd, verbose=True)
        if result: 
            successes += 1
            print(f"{str(f)} found and encrypted.", file=sys.stderr)
        else: 
            failures += 1
            print(cmd)
            sys.exit(-1)

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
        uu.tombstone("{} was not a Canøe program.".format(sys.argv[-1]))
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
        uu.tombstone("ERROR_ACTION is {}".format(ERROR_ACTION(error_action).name))
        sys.exit(os.EX_OK if error_action is ERROR_ACTION.proceed else os.EX_DATAERR)

    except KeyError as e:
        print("The program {} has no opcodes for the {} operation.".format(sys.argv[-1], vm_function))

    except Exception as e:
        print(uu.type_and_text(e))

    finally:
        print(80*'=')
    
