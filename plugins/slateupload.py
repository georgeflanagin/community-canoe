#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Upload files to Slate over https.
"""
import typing
from typing import *

# Built in imports

import http
import os
import requests
import sys

# Canøe imports

import canoestats
from   canoestats import LED
from   grammar import *
import tombstone as tomb
import urutils as uu


def slateupload_main(opcodes:uu.SloppyDict) -> ERROR_ACTION:
    """
    Upload a file to Slate's drop point.
    """
    stats = canoestats.default()
    mytype = 'xforms_custom'
    myname = uu.name_from_dirname(opcodes.local_dir)
    stats.update(myname, mytype, LED.RED)

    slate_password = opcodes.password
    service_url = opcodes.url.format(opcodes.format_id)  

    try:
        # This assignment statement allows us to show the correct file name
        # in the tombstones instead of showing MMDDYYYY, etc.
        opcodes.file = uu.date_filter(opcodes.file)
        with open(opcodes.file, 'r') as f:
            data_set = f.read()
    except FileNotFoundError as fe:
        tomb.tombstone(f'Slate upload data file not found: {opcodes.file}')
        return opcodes.on_error

    try:
        resp = requests.post(service_url,
            data=data_set,
            auth=(opcodes.username,opcodes.password))

    except Exception as e:
        tomb.tombstone(uu.type_and_text(e))
        uu.tombstone(uu.type_and_text(e))
        return opcodes.on_error
    
    if resp.status_code != http.HTTPStatus.OK:
        msg = f'Slate API call failed with HTTP status {resp.status_code}'
        if resp.reason:
            msg += f'\nReason is {resp.reason}'
        tomb.tombstone(msg)
        return opcodes.on_error

    tomb.tombstone(f"{opcodes.file} uploaded successfully.")
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
        tomb.tombstone("ERROR_ACTION is {}".format(error_action))
        sys.exit(os.EX_OK if not error_action else os.EX_DATAERR)

    except KeyError as e:
        print("The program {} has no opcodes for the {} operation.".format(sys.argv[-1], vm_function))

    except Exception as e:
        print(uu.type_and_text(e))

    finally:
        print(80*'=')
    
