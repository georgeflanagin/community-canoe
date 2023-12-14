# -*- coding: utf-8 -*-
""" 
Plugin for inspection of pgp archives.
"""

import typing
from   typing import *

# System imports

import glob
import json
import os
import os.path
import re
import shlex
import sys

# Installed imports

# Canoe imports

import canoestats
from   canoestats import LED
from   grammar import *
import subprocess
import tombstone as tomb
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

key_phrases = uu.SloppyDict({
    "bad":(
        re.compile('^gpg: WARNING:.*$'),
        ),

    "info":(
        re.compile('^Version:.*$'),
        re.compile('^Comment:.*$')
        ),

    "good":(
        re.compile('^gpg: encrypted with.*$'),
        re.compile('^:pubkey enc packet:.*$'),
        re.compile('mdc_method: .')
        ),

    "signed":(
        re.compile('^:signature packet:.*$'),
        )
    })

exe = "{} --list-packets --verbose ".format(shutil.which('gpg'))

@trap
def pgpinspect_main(opcodes:uu.SloppyDict) -> ERROR_ACTION:
    """
    Triage the incoming files.
    """
    global key_phrases

    stats = canoestats.default()
    mytype = 'xforms_custom'

    myname = uu.name_from_dirname(opcodes.local_dir)
    stats.update(myname, mytype, LED.ON)

    # The beginning of our command to inspect the files.

    # Get a list of files to inspect.
    # gpg --list-packets xyz.txt.asc > out 2>&1
    files = glob.glob(os.path.join(opcodes.local_dir, '*'))
    files = [ f for f in files if f.endswith(('gpg', 'pgp', 'asc')) ]
    if not len(files): 
        uu.tombstone(f"INFO: no gpg archives found in {opcodes.local_dir}")
        return ERROR_ACTION.proceed
    
    for f in files:
        signed = False
        encrypted = False
        warnings = 0

        result = subprocess.run(
            shlex.split(f"{exe} {f}"), 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT
            )
        if result.returncode != 0:
            tomb.tombstone(f"INFO: {f} is not a gpg archive.")
            continue

        lines = result.stdout.decode('utf-8').split("\n")
        report = open(f"{f}.diag", "a+")

        # First look for the .info pieces
        for regexp in key_phrases.info:
            for line in lines:
                try:
                    report.write(regexp.search(line).group() + "\n")
                except AttributeError as e:
                    pass

        # Now the bad parts; we hope there are none.
        warnings = 0
        for regexp in key_phrases.bad:
            for line in lines:
                try:
                    report.write(regexp.search(line).group() + "\n")
                    warnings += 1
                except AttributeError as e:
                    pass

        # We should find these strings in the output if the file is encrypted.
        for regexp in key_phrases.good:
            for line in lines:
                try: 
                    report.write(regexp.search(line).group() + "\n")
                    encrypted = True
                except AttributeError as e:
                    pass
        
        for regexp in key_phrases.signed:
            for line in lines:
                try: 
                    report.write(regexp.search(line).group() + "\n")
                    signed = True
                except AttributeError as e:
                    pass
        
        if warnings: tomb.tombstone(f"File {f} had encryption warnings.")
        if not encrypted: tomb.tombstone(f"File {f} was not encrypted.")
        if not signed: tomb.tombstone(f"File {f} was not signed.")

        report.close()                   
        if not warnings and encrypted and signed: os.unlink(f)

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
    
