# -*- coding: utf-8 -*-
""" 
Plugin to keep ssh keys of all kinds in sync.
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


@trap
def keymaint_main(opcodes:list) -> ERROR_ACTION:

    opcodes = uu.listify(opcodes)
    stats = canoestats.default()
    mytype = 'xforms_custom'
    working_dir = opcodes[0].local_dir
    myname = uu.name_from_dirname(working_dir)
    stats.update(myname, mytype, LED.ON)
    known_hosts_file = uu.expandall('~/.ssh/known_hosts')
    diff_file = uu.path_join(working_dir, 'keys.diff')
    out_f = uu.expandall(f"{known_hosts_file}.new")

    ###
    # If there is a link with the "new" name, remove it before
    # we start, and in any case create it and chmod it to
    # the correct permissions.
    ###
    try:
        os.unlink(out_f)

    except FileNotFoundError as e:
        # Not a problem.
        pass

    except Exception as e:
        # Big problem!
        stats.update(myname, mytype, LED.RED)
        uu.tombstone(str(e))
        return ERROR_ACTION.notify

    finally:
        uu.dorunrun(f"/usr/bin/touch {out_f}", quiet=True)
        uu.tombstone(f"Created {out_f}")
        uu.dorunrun(f"/usr/bin/chmod 640 {out_f}", quiet=True)
        uu.tombstone(f"changed permissions on {out_f} to 640")

    ###
    # Poll the known hosts for keys. Sort the host names so that we can 
    # diff them.
    ###
    for host in sorted(_ for _ in uu.get_ssh_host_info('all') if _ != '*'):
        hostname = uu.get_ssh_host_info(host).hostname
        if (code := uu.dorunrun(
                f'/usr/bin/ssh-keyscan {hostname} 2>/dev/null >> {out_f}', 
                quiet=True, return_exit_code=True)):
            uu.tombstone(f"Error {code} getting keys for {host}")
        else:
            uu.tombstone(f"Got keys for {host}")

    # Create a diff file.
    uu.dorunrun(f'/usr/bin/diff {out_f} {known_hosts_file} > {diff_file}')

    # Out with the old, and in with the new in a couple of steps.
    # 1. if it exists, remove the known_hosts.old file. Effectively,
    #    rm -f ~/.ssh/known_hosts.old
    try:
        os.unlink(f"{known_hosts_file}.old")
    except:
        pass

    # 2. Make a link from the current known_hosts file to .old Effectively,
    #    cp ~/.ssh/known_hosts ~/.ssh/known_hosts.old
    os.link(known_hosts_file, f"{known_hosts_file}.old")

    # 3. Remove the current known hosts file. Effectively,
    #    rm -f ~/.ssh/known_hosts
    os.unlink(known_hosts_file)

    # 4. Make a link to the new data named known_hosts. Effectively,
    #    cp ~/.ssh/known_hosts.new ~/.ssh/known_hosts
    os.link(out_f, known_hosts_file)

    f = fname.Fname(diff_file)

    # Set to yellow if there were changes.
    stats.update(myname, mytype, (LED.YELLOW if len(f) else LED.GREEN))
    
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
    
