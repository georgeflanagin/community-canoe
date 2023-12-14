# -*- coding: utf-8 -*-
""" 
This plugin checks the availability of the connections known to 
the Canøe installation on this machine. It is typically used
at startup.
"""

# Standard library imports go here.

import os
import os.path
import socket
import sys
from   typing import *

# Third party imports go here.

import pandas

# Canoe project imports go here. These are pre-stocked for your use
# because almost every plugin will need them. 

import canoestats
from   canoestats import LED
import fname
from   grammar import *
import hop
import pluginlib
import tombstone as tomb
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
def testconnect_main(opcodes:list) -> ERROR_ACTION:

    # It is easier to always work from a list, even if the list
    # has only one thing in it.
    opcodes = uu.listify(uu.deepsloppy(opcodes))
    stats = canoestats.default()
    mytype = 'xforms_custom'
    try:
        working_dir = opcodes[0].local_dir
        myname = uu.name_from_dirname(working_dir)

    except Exception as e:
        uu.tombstone('FATAL: Cannot determine the name of the current integration.')
        return ERROR_ACTION.notify
    
    # OK. Let's begin execution.
    stats.update(myname, mytype, LED.ON)

    # There is only one part to this diagnostic.
    opcodes = opcodes[0]
    timeout = opcodes.get('timeout', 5)
    report_filename = uu.path_join(working_dir, 'connect.test.md')
    report = open(report_filename, 'w')
    
    tally = uu.SloppyDict(dict.fromkeys(('attempts', 'found', 'connected'), 0))

    # Check the hosts.
    tomb.tombstone(f"Checking host connectivity with {timeout=}")
    hostnames = sorted(h for h in uu.get_ssh_host_info('all') if '*' not in h)
    report_data = uu.SloppyDict()

    for host in hostnames:
        report_data[host] = uu.now_as_string().split()
        tally.attempts += 1
        info = uu.SloppyDict(uu.get_ssh_host_info(host))

        ###
        # Attempt to open a socket on the host just to see if
        # it is reachable. There is a lot of diagnostic code here
        # because we are interested in the reason a connection
        # does not complete.
        ###

        hostname = info.hostname
        uu.tombstone(f"Checking {host}, a.k.a., {hostname}")
        port = int(info.get('port', 22))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            sock.settimeout(timeout)
            sock.connect( (hostname, port) )
            uu.tombstone(f"{hostname} accepted TCP connection on {port}.")
            report_data[host].append('reached')
            tally.found += 1

        except socket.timeout as e:
            uu.tombstone(f"{hostname} is not reachable within {timeout} seconds.")
            report_data[host].append('timeout')
            continue

        except socket.gaierror as e:
            uu.tombstone(f"{hostname} cannot be found.")
            report_data[host].append('nofind')
            continue

        except ConnectionRefusedError as e:
            uu.tombstone(str(e))
            uu.tombstone(f"{hostname} refused the connection as {info.user}.")
            report_data[host].append('refused')
            continue

        except ConnectionAbortedError as e:
            uu.tombstone(str(e))
            uu.tombstone(f"{hostname} aborted the connection as {info.user}.")
            report_data[host].append('aborted')
            continue

        except ConnectionResetError as e:
            uu.tombstone(str(e))
            uu.tombstone(f"{hostname} reset the connection as {info.user}.")
            report_data[host].append('reset')
            continue


        connection = hop.HOP(info) 
        if not connection:
            uu.tombstone(f"Could not connect to {info.user}@{hostname}")
            report_data[host].append('failed')
            continue
        else:
            report_data[host].append('connected')

        uu.tombstone(f"{hostname} is OK")
        tally.connected += 1
         
    uu.tombstone(f"Connection test results (attempts:found:connected): {tally.attempts} : {tally.found} : {tally.connected}")
    
    report.write("```\n")
    for name, data in report_data.items():
        report.write(f"{name}")
        for s in data:    
            report.write(f"\t{s}")
        report.write("\n")
    report.write("```\n")

    # This is the "all went well" exit.
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
    
