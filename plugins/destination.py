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
import shlex
import subprocess
import sys
import time

# Installed imports

# Canoe imports

from   canoestats import Blinker
from   canoestats import LED
import fname
from   grammar import *
import hop
import pluginlib
import tombstone as tomb
import sharefile
import urbox as ux
import urcurl
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

debugging = True
g_blinker = None

@trap
def snooze(n:int) -> int:
    """
    Calculate the delay. The formula is arbitrary, and can
    be changed.

    n -- how many times we have tried so far.

    returns -- a number of seconds to delay
    """
    num_retries = 10
    delay = 10
    scaling = 1.2

    g_blinker.blink(LED.WAITING)

    if n == num_retries: return None
    nap = delay * scaling ** n
    tomb.tombstone('Waiting {} seconds to try again.'.format(nap))
    time.sleep(nap)
    return nap


@trap
def deliver_file_box(handle:object, 
    filename:str, cloud_folder_id:int, klobber:bool) -> bool:

    num_tries = 0
    OK = True
    while OK:
        num_tries += 1
        try:
            handle.put(filename, cloud_folder_id, klobber) 
            return OK

        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            OK = snooze(num_tries)
        
    g_blinker.blink(LED.GREEN if OK else LED.WAIT_EXPIRED)

    return OK


@trap
def deliver_file_host(handle:object,
    local_filename:str, 
    remote_directory:str, 
    remote_filename:str, 
    overwrite:bool) -> bool:

    uu.tombstone(f"delivering {local_filename}")

    num_tries = 1
    while not (result:=handle.send_one_file(
        local_filename, remote_filename, overwrite, remote_directory)):
        if result is None: 
            return None

        num_tries += 1
        uu.tombstone(f"Try #{num_tries}")
        OK = snooze(num_tries)
        if not OK: 
            g_blinker.blink(LED.WAIT_EXPIRED)
            return False

    g_blinker.blink(LED.GREEN)
    return True


@trap
def deliver_file_s3(handle:object,
    local_name:str, 
    remote_dir:str) -> bool:

    num_tries = 1
    tomb.tombstone(uu.fcn_signature('send_to_bucket', local_name, remote_dir))
    while not handle.send_to_bucket(local_name, remote_dir):
        num_tries += 1
        OK = snooze(num_tries)
        if not OK: 
            g_blinker.blink(LED.WAIT_EXPIRED)
            return False

    g_blinker.blink(LED.GREEN)
    return True


@trap
def zipit(f:str, op:str, ext:str) -> str:
    """
    Apply a zip operation just before delivery, and return the name
    of the zipped file.

    f   -- file to change.
    op  -- What to "do"
    ext -- The extension (minus the dot) to append to the original file.
    """
    global debugging

    debugging and uu.tombstone(f"evaluating zip for {f} {op} {ext}")

    f = str(f)
    if not op: return f

    try:
        result = uu.dorunrun(f"{op} {f}", timeout=2, verbose=debugging)
        return f"{f}.{ext}" if result is True else f
    except Exception as e:
        tomb.tombstone(uu.type_and_text(e))
        return f
        

@trap
def destination_main(opcodes:list) -> ERROR_ACTION:
    """
    transfer data to one of our endpoints by executing the opcodes. 

    opcodes -- a collection of Canøe opcodes.

    returns -- ERROR_ACTION.proceed if everything goes according to plan, and
        another ERROR_ACTION object if things go awry. Note: ERROR_ACTION.skip is 
        always consumed -- never returned -- because it results in the raise of
        the OuterLoop exception which is converted to a continue.
    """
    global debugging
    global g_blinker

    # Use a generator expression to iterate the opcodes.
    mytype = 'files_out'
    tomb.tombstone(">>>>>> DESTINATION")
    for i, subroutine in enumerate([uu.deepsloppy(_) for _ in opcodes], start=1):
        debugging = subroutine.debug
        myname = uu.name_from_dirname(subroutine.local_dir)
        if i == 1: g_blinker = Blinker(myname, mytype)
        debugging and uu.tombstone(f'section {i} of {len(opcodes)} in {myname}.{mytype}')
        
        files = [ fname.Fname(_) for _ in uu.build_file_list(subroutine.file) ] 
        
        subroutine.on_error = ERROR_ACTION(subroutine.on_error)
        if pluginlib.test_empty(files, subroutine.on_error):
            tomb.tombstone('Nothing to transfer.')
            g_blinker.blink(LED.GREEN)
            return ERROR_ACTION.stop       
        else:
            tomb.tombstone('Transferring {} files: \n{}\n'.format(len(files), [str(_) for _ in files]))     
            
        try:
            if 'box' in subroutine:
                handle = ux.URBoxHOP(subroutine.box)
                for f in files:
                    # f = fname.Fname(zipit(f, *subroutine.zip))

                    tomb.tombstone('transferring {} to Box/{}'.format(f, subroutine.directory))
                    if deliver_file_box(handle, filename=f.fqn, 
                            cloud_folder_id=subroutine.directory, 
                            klobber=subroutine.overwrite): 
                        g_blinker.blink(LED.GREEN)
                        continue

                    elif subroutine.on_error is ERROR_ACTION.proceed: 
                        g_blinker.blink(LED.YELLOW)
                        continue

                    elif subroutine.on_error is ERROR_ACTION.skip: 
                        g_blinker.blink(LED.YELLOW)
                        raise uu.OuterLoop()

                    g_blinker.blink(LED.RED)
                    return subroutine.on_error
                
            elif 'curl' in subroutine:
                
                curler = urcurl.URcurler(subroutine.curl.type)

                setup = { k:v for k,v in subroutine.curl.items() if k != 'type' }
                curler.add_credentials(**setup)

                if not curler.attachIO():
                    uu.tombstone(f'incomplete credentials {setup}')
                    g_blinker.blink(LED.RED)
                    uu.tombstone(f'cannot attach to {subroutine.curl.host}')
                    return ERROR_ACTION.notify

                curler.put(subroutine.file, subroutine.directory)
                

            elif 's3' in subroutine:
                if uu.dorunrun(uu.date_filter(subroutine.ops), verbose=True): 
                    g_blinker.blink(LED.GREEN)
                    continue

                elif subroutine.on_error is ERROR_ACTION.proceed: 
                    g_blinker.blink(LED.YELLOW)
                    continue

                elif subroutine.on_error is ERROR_ACTION.skip: 
                    g_blinker.blink(LED.YELLOW)
                    raise uu.OuterLoop()

                g_blinker.blink(LED.RED)
                return subroutine.on_error


            elif 'host' in subroutine:
                handle = hop.HOP(subroutine.host)
                for f in files:
                    # f = fname.Fname(zipit(f, *subroutine.zip))

                    uu.tombstone('transferring {} to {}:{}'.format(
                        f.fqn, subroutine.host.hostname, subroutine.directory))
                    if deliver_file_host(handle, local_filename=f.fqn, 
                            remote_directory=subroutine.directory,
                            remote_filename=f.fname, 
                            overwrite=subroutine.overwrite): 
                        g_blinker.blink(LED.GREEN) 
                        continue

                    elif subroutine.on_error is ERROR_ACTION.proceed: 
                        g_blinker.blink(LED.YELLOW)
                        continue

                    elif subroutine.on_error is ERROR_ACTION.skip: 
                        g_blinker.blink(LED.YELLOW)
                        raise uu.OuterLoop()

                    g_blinker.blink(LED.RED)
                    return subroutine.on_error

            elif 'sharefile' in subroutine:
                handle = sharefile.ShareFile(subroutine.sharefile)
                for f in files:
                    uu.tombstone(f'transferring {f} to {subroutine.sharefile.host}')
                    if handle.send(str(f)):
                        g_blinker.blink(LED.GREEN)
                        continue
                    
                    elif subroutine.on_error is ERROR_ACTION.proceed:
                        g_blinker.blink(LED.YELLOW)
                        continue

                    elif subroutine.on_error is ERROR_ACTION.skip: 
                        g_blinker.blink(LED.YELLOW)
                        raise uu.OuterLoop()
                        
                    g_blinker.blink(LED.RED)
                    return subroutine.on_error

            else:
                tomb.tombstone('unreachable destination {}'.format(list(subroutine.keys())))
                g_blinker.blink(LED.RED)
                return ERROR_ACTION.notify

        except PermissionError as e:
            tomb.tombstone('unable to deliver files because of permissions problem.')
            g_blinker.blink(LED.RED)
            return ERROR_ACTION.notify

        except uu.OuterLoop as e:
            continue

    else:
        g_blinker.blink(LED.GREEN)
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
    
    
