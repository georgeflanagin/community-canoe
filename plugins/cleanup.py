# -*- coding: utf-8 -*-
""" Module description """

import typing
from   typing import *

# System imports

import fnmatch
import importlib as il
import os
import sqlite3
import sys
import time

# Installed imports


# Canoe imports

import executive
import fname
from   grammar import *
import remote_ops
import sqlitedb
import tombstone as tomb
import urpacker
import urutils as uu
from   urdecorators import show_exceptions_and_frames as trap

# Credits

__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Development'

__license__ = 'MIT'
import license

db = None
junk_patterns = ('*.columns', '*.rows')

def junk_filter(s:str) -> str:
    """
    This function eliminates useless temporary files that
    are created as artifacts of the integration process.
    """
    global junk_patterns
    for pattern in junk_patterns:
        if fnmatch.fnmatch(s, pattern): return None
    return s


@trap
def new_file(s:str) -> bool:
    """
    s -- a file name.

    returns -- True if this file's hash is new, else False.
    """
    global db
    if db is None: return False
    # db = sqlitedb.SQLiteDB('/sw/canoe/var/data/fifo/hashes.db')
    if not (f := fname.Fname(s)): return True

    try:
        # print(f"the hash of {str(f)} is {f.hash}")
        if db.execute_SQL( f"SELECT * FROM hashes WHERE hash = '{f.hash}'") != []:
            uu.tombstone(f"{str(f)} previously seen.")
            return False

        db.execute_SQL(f"INSERT INTO hashes VALUES ('{str(f)}', '{f.hash}')")
        uu.tombstone(f"{str(f)} added to database.")

    except Exception as e:
        uu.tombstone(f"Unanticipated error: {str(e)}")
        uu.tombstone(f"{str(f)} / {f.hash}")
        return False

    return True


@trap
def seen_before(s:str) -> bool:
    """
    A semantic reversal of new_file, so that the code contains
    fewer negative statements.
    """
    return not new_file(s)


@trap
def cleanup_main(opcodes:uu.SloppyDict) -> ERROR_ACTION:
    """
    Due to restructuring of the Canøe opcodes, this instruction
    is not commonly used. Most of the time, it will consist of the
    bare opcode, and no arguments. In which case, we note that in
    the logfile, and return.
    """
    tomb.tombstone(">>>>> CLEANUP ")
    global db

    fifo_dir = os.environ.get('FIFODIR', '/sw/canoe/var/data/fifo')
    earliest_file_allowed = time.time() - int(os.environ.get('FIFOMINUTES', 26*60)) * 60
    sn = os.environ.get('sn')
    
    delete_count = 0
    opcodes = uu.listify(opcodes)
    local_dir = opcodes[0].local_dir

    if fifo_dir is not None:
        db = sqlitedb.SQLiteDB(uu.path_join(fifo_dir, 'hashes.db'), use_pandas=False)
    
        # Step 1: remove anything older than FIFOMINUTES old.
        for n, f in enumerate(uu.all_files_in(fifo_dir)):
            # Guard against Canøe being down for a day.
            if f.endswith('hashes.db'): continue
            if os.path.getmtime(f) < earliest_file_allowed:
                try:
                    if (f := fname.Fname(f)): 
                        db.execute_SQL(f"DELETE FROM hashes WHERE hash = '{f.hash}'")
                        uu.tombstone(f"hash {f.hash} removed from database.")
                        os.unlink(str(f))
                        uu.tombstone(f"file {str(f)} removed from fifo.")
                        delete_count += 1                    

                except FileNotFoundError as e:
                    # No reason for concern. Another process has deleted the file.
                    pass

                except Exception as e:
                    tomb.tombstone(f"Could not unlink {str(f)} because {str(e)}")

        
        # Step 2: Make the links.
        uu.make_dir_or_die(fifo_dir)

        uu.tombstone(f"Adding links to new files in {local_dir} to {fifo_dir}")
        local_dir = opcodes[0].local_dir
        for n, f in enumerate(uu.all_files_in(local_dir)):

            # Do not link junk files.
            if (f_base := junk_filter(os.path.basename(f))) is None: continue
            # Do not create links for a previously seen file.
            if seen_before(f): continue

            try:
                os.link(f, uu.path_join(fifo_dir, f_base))
                
            except FileExistsError as e:
                # If the file is a duplicate name, let's just append the
                # serial number to it.
                os.link(f, uu.path_join(fifo_dir, f"{f_base}.{sn}"))

            except Exception as e:
                tomb.tombstone(f"Could not create link to {f} because {str(e)}")

            else:
                uu.tombstone(f"Created link for {f}")

        tomb.tombstone(f"Removed {delete_count} of {n} files in {fifo_dir}")

    if len(opcodes) == 1:
        op_block = opcodes[0]
        if 'host' not in op_block and 'db' not in op_block:
            uu.tombstone("[NOP] instruction in cleanup; nothing to do.")
            return ERROR_ACTION.proceed

    else:
        uu.tombstone(f"found {len(opcodes)} cleanup instructions.")
        return remote_ops.remote_ops_main(opcodes)


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
    
