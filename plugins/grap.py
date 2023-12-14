#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This plugin retrieves and distributes the contents of a zip-archive
from Box. The contents can be sent back to Box, to the local file
system, or both.
"""
import typing
from   typing import *

# Built in imports
import collections
import datetime
import fnmatch
import http
import os
import sys
import zipfile

# Canøe imports

import canoestats
from   canoestats import LED
from   grammar import *
import tombstone as tomb
import urbox
from   urdecorators import show_exceptions_and_frames as trap
import urutils as uu

###
# Named tuple that we will use to track the working directory path
# and the destination directory path for each file we download/extract.
###
DistroPaths = collections.namedtuple('DistroPaths', 'work_path distro_path file_name')

debugging = True

@trap
def grap_main(opcodes:uu.SloppyDict) -> ERROR_ACTION:
    """
    This operation takes place exclusively on UR's Box instance and file
    system. To "grap" is to get a zip file, unpack it, and distribute its 
    contents.

    opcodes contains six parameters:

        box -- the parameters for opening the box instance. (supplied by
            the compiler)
        zipfile -- wild card pattern for a zip file we want to 
            process. (defaults to "*.zip")
        filter -- type of thing you are looking for in the zip. (defaults to "*")
            This pattern must be something that can be processed by fnmatch.
        box_stage -- where to find the zip.  
        box_backup -- where to stash the working contents; if this parameter is
            empty or None, no backup is made.
        destination_dir -- where to send the exploded contents of the zip; if 
            this parameter is empty or None, no distribution is made.

    """

    ###
    # A little boiler plate.
    ###
    global debugging
    stats = canoestats.default()
    mytype = 'xforms_custom'
    myname = uu.name_from_dirname(opcodes.local_dir)
    stats.update(myname, mytype, LED.ON)
    tomb.tombstone("grap plugin started")

    ###
    # Gain a little notational convenience.
    ###
    staging_folder_id = opcodes.box_stage
    backup_folder_id  = opcodes.box_backup if opcodes.box_backup else None
    local_dir         = opcodes.local_dir
    destination_dir   = opcodes.destination_dir if opcodes.destination_dir else ""
    filter            = opcodes.filter
    a_zipfile         = opcodes.zipfile
    
    ###
    # Ensure the OnBase directory exists and we can use it.
    ###
    if destination_dir:
        try:
            os.makedirs(destination_dir, 0o777, exist_ok=True)
            uu.tombstone(f"{destination_dir} is available.")

        except PermissionError as e:
            tomb.tombstone(f"Unusable dir {destination_dir}")
            stats.update(myname, mytype, LED.RED)
            return ERROR_ACTION.crash

    ###
    # Open Box. Look in the staging_folder and find anything that
    # matches the glob.
    ###
    box_handle = urbox.URBoxHOP(opcodes.box)
    staging_folder_info = box_handle.browse(staging_folder_id)
    staging_filenames = fnmatch.filter(staging_folder_info.keys(), a_zipfile)    
    uu.tombstone(f"{len(staging_filenames)} filenames match {a_zipfile}")
    if not staging_filenames:
        tomb.tombstone("Nothing to do.")
        stats.update(myname, mytype, LED.GREEN)
        return ERROR_ACTION.stop

    ###
    # Download matching filenames
    ###
    box_results = []
    box_failures = []
    for i, _ in enumerate(staging_filenames):
        uu.tombstone(f"Getting file {i}, {_}")
        if box_handle.get(_, staging_folder_id, local_dir, True):
            box_results.append(os.path.join(opcodes.local_dir, _))
        else:
            box_failures.append(os.path.join(opcodes.local_dir, _))

    if box_failures:
        tomb.tombstone(f"Unable to retrieve {box_failures}")
        stats.update(myname, mytype, LED.YELLOW)

    ###
    # Build dict with zip file names as keys and DistroPaths tuples as values
    ###
    archive_map = {}
    for result in box_results:
        tomb.tombstone(f"exploding zip file {result}")
        z = zipfile.ZipFile(result)
        filtered_names = fnmatch.filter(z.namelist(), opcodes.filter) 
        uu.tombstone(f"Found these files {filtered_names} to extract from {result}")
        if not filtered_names: continue

        ###
        # Build record with working directory and onbase import directory paths
        ###
        records = [ DistroPaths(work_path=os.path.join(local_dir, name), 
                                 distro_path=os.path.join(destination_dir, name),
                                 file_name=name) for name in filtered_names ]

        ###
        # Add the map info to the dict and extract the files from the zip archive
        ###
        archive_map[z.filename] = records
        for record in records:
            z.extract(record.file_name, local_dir)
            if destination_dir: uu.fcopy_safe(record.work_path, record.distro_path)


    ###
    # See if there is an archive folder named after today. If not, create one
    ###
    archive_folder_name = datetime.datetime.today().strftime('%Y%m%d_files')
    backup_folder_info = box_handle.browse(backup_folder_id)

    if archive_folder_name in backup_folder_info.keys():
        tomb.tombstone(f"Adding to archive folder {archive_folder_name}")
        archive_folder_id = backup_folder_info[archive_folder_name]
    else:
        tomb.tombstone(f"Created new archive folder {archive_folder_name}")
        archive_folder_id = box_handle.create_folder(folder_name=archive_folder_name,
                                              parent_folder_id=backup_folder_id)

    ###
    # Upload each individual file we extracted to the Box archive folder.
    ###
    for z in archive_map.keys():
        for _ in archive_map[z]:
            tomb.tombstone(f"archiving file {_.file_name}")
            if box_handle.put(_.work_path, archive_folder_id, True):
                uu.tombstone(f"Uploaded {_.file_name}")
                os.unlink(_.work_path)
            else:
                tomb.tombstone(f"Unable to upload {_.work_path}")
                box_failures.append(_.work_path)

        ###
        # Move the zip archive in Box, then remove it from the working directory
        ###
        tomb.tombstone(f"moving zip archive {os.path.basename(z)}")
        try:
            num_moved = box_handle.rename(os.path.basename(z), 
                        cloud_folder_id=staging_folder_id, 
                        new_folder_id=backup_folder_id)

        except Exception as e:
            if e.status == http.HTTPStatus.CONFLICT: 
                tomb.tombstone("Zip file is already present in backup folder, deleting")
                box_handle.delete(filename=os.path.basename(z), cloud_folder_id=staging_folder_id)
            else:
                tomb.tombstone(f"Unexpected error {str(e)}")

        os.unlink(z)

    if not box_failures: stats.update(myname, mytype, LED.GREEN)
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
    
