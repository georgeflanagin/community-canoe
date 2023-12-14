# -*- coding: utf-8 -*-

import typing
from   typing import *

# Credits
__longname__ = "University of Richmond"
__acronym__ = " UR "
__author__ = 'Ray Cargill, original version'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '3.0'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Production'

# Builtins
import csv
import sys
import os
import os.path
import shlex
import shutil
import subprocess
import sys
import time
 
# UR libraries
import canoestats
from   canoestats import LED
import fname
from   grammar import *
import tombstone as tomb
import urpacker
import urutils as uu
from   urdecorators import show_exceptions_and_frames as trap

one_second = 1

@trap
def picname(URID:str, d:str) -> str:
    """
    URID -- a key in the dictionary
    d -- directory to look in.

    returns -- the name of file if it exists, otherwise None.
    """
    f = fname.Fname("{}.jpg".format(uu.path_join(d, URID)))
    return str(f) if f else None


@trap
def fusionpics_main(opcodes:uu.SloppyDict) -> ERROR_ACTION:
    """
    Resize and rename appropriate files for the fusion pics.

    This code is optimized for execution speed if you consider
        the following characteristics of the data:

    - There are many more pictures than there are people who need
        a resized and renamed picture for the Rec & Wellness 
        facility.
    - Pictures are updated rarely. So if we have seen the 
        picture before there is a good chance that a resized
        picture already exists, and that a renamed picture
        already exists.
    - Resizing is a CPU intensive activity. We only resize
        what we need to.
    - Copying files is fast, and there are only a few files,
        so we can copy instead of rename. This allows us to
        know if the renamed files are good to go without
        constantly recreating them.
    - The resized files are almost always smaller than the 
        originals.
    """
    tomb.tombstone('picture processing beginning at {}'.format(time.ctime()))
    stats = canoestats.default()
    mytype = 'xforms_custom'
    myname = uu.name_from_dirname(opcodes.local_dir)
    stats.update(myname, mytype, LED.ON)

    all_pics = {}

    with open(str(uu.path_join(opcodes.local_dir, opcodes.xref))) as csvfile:
        reader = csv.reader(csvfile,delimiter='|')
        for row in reader:
            all_pics[row[0]] = row[1]

    tomb.tombstone("There are {} entries in the URID/PIDM xref.".format(len(all_pics)))

    fusiondirs = uu.SloppyDict({
        'root':opcodes.local_dir
        ,'pics':uu.path_join(opcodes.local_dir, 'pics')
        ,'resized':uu.path_join(opcodes.local_dir, 'pics.resized')
        ,'renamed':uu.path_join(opcodes.local_dir, 'pics.renamed')
        })

    #################### STEP 1 ##############################
    # In the dictionary, the keys are the URIDs, and the values
    # are the PIDMs with some modifiers for the correct filename    
    ###

    for i, urid in enumerate(list(all_pics.keys())):
        if not i % 100:
            tomb.tombstone("processing #{}".format(i))
        # First ... let's see if there is a file at all.
        original = picname(urid, fusiondirs.pics)
        if original is None: 
            all_pics.pop(urid, None)
            continue
    
        # Now, check to see if there is resized file already, and one that
        # is no older than the original file.
        resized = picname(urid, fusiondirs.resized)
        try:
            do_resize = os.stat(resized).st_mtime < os.stat(original).st_mtime
        except:
            do_resize = True

        ###
        # OK, we need a resized pic. This is the real substance of the
        # loop -- the above logic is just a filter.
        ###
        try:
            cmd=" ".join([
                opcodes.convert_exe, original, opcodes.convert_ops, 
                "{}{}{}.jpg".format(fusiondirs.resized, os.sep, urid)
                ])
            subprocess.run(shlex.split(cmd)).check_returncode()

        except subprocess.CalledProcessError as e:
            stats.update(myname, mytype, LED.RED)
            tomb.tombstone(uu.type_and_text(e))
            return ERROR_ACTION.stop

        except Exception as e:
            stats.update(myname, mytype, LED.RED)
            tomb.tombstone(uu.type_and_text(e))
            return ERROR_ACTION.stop
    else:
        tomb.tombstone("processed {} pics.".format(i))        


    ######################### STEP 2 ##################################
    # Now we need to create renamed pics; as this is a simple matter
    # we just rename them all.
    ###

    for urid, pidm in all_pics.items():

        # First .. let's see if there is a resized_pic. There
        # probably is, but if there was no original, then we 
        # won't have a resized one, either, and in that case 
        # there will be no renamed one. Follow? Do ya follow?

        resized_pic = picname(urid, fusiondirs.resized)
        if not resized_pic: continue

        # If this is the fiftieth time through, then there probably
        # is a renamed pic, also. In that case, we change the question
        # to "Is it significantly out of date /because/ it is
        # from a previous execution?"

        renamed_pic = picname(all_pics[urid], fusiondirs.renamed)
        if renamed_pic is not None:
            renamed_mtime = os.stat(renamed_pic).st_mtime
            resized_mtime = os.stat(resized_pic).st_mtime

            # We allow for a sloppy second, thus the 1 (second) diff in 
            # mtimes is what we test.

            if abs(renamed_mtime - resized_mtime) < one_second:
                continue
        else:
            renamed_pic = "{}{}{}".format(fusiondirs.renamed, os.sep, all_pics[urid])
        
        # NOTE: the copying functions in shutil always overwrite. I set
        # follow_symlinks to False in case we decide at some point in the
        # future to relocate the files or their containing directories.
        # In such a case, we need only recreate the links.

        try:
            shutil.copy2(resized_pic, renamed_pic, follow_symlinks=False) 
        except Exception as e:
            stats.update(myname, mytype, LED.RED)
            tomb.tombstone(uu.type_and_text(e))
        else:
            tomb.tombstone(resized_pic + ' => ' + renamed_pic)

    ######################## THE END ##############################

    tomb.tombstone('Fini @ {}'.format(time.ctime()))
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
