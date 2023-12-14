#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This Canøe plugin creates diff results by exploiting the
row tagging ability in pandas dataframes.
"""
import typing
from typing import *

# Built in imports

import os
import subprocess
import sys

# Installed packages

import pandas

# Canøe imports

import tombstone as tomb
from   grammar import *
from   urdecorators import show_exceptions_and_frames as trap
import urutils as uu

def touch(fname:str) -> bool:
    """
    Create a file, or update its os.stat mtime.
    """
    results = subprocess.run(['touch',fname])
    return results.returncode == os.EX_OK
    

@trap
def framediff_main(o:object) -> bool:
    """
    The argument should contain a collection of k-v pairs, and these are the
    required ones:

    dirr -- directory where the files are found. This will be prepended to 
        `first` and `second` unless either is an absolute filename.     
    first -- first file of data
    second -- second file of data to compare to the first.

    output1 -- name of file to contain frame1 - frame2, or the rows that
        are only in frame1
    output2 -- name of file to contain frame2 - frame1, or the rows that
        are only in frame2

    Optional are:

    emptyfile -- values are:
        True    - if there is no difference, create an empty file.
        None   - if there is no difference, produce no output.
        header  - if there is no difference, produce a header row with no data.
    sep -- usually a comma [DEFAULT]
    quotes -- 0 -> none, 1 -> single, 2 -> double [DEFAULT], 3 -> backtick. 
    header -- boolean, DEFAULTs to True
    escape -- DEFAULTS to backslash

    exceptions raised -- None / everything should be supressed.

    returns -- True if everything worked, False otherwise.
    """

    # Step 0. Start with slop, stay with slop.
    o = uu.sloppy(o)

    # Step 1. Check the arguments.
    for k in ['first', 'second', 'output1', 'output2']:
        if k not in o:
            tomb.tombstone('required argument {} is missing.'.format(k))
            return False

    try:
        this_dir = o.dirr
    except:
        this_dir = os.getcwd()
    finally:
        if not o.first.startswith(os.sep): o.first = os.path.join(this_dir, o.first)
        if not o.second.startswith(os.sep): o.second = os.path.join(this_dir, o.second)


    inputs = [ o.first, o.second ]
    outputs = [ os.path.join(this_dir, _) for _ in [ o.output1, o.output2 ] ]

    quotes = ["", "'", '"', '`']
    my_args = uu.sloppy({})
    default_args = {
        "index":False, 
        "escapechar":"\\", 
        "header":True,
        "emptyfile":"header",
        "quoting":0}

    if 'sep'        in o: my_args['sep'] = o.sep
    if 'quotes'     in o: my_args['quotechar'] = quotes[min(o.quotes, 3)]
    if 'header'     in o: my_args['header'] = o.header
    if 'escape'     in o: my_args['escapechar'] = o.escape
    if 'quoting'    in o: my_args['quoting'] = min(o.quoting, 3)
    if 'emptyfile'  in o: my_args['emptyfile'] = o.emptyfile

    my_args = uu.sloppy({**default_args, **my_args})  
    if __name__ == "__main__":
        print("actual arguments are: {}".format(my_args))

    # Step 2. Read the input files, converting each to a pandas.DataFrame
    try:
        frame1 = pandas.read_csv(o.first, sep='|')
        cols = my_args.sep.join(frame1.columns)
        frame2 = pandas.read_csv(o.second, sep='|')
    except Exception as e:
        tomb.tombstone('unable to read input')
        tomb.tombstone(uu.type_and_text(e))
        return False

    #######################################################################
    # Step 3. Do the diff. For each "only in ..." file, strip off the 
    # tag because it is not a part of the data set.
    #
    # These files have a header, but they have the /same/ header (or they
    # certainly should). Per jmesser2, instead of an empty file or no file
    # at all, the receiving process should have a file with a bare header
    # row and no data. So, let's grab the header (first line) of file1.
    #######################################################################
    
    merged_frame = frame1.merge(frame2, indicator=True, how='outer')

    out1 = merged_frame[merged_frame['_merge'] == 'left_only']
    out1 = out1.loc[:, out1.columns[:-1]]
    out2 = merged_frame[merged_frame['_merge'] == 'right_only']
    out2 = out2.loc[:, out2.columns[:-1]]

    """ Step 4. Write the results. """
    for f, g in zip((out1, out2), outputs):
        if f.empty:
            if my_args.emptyfile is True:
                touch(g)
                continue
            elif my_args.emptyfile is None:
                with open(g, 'w') as outfile:
                    outfile.write(cols)
                continue
        try:
            f.to_csv(g, sep=my_args.sep, quoting=my_args.quoting, index=False)
        except Exception as e:
            tomb.tombstone('write failed')
            tomb.tombstone(uu.type_and_text(e))
            return False

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
    
