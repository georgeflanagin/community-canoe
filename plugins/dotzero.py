#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This Canøe plugin will remove a trailing ".0" from integer values
in a text/csv file.
"""
import typing
from typing import *

# Built in imports

import os
import re
import sys

# Canøe imports

import tombstone as tomb
import urutils as uu

def dotzero_main(opcodes:object) -> bool:
    """
    The argument should contain a collection of k-v pairs, and these are the
    required ones:

    first -- the file we will read
    second -- the file we will write.

    Optional are:

    dirr -- directory where the files are found. This will be prepended to 
        `first` and `second` unless they are absolute filenames.     
    sep -- usually a comma [DEFAULT]
    quotes -- 0 -> none, 1 -> single, 2 -> double [DEFAULT], 3 -> backtick. 
    header -- boolean, DEFAULTs to True

    exceptions raised -- None.

    returns -- True if everything worked, False otherwise.
    """

    # Step 1. Set the variables.
    for k in ['first', 'second']:
        if k not in opcodes:
            tomb.tombstone('required argument {} is missing.'.format(k))
            return False

    try:
        this_dir = opcodes.dirr
    except:
        this_dir = os.getcwd()
    finally:
        if not opcodes.first.startswith(os.sep): opcodes.first = os.path.join(this_dir, opcodes.first)
        if not opcodes.second.startswith(os.sep): opcodes.second = os.path.join(this_dir, opcodes.second)


    quotes = ["", "'", '"', '`']
    my_args = uu.sloppy({})
    default_args = {
        "index":False, 
        "escapechar":"\\", 
        "header":True,
        }

    if 'sep' in opcodes: my_args.sep = opcodes.sep
    if 'quotes' in opcodes: my_args.quotechar = quotes[min(opcodes.quotes, 3)]
    if 'header' in opcodes: my_args.header = opcodes.header
    if 'escape' in opcodes: my_args.escapechar = opcodes.escape


    my_args = uu.sloppy({**default_args, **my_args}) 
    expression = re.compile(r"\.0"+my_args.sep)
    
    start_line = 1 if my_args.header else 0

    # Step 2. Read the input files. """
    with open(first) as f:
        lines = f.read().split("\n")
    
    # Step 3. Replace.
    with open(second, 'w') as f:
        for line_no, line in enumerate(lines):
            if line_no < start_line: f.write(line+"\n")
            f.write(re.sub(expression, my_args.sep, line)+"\n")

    return True


if __name__ == "__main__":
    import csv
    if len(sys.argv) < 3:
        print(sys.argv)
        print("Usage: ")
        print(" {} infile outfile".format(sys.argv[0]))
        sys.exit(os.EX_DATAERR)

    d = {}
    d['first'] = sys.argv[1]
    d['second'] = sys.argv[2]
    d['sep'] = '|'

    tomb.tombstone("dotzero returned {}".format(dotzero_main(d)))
    
else:
    pass
