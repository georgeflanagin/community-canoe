#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys

import fname
from   urpacker import URpacker
import urutils as uu
from   urdecorators import show_exceptions_and_frames as trap

@trap
def unpack_main(f:str, data_type:str) -> int:
    """
    Create a new version of the file with .csv on the extension.
    """
    
    p = URpacker()
    p.attachIO(f, s_mode='read')
    data = p.read(data_type)

    if data_type == 'raw':
        with open(f"{f}.bytes", 'wb') as outfile:
            outfile.write(data)

    elif data_type == 'pandas':
        data.to_csv(f"{f}.csv", index=False)

    elif data_type == 'python':
        with open(f"{f}.py", 'w') as outfile:
            outfile.write(str(data))

    else:
        print(f"Unknown format requested: {data_type}")
        return os.EX_DATAERR

    return os.EX_OK


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("""
        Usage: unpack {mode} {filename}

        This utility converts the intermediate data files created by Canøe into
        ordinary CSV files for inspection. The result is a file in the same 
        directory as the original, but ending in '.csv'. So /x/y/f becomes 
        /x/y/f.csv. The input file is unchanged.

        It also allows you to read other packed files. The 'mode' parameter
        needs to be either 'raw' or 'python'. In the first case, you just get 
        bytes object, and in the second you get a python object. These are
        written to a .bytes and .py file, respectively.
        """)
        sys.exit(os.EX_USAGE)

    f = fname.Fname(sys.argv[-1])
    if not f:
        print(f"{f} is not a file, or cannot be found.")
        sys.exit(os.EX_NOINPUT)

    sys.exit(unpack_main(str(f), sys.argv[-2]))
