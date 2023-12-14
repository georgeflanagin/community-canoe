# -*- coding: utf-8 -*-
""" 
The module that loads compiled integration code.
"""

import typing
from   typing import *

# System imports

import glob
import json
import os
import os.path
import sys

# Installed imports

# Canoe imports

import urpacker
import tombstone as tomb
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
def load(location:str) -> uu.SloppyDict:

    for _ in (glob.glob(os.path.join(location, '*.jsc'))):
        loader = urpacker.URpacker()
        loader.attachIO(_, s_mode='read')
        yield uu.deepsloppy(loader.read())

    return None

@trap
def loader(location:str) -> tuple:
    """
    Load the code found in the location pointed to by the argument.

    location -- something that can be associated with an fname object.

    returns --  a tuple consisting of a status code and the information
                    retrieved. The information is a dict of SloppyDicts,
                    or an empty dict in the case of errors, etc. The 
                    dict is keyed on the object's name.

                >0 : number of components loaded
                 0 : no data found to load
                <0 : negated constant from os.EX_*
    """
    programs = {}

    if not os.path.isdir(location): return (0 - os.EX_CONFIG), programs
    gen = load(location)

    try:
        while True:
            program = next(gen)
            programs[program.name] = program
    except StopIteration as e:
        pass

    return len(programs), programs


if __name__ == '__main__':
    import pprint
    """
    Test program for the loader.
    """
    if len(sys.argv) < 2: sys.exit(os.EX_DATAERR)

    result, programs = loader(sys.argv[-1])
    if result < 0: 
        print("Failed with code {}".format(uu.explain(-result)))
        sys.exit(-result)
    elif not result:
        print("Nothing found in {}".format(sys.argv[-1]))
    else:

        for name, program in programs.items():
            print("\n\nRecipe {}\n".format(name))
            pprint.pprint(program, indent=4, width=100, compact=True)

    sys.exit(os.EX_OK)

