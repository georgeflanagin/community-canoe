#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from typing import *

""" Core/startup/main routine for Canoe.

The length of this routine, which should really have only the line
that says

   CanoeConsole.cmdloop()

is due to our need to determine if we are or are not running inside
a virtual environment, and if so what we may need to do before we can
run without crashing.

In this programming environment there are a few casually reserved 
variable names:

 k : k is used to refer to the store of constants. see the konstants
    module for more details.
 g : g is the handle to the global configuration data. It has "members"
    for each of the things it has read from a config file, and those 
    members' elements are accessed by dictionary notation.
 r : is usually the Recipe currently being executed.
db : is the handle to //Canoe's// database. There is some need for
    caution here, as Canoe often has more than one database open at
    a time.
[cu][ue] : These aliases are used after imports.

Some care has been taken to improve the visibility of these within
the code. The python.vim file has been appropriately changed.

"""

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.4'
__maintainer__ = 'George Flanagin, Douglas Broome'
__email__ = 'gflanagin@richmond.edu, dbroome@richmond.edu'
__status__ = 'Working Prototype'
__required_version__ = 3.3

__license__ = 'MIT'
import license


# These are system packages, and there are no python environments
# that do not have them present.
import os
import signal
import subprocess
import sys
import traceback

import canoelibs.canoeenv as canoeenv
import canoelibs.canoeconsole as canoeconsole
import tombstone as tomb
import urdecorators
import urutils as uu
# OK, let's see if we can start up.

@urdecorators.show_exceptions_and_frames
def main_foo():
    try:
        canoeconsole.CanoeConsole().cmdloop()
    except KeyboardInterrupt:
        print("Whoa! You asked to exit.")
        sys.exit(os.EX_OK)
    except:
        raise

if __name__ == "__main__":
    main_foo()
