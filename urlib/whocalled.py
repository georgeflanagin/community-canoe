# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from   typing import *

""" 
This is the main routine of canoed. It is intended to be launched 
with nohup.
"""

# Credits
__author__ = 'George Flanagin, Douglas Broome, Virginia Griffith, Ray Cargill'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.4'
__maintainer__ = 'George Flanagin, Douglas Broome, Virginia Griffith, Ray Cargill'
__email__ = 'canoe@richmond.edu'
__status__ = 'Working Prototype'
__required_version__ = (3, 6)

__license__ = 'MIT'
import license

# Standard imports

import inspect
import os
import sys

if sys.version_info < __required_version__:
    print("This software requires Python " + str(__required_version__))
    sys.exit(os.EX_SOFTWARE)


class StackItem:
    """
    Represents a 'plane' in the call stack.
    """

    __slots__ = ['file_name', 'module_name', 'module', 
        'function_name', 'function', 
        'line_number', 'context', 'index']

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            try:
                setattr(self, key, value)
            except Exception as e:
                print('{} is not a member of StackItem'.format(key))


    def __call__(self) -> tuple:
        """
        Return a tuple of values.
        """
        return tuple([self.file_name, self.module_name, self.module,
            self.function_name, self.function, self.line_number, 
            self.context, self.index])


    def __str__(self) -> str:
        """
        Something nice to say about itself.
        """
        return ", ".join([str(getattr(self,_)) for _ in StackItem.__slots__])


def whocalledme(s:list=None) -> dict:
    """
    This function reformats the call stack, and returns information
    as a list of tuples.

    0: file name
    1: module name
    2: module reference
    3: function name
    4: function reference
    5: line number
    6: the code context
    7: index
    """

    info = {}

    # if we are being passed the stack, then we treat it naively. But if
    # we are calling inspect.stack() ourselves, then there will be an
    # extra and meaningless level created by jumping into this function.
    stak, skip_top = (s, False) if s else (inspect.stack(), True)

    for level, plane in enumerate(stak):
        if level == 0 and skip_top: continue
        module_name, _ = os.path.splitext(os.path.basename(plane.filename))
        try:
            module = sys.modules[module_name]
            info[level] = StackItem(
                file_name = plane.filename,
                module_name = module_name,
                module = module,
                function_name = plane.function,
                function = getattr(module, plane.function),
                line_number = plane.lineno,
                context = plane.code_context,
                index = plane.index
                )
        except (KeyError, AttributeError) as e:
            # This means we hit something that doesn't have a module name
            # in Python such as <stdin> if we are testing, or a C-library.
            continue


    return info                
        


