#!/usr/bin/python
# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from typing import *

""" A recipe is the parsed, ready-to-eat version of the JSON representation
    of a Canoe recipe. """

# System imports
from   collections.abc import Iterable
import os
import sys

# Canoe imports
import urutils as uu

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Prototype'


__license__ = 'MIT'
import license

class Recipe:
    pass

class Recipe(uu.SloppyDict):
    """ 
    The recipes are written as JSON. When recipe files are loaded, they
    are transformed into python objects with the same members and values,
    with semantics of what the keys and values represent. This
    class contains all the data after processing. Objects of this
    type are built by the RecipeCompiler.

    Recipe inherits from uu.SloppyDict. This sleight of hand allows
    us to use either recipe['name'] or recipe.name in the code. Sometimes
    this is handy, and it is always uu.sloppy.
    """
    def __init__(self) -> None:
        pass


    def finish(self) -> Recipe:
        """
        Call deepsloppy so that we can reference all elements with
        the more convenient dot notation.
        """
        self = uu.deepsloppy(self)
        return self


    def reorder(self, ordering:Iterable) -> Recipe:
        """
        Reorder the symbol table for printing.
        """
        if not ordering: return self

        keys_not_in_ordering = sorted(list(set(self.__dict__.keys()) - set(ordering)))
        rr = uu.SloppyDict()
        for k in ordering:
            rr[k] = self[k]
        for k in keys_not_in_ordering:
            rr[k] = self[k]

        return rr.finish()


    def show(self) -> None:
        """ 
        Recipes are subject to a lot of scrutiny, so I put in this
        function to make them more suitable to inspection. 
        """
        roster_keys = self.roster
        other_keys = sorted(list(set(self.keys()) - set(roster_keys)))
        key_order = other_keys + roster_keys

        p = PrettyPrinter()
        print('\n---------- Recipe: ' + self.name + ' ---------------\n')
        for k in key_order:
            print('"{}":'.format(k), end='')
            p.pprint(self.k)
        print(' ')


