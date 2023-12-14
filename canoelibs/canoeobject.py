# -*- coding: utf-8 -*-
""" This is a base class from which all the Canoe classes inherit.
Well, all the classes for which it makes sense to do so. This base
class contains essential operators that are safe in the context of
normal operations. """

# System imports
import traceback
import os
import typing
from   typing import *

# Canoe imports
import urobject as ur

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


# class
class CanoeObject(ur.URObject):
    """ Base class to provide uniform errors for URLib. """

    def __init__(self, g_data=None):
        ur.URObject.__init__(self,g_data=g_data)

if __name__ == "__main__":
    def tester():

        config_data = {"x":"y"}
        print("The ID of config_data is " + str(id(config_data)))

        o = CanoeObject()
        o.attach_config_data(config_data)

        print("The ID of the object's config data is " + str(id(config_data)))

        o.set_error("An error message")
        print(o.error_code())
        print(o.error_message())
        print(o.get_stack())

        exit(0)

    tester()
else:
    # print(str(os.path.abspath(__file__)) + " compiled.")
    print('*', end='')
