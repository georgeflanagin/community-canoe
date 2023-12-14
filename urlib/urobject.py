# -*- coding: utf-8 -*-
""" This is a base class from which all URLib classes inherit.
Well, all the classes for which it makes sense to do so. This base
class contains essential operators that are safe in the context of
normal operations. """

# System imports
import os
import traceback
import typing
from   typing import *

# URLib imports
import urutils as uu
import time

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Prototype'

# non-class functions

# class
class URObject:
    pass

class URObject:
    """ Base class to provide uniform errors for URLib. """

    def __init__(self, g_data=None):
        """ These are the constituents of the URObject """
        self.g_data = g_data
        self._OK = True
        self._error = 0
        self._error_message = list()
        self._trace = None
        self.void_before = time.time()
        self.void_after = 2**31 - 1


    def __bool__(self) -> bool:
        """ method called by "if" test. """
        return not self._OK


    def __str__(self) -> str:
        """ print out the error. """
        return self._error_message


    def __eq__(self, other:Any) -> bool:
        """ in many cases, this operator will be replaced. As written,
        it ensures equality will be tested in the context of the type
        system. """

        if isinstance(other, type(self)):
            return self.__dict__ == other.__dict__
        elif isinstance(other, str):
            return str(self) == other
        else:
            raise NotImplemented


    def attach_config_data(self, g_data) -> URObject:
        self.g_data = g_data
        return self


    def clear_error(self) -> URObject:
        """ Re-initializes the members of the object. """
        self._OK = True
        self._error = 0
        self._error_message = list()
        self._trace = None
        return self


    def set_error(self, msg, line=0) -> URObject:
        """ This is the magic function. It does a partial stack-unwind
        to determine the way we got here. """

        self._OK = False

        try:
            self._error = uu.this_line(2) if line == 0 else line
            self._error_message.append(str(msg))
            self._trace = uu.formatted_stack_trace()
        except Exception:
            print("This message should never be seen.")

        return self


    def error_message(self) -> str:
        """ Return only the string that was used when set_error() was called. """
        return "\n".join(self._error_message)


    def error_code(self) -> int:
        """ Return the integer. """
        return self._error


    def show_stack(self) -> str:
        """ Show the stack. """
        return str(self._error) + " ::: " + ("\n".join(self._error_message))


    def get_stack(self) -> str:
        """ Show the stack. """
        return "\n".join(self._trace)


    def valid(self) -> bool:
        return self.void_before <= time.time() < self.void_after
 

if __name__ == "__main__":
    def tester():

        config_data = {"x":"y"}
        print("The ID of config_data is " + str(id(config_data)))

        o = URObject()
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
    print("*", end="")

