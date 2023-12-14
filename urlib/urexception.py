# -*- coding: utf-8 -*-

import os

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Prototype'

class URException(Exception):
    """ Base exception for any classes in urlib """

    def __init__(self, msg:str=None, line:int=None):
        Exception.__init__(self)
        self.line = line if line else 0
        self.msg = msg

    def __str__(self):
        if self.line != 0:
            return " On line " + str(self.line) + " :: " + str(self.msg)
        else:
            return str(self.msg)

class URDBException(URException):
    """ Raised where there are unrecoverable database errors. """

    def __init__(self, msg=None, line=None):
        URException.__init__(self, msg, line)



if __name__ == "__main__":
    pass
else:
    # print(str(os.path.abspath(__file__)) + " compiled.")
    print("*", end="")

