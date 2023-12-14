# -*- coding: utf-8 -*-


#pylint: disable=anomalous-backslash-in-string
""" One JSON munger to rule them all. """

import os
import pprint as pp
import re
import simplejson 
import sys
import typing
from   typing import *

import fname 
import urutils as uu

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.5'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Prototype'

class JSONReader:
    """ Hack to support forward reference. """
    pass


class JSONReader:
    """ 
    A single purpose JSON converter and syntax checker for 
    reading in JSON data and reporting errors in a useful way. 
    """

    def __init__(self):
        self.origin = None
        self.s = None     

        
    def comment_stripper(self) -> JSONReader:
        """
        Remove (illegal) bash type comments from the source code.
        Build a list of lines that are really JSON, and join them
        back into a string. 
        """
        if __name__ == "__main__": uu.tombstone("Strippin' comments...")
        if self.s is None: return self

        comment_free_lines = []
        for line in self.s.split("\n"):
            if len(line.strip()) == 0: 
                continue

            elif line.strip()[0] == '#': 
                print(line)
                continue

            else: comment_free_lines.append(line)

        self.s = "\n".join(comment_free_lines)
        return self


    def attach_IO(self, f, strip_comments=True) -> JSONReader:
        """
        This function treats a string argument as a filename that needs to 
        be opened, and treats a file as something that needs to be read.

        The function does not /parse/ the contents, but it does return self
        for ease of chaining.

        Raises Exception if no file exists, or if it cannot be opened for
        read access, or if the argument is an incompatible type.
        """

        try:
            x = open(f, 'r')
            self.s = x.read()
            x.close()
            if strip_comments: self.comment_stripper()

        except AttributeError as e:
            # This tests for a filename
            x = fname.Fname(f)
            if not x:
                print("No source named {} exists.".format(x))
                return self

            self.origin = x.fqn
            self.attach_IO(self.origin, strip_comments)   

        except IOError as e:
            uu.tombstone(uu.type_and_text(e))
            return self

        return self


    def convert(self, source=None) -> object:
        """
        Parse the self.s string as JSON.
    
        Returns the parsed object.

        Raises Exception on failure to parse. 

        NOTE: just because the parse is successful, the JSON message might still
        be meaningless. Thus, the name of the function is 'convert' not 'parse.'
        """
        if source is not None:
            self.s = source

        o = None
        try:
            o = simplejson.loads(self.s)

        except simplejson.JSONDecodeError as e:
            start = max(0, e.pos-30)
            stop = min(e.pos+3, len(self.s))

            t = """
    Syntax error: {} 
    in file {} at offset {}
    Line/Column {}/{}.
    Near the end of the phrase 
{} 
    of the original input.""".format(
            e.msg, self.origin, e.pos, e.lineno, e.colno, uu.blind(self.s[start:stop])
            )
            print(t)
            return None

        else:
            return o


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Syntax: python jparse.py file1 [ file2 [ file3 [...]]]")
        exit(0)

    jp = JSONReader()
    printer=pp.PrettyPrinter()

    for f in sys.argv[1:]:
        try:
            print("attempting {}".format(f))
            o = jp.attach_IO(f, True).convert()
            print("Compiled!")
            printer.pprint(o)

        except Exception as e:
            uu.squeal("{} failed to compile.".format(f))
            print(str(e))

else:
    # print(str(os.path.abspath(__file__)) + " compiled.")
    print("*", end="")

