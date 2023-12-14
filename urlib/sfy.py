# -*- coding: utf-8 -*-

import os
import typing
from   typing import *

"""This module is designed to produce functions that accept arbitrary
sets of arguments, including iterable and nested iterable objects,
and flatten them out into a single list, which is then processed by a
another function supplied by the caller.

The generate function returns a callable fuction that uses _flatten
to produce a single list and then applies a processing function to
the result.

EXAMPLES:

>>> to_upper = generate(lambda x: ' '.join([r.upper() for r in x]))
>>> to_upper('Convert', 'to', 'uppercase')
'CONVERT TO UPPERCASE'

>>> collapse_and_stringify = generate(lambda x: [ str(_) for _ in x ])
>>> collapse_and_stringify((((1),),[(2,)]),['Buckle',[[[[['my']]]]]],'shoe')
['1', '2', 'Buckle', 'my', 'shoe']

"""

# Credits
__author__ = 'Douglas Broome'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.2'
__maintainer__ = 'George Flanagin, Douglas Broome'
__email__ = 'gflanagin@richmond.edu, dbroome@richmond.edu'
__status__ = 'Prototype'

def _flatten(itr:Iterable) -> Iterable:
    """Collapses nested iterable structures into a single list while
    preserving whole strings instead of treating them as iterable 
    groups of characters """

    result = []

    for _ in itr:
        if hasattr(_,'__iter__') and not isinstance(_,str):
            result += _flatten(_)
        else:
            result.append(_)
    
    return result


def generate(prc:Callable) -> Callable:
    """Returns a function that flattens *args into a single list with
    _flatten and then applies the function passed in prc to the list""" 
     
    def func (*args):
        result = _flatten(args)
        return prc(result)
    
    return func


#Spacify
sfy = generate(lambda lst: " ".join([str(l).strip() for l in lst]))
"""sfy returns a string containing all elements supplied in *args 
converted to string, stripped of any whitespace, and joined by a 
a single space. 
"""

#Returnify
rfy = generate(lambda lst: "\n".join([l for l in lst]))
"""rfy returns a string containing all elements supplied in *args
joined by carriage returns 
"""

#Commafy
cfy = generate(lambda lst: ",".join([str(l).strip() for l in lst]))
"""cfy returns a string containing all elements supplied in *args
converted to a string, stripped of any whitespace, and joined by a
comma.
"""

#Quote-delimited commafy
dcfy = generate(lambda lst: ",".join(['"' + str(l).strip() + '"' for l in lst]))
"""dcfy returns a string containing all elements supplied in *args
converted to a string, stripped of any whitespace, surrounded by
double-quotes, and separated by commas
"""

#Pathify
pfy = generate(lambda lst: os.sep.join([l.replace(' ','\ ') for l in lst]))
"""pfy returns a string containing all elements supplied in *args
    separated by the path delimiter character defined in the os 
    module
    """

if __name__ == '__main__':
    import doctest
    doctest.testmod()
    doctest.testfile('sfytests.txt')

    LIGHTS = 'There are 4 lights!'
   
    assert sfy() == '' 
    assert sfy(None) == 'None' 
    assert sfy('There','are',4,'lights!') == LIGHTS
    assert sfy(['There','are'],'4','lights!') == LIGHTS
    assert sfy(((),),[[]],'There ','   are  ',([4, 'lights!'])) == LIGHTS
else:
    # print(str(os.path.abspath(__file__)) + " compiled.")
    print("*", end="")

