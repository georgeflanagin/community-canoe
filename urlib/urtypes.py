# -*- coding: utf-8 -*-

# Added for Python 3.5+
import typing
from typing import *

import enum
from http import HTTPStatus

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2017, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Prototype'


# Turn on covariant type enforcement. In covariant mode, enforce will consider subclasses
# of a type named in a type hint to pass the type check. In invariant mode, class instances
# would need to be exactly the same type to pass the type check.
# ----------
# The loc that does the work is commented out here. We are turning off enforcement for
# now because it doesn't work with types declared with NewType(). We will turn it back
# on in a future version if it acquires this capability.
# ----------
#enforce.config({'mode':'covariant'})

DB_ROW = NewType('DB_ROW', Dict)
DB_ROWSET = NewType('DB_ROWSET', List[Dict])

class TCSPOp(enum.Enum):
    Get =    0
    Put =    1
    Delete = 2
    Rename = 3

TCSPOpResult = NamedTuple('TCSPOpResult',[('Op',TCSPOp),('Status',HTTPStatus),
                                          ('SourcePath',Tuple[str,str]),('TargetPath',Tuple[str,str])])

TCSPResults = NewType('TCSPResults',List[TCSPOpResult])
