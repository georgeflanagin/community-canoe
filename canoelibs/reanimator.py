# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from typing import *

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2018, University of Richmond'
__credits__ = None
__version__ = '1.0'
__maintainer__ = 'George Flanagin, Douglas Broome'
__email__ = 'gflanagin@richmond.edu, dbroome@richmond.edu'
__status__ = 'Production'
__required_version__ = (3, 5)
__license__ = 'MIT'

import importlib
import os
import sys

import tombstone as tomb

class ReAnimator:

    def __init__(self, packages:List[str]) -> None:
        """
        Note that this routine sets up the object, but does not load any
        code.
        """
        self.handles = {}
        self.packages = []
        self.add_packages(packages)


    def add_packages(self, packages:Union[str,List[str]]) -> int:
        """
        packages -- str or list thereof with package names.

        returns -- the revised number of packages. NOTE: adding a package
            already in the list does nothing.
        """
        if isinstance(packages, str): packages = [packages]
        self.packages.extend(packages)
        self.packages = list(set(self.packages))
        return len(self.packages)


    def load_code(self) -> int:
        """
        populate the code library when we start.

        returns -- number of modules that are loaded.
        """
        try:
            for package_name in self.packages:
                for _ in importlib.import_module(package_name).__all__:
                    self.handles[_] = importlib.import_module(_)
        except Exception as e:
            sys.stderr.write(str(e) + '\n')
            raise

        return len(self.handles)
        

    def reload_code(self) -> Tuple[int, int]:
        """
        using the (possibly new) list in package.__all__, attempt to reload
        the modules. If they fail to reload it is because the module is
        not already in the list, and must be loaded.

        returns -- A tuple, with the number of modules that are (loaded, reloaded).
        """
        new_handles = {}

        for package_name in self.packages:
            for name in importlib.import_module(package_name).__all__:
                try:
                    self.handles[name] = importlib.reload(self.handles[name])
                except IndexError as e:
                    new_handles[name] = importlib.import_module(name)
                except Exception as e:
                    sys.stderr.write(str(e) + '\n')
                    raise

        self.handles = {**self.handles, **new_handles}

        return len(new_handles), len(self.handles) - len(new_handles)


    def __str__(self) -> str:
        return "Loaded modules: {}".format(', '.join(self.handles.keys()))


if __name__ == '__main__':
    obj = ReAnimator(['plugins','urlib','canoelibs'])
    try:
        print('Loaded {} modules initially.'.format(obj.load_code()))
        x, y = obj.reload_code()
        print('Loaded {} modules and reloaded {} modules'.format(x, y))
        print('We now have {} packages.'.format(obj.add_packages('canoelibs')))
        print('We now have {} packages.'.format(obj.add_packages('canoelibs')))
        x, y = obj.reload_code()
        print('Loaded {} modules and reloaded {} modules'.format(x, y))

        print(str(obj))
    except Exception as e:
        print(str(e))
else:
    pass
