# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from typing import *

""" 
This module reads the config files, and keeps them for ease of access. 
"""

import getpass
import json
import os
import sys
import time
import typing 
from   typing import *

import jparse
import compiler as rcompiler
import urutils as uu

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.2'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Prototype'


__license__ = 'MIT'
import license

class CanoeConfig:
    pass

class CanoeConfig(uu.SloppyDict):
    """ 
    An object representing all the configs and recipes.
    """

    def __init__(self, verbosity:bool=False) -> None:
        self.data_objects_loaded = False
        self.recipes_compiled = False


    def __str__(self) -> str:
        return("{} {}".format(self.data_objects_red, self.recipes_compiled))


    def __len__(self) -> int:
        return len(self.keys()) - 2


    def __missing__(self, name: str) -> None:
        """ 
        Existence of this function prevents raising a KeyError.
        Instead, we just return None. Note that this is a different
        behavior from uu.SloppyDict() where missing keys /do/ raise
        an exception.
        """
        return None


    def finish(self):
        self = uu.deepsloppy(self)
        return self


    def add(self, key:str, value:Any) -> None:
        if key in self: 
            raise Exception('conflicting definition for {}'.format(key))
        self[key] = value


    def add_config(self, filename:str) -> int:
        """ 
        Open a file, assume it is JSON, and build an object. Combine
        the keys from the object with the existing one.

        filename -- a FQN
        returns: -- 1 if file was added
                    0 if file was not a data object
                    None if there was a syntax error. 
        """
        o = None
        try:
            json_reader = jparse.JSONReader()
            o = json_reader.attach_IO(filename, True).convert()

        except Exception as e:
            uu.tombstone('{} contains a syntax error.'.format(filename))
            uu.tombstone(uu.type_and_text(e))
            return None

        else:
            for k in list(o.keys()):
                self[k] = o[k]
        return o


    def get_by_name(self, name:str) -> List[Any]:
        """ 
        retrive an object by name from the config data, even if it
        is one level deep.
        """

        k, v = o.popitem()
        if v and 'schedule' not in v: 
            self.add(k, v)
            return 1
        else:
            return 0


    def replace_object(self, 
            object_name:str, 
            new_object_value:Any) -> CanoeConfig:
        try:
            self.pop(object_name)
        except KeyError as e:
            pass
        else:
            self[object_name] = new_object_value
        
        return self


    def get_recipe_by_name(self, name:str="") -> List[dict]:
        """ 
        Returns a sorted list of recipes that start with name.

        name -- The name of the recipe.
        """
        recipes = []
        for element in self.keys():
            if element.startswith(name) and 'schedule' in element: 
                recipes.append(element)

        return sorted(recipes)


    def get_recipes(self) -> List[dict]:
        """ 
        return a list of objects that are of Recipe type. Note
        that this is quite different from get_recipe_names 
        """
        return self.get_recipe_by_name()


    def get_recipe_names(self) -> List[str]:
        """ Return the names of the recipes. """

        names = []
        for _ in self.keys():
            if 'schedule' in _: names.append(_)
        return names


    def load_all_data(self, home:str="") -> CanoeConfig:
        """
        Using either 'home' or the environment variable CANOE_CONFIG
        as a location, load all the objects that are not recipes.
        """
        if not home:
            try:
                home = os.environ['CANOE_CONFIG']
            except KeyError as e:
                raise Exception('$CANOE_CONFIG is not defined.')

        files = []
        for r, ds, fs in os.walk(home, followlinks=True):
            files.extend(
                [ os.path.join(r, _) for _ in fs if _.endswith('.json') ]
                ) 
        
        i = num_processed = 0
        for i, _ in enumerate(files):
            result = self.add_config(_)
            if result is not None: num_processed += 1
        uu.tombstone('{} of {} files loaded.'.format(num_processed, i))
        self.data_objects_loaded = True


    def load_all_recipes(self, home:str='') -> CanoeConfig:
        """
        Gets the recipes 
        """
        if not self.data_objects_loaded: self.load_all_data(home)
        if not home:
            try:
                home = os.path.join(os.environ['CANOE_CONFIG'], 'local')
            except KeyError as e:
                raise Exception('$CANOE_CONFIG is not defined.')

        compiler = rcompiler.RecipeCompiler(self)
        files = []
        for r, ds, fs in os.walk(home, followlinks=True):
            files.extend(
                [ os.path.join(r, _) for _ in fs if _.endswith('.json') ]
                ) 
        
        successes = 0
        json_reader = jparse.JSONReader()
        for i, f in enumerate(files):
            try:
                o = json_reader.attach_IO(f, True).convert()

            except Exception as e:
                uu.tombstone('{} contains as syntax error.')
                uu.tombstone(uu.type_and_text(e))

            print("compiling {}".format(f))
            recipe, errors, warnings = compiler.compile(o, f)
            if recipe:
                successes += 1
                self[recipe.name] = recipe

        uu.tombstone("searched {} files, compiled {} recipes.".format(
            i, successes))
            
        self.recipes_compiled = True
        return self 


    def load_all_configs(self, home:str='') -> CanoeConfig:
        """ Reads the entire tree of config files.

        NOTE: The idea is that we go to some directory known as the
        home or root directory for this program. We read config files
        in this directory and the subdirectories, skipping the ones
        in $local (which are recipes).

        The objects are then merged.

        returns: -- An object made by compositing all the config data.
        """
        self.load_all_data(home)
        self.load_all_recipes(home)
        self=uu.deepsloppy(self)
        return self


if __name__ == '__main__':
    x = CanoeConfig()
    try:
        home = sys.argv[1]
    except:
        home = ''

    x.load_all_configs(home)
    print("There are now {} global objects.".format(len(x)))
