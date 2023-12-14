#!/usr/bin/python
# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from typing import *

""" 
A Recipe is the parsed, ready-to-eat version of the JSON representation
of a Canøe job. The RecipeCompiler is a factory for Recipes.
"""

# System imports
import argparse
import collections
from   collections.abc import *
import configparser
import copy
import datetime
from   datetime import datetime
import glob
import inspect
import json
import math
import numpy
import os
import pprint
import shutil
import string
import sys
import time

# Pip installed imports

import gnupg

# Canoe imports
import canoecrypter
import canoedb
import canoestats
import fname
import grammar
from   grammar import *
import ijklparser
from   ijklparser import IJKLparser
import jparse as jp
from   recipe import Recipe
from   urdecorators import show_exceptions_and_frames as trap
import urpacker
import urutils as uu


# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2018, University of Richmond'
__credits__ = None
__version__ = '0.9'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Development'

from_command_line = __name__ == '__main__'

__license__ = 'MIT'
import license

###
# non-class functions to handle keys in a uniform way.
###
gpg_obj = gnupg.GPG(binary=shutil.which('gpg'), 
    homedir=os.path.join(os.path.expanduser('~'), '.gnupg'))

def filter_keys(gpg_obj:gnupg.GPG, *, 
        expiry_date:int=int(time.time()),
        min_len:int=grammar.GPG_MIN_LEN,
        grace_period:float=grammar.GPG_GRACE_PERIOD,
        max_age:int=grammar.GPG_MAX_YEARS) -> list:
    """
    Build a list of valid keys, and add warings for short, old, and expiring keys.

    expiry_date     -- Defaults to "now". (UOM:seconds)
    min_len         -- UOM: bits
    grace_period    -- UOM: years)
    max_age         -- UOM: years)

    returns         -- list of SloppyDicts of info about non-expired keys.
    """
    now = int(time.time())

    # if max_age is 5, we want keys less than 5 years old.
    earliest_creation = 0 if max_age == 0 else now - max_age*uu.Konstants.YEAR_IN_SECONDS
    grace_date = now + grace_period*uu.Konstants.YEAR_IN_SECONDS

    keys = uu.deepsloppy(gpg_obj.list_keys())
    keys = [ k for k in keys if not len(k.expires) or int(k.expires) > expiry_date ]

    for i, k in enumerate(keys):
        # Make all the UID text lower case so that users can refer to
        # either "Cigna" or "cigna" without worry.
        k.uids = " ".join(k.uids).lower()
        k.old = int(k.date) < earliest_creation
        k.short = int(k.length) < min_len
        k.expiring_soon = len(k.expires) and int(k.expires) < grace_date
        keys[i] = k

    return keys
         


this_commit = uu.version(full=False)
compiler_mod_time = str(datetime.fromtimestamp(os.stat(__file__).st_mtime))[:19]

def main_tombstone(o:object) -> str:

    """
    Calls tombstone iff we are running recipe.py as a program. Otherwise,
    this statement has no effect.
    """
    return uu.tombstone(o) if __name__ == '__main__' else ""


class RecipeCompiler:
    """
    Build a Recipe class object from a JSON file.

    The basic plan for use of this code is:

    1.      c = RecipeCompiler()
    ---------------------------------------------

    The RecipeCompiler will open the database, get a s/n, and read
        Canøe's global configuration.
    The compiler can be reused; just give it another file name, and 
        it will create a new compiled object. The compiler should not
        raise exceptions unless something is so wrong that the
        compiler's own code cannot be executed.

    2.      obj, errors, warnings = c.compile(file_name)
    ----------------------------------------------

    The RecipeCompiler.compile function works as a recipe factory, producing
        a compiled object, and the count of errors & warnings. 
        The return value of obj will be None if there are errors or warnings.

    3. Internally, the plan is:
    ----------------------------------------------

        NOTE: ** this_fcn = inspect.stack()[0][3] ** causes the function
            name to be printed at the time it is called.

        check_* -- functions that tackle top-level sections of the 
            recipe.

        _validate_* -- functions called from check_* functions that resolve
            the meanings of common items such as 'host' and 'file'.

        __* -- functions that are helpers from within _validate_* family.

    """

    @trap
    def __init__(self, opts:argparse.Namespace):
        """ 
        Build the compiler.
        """
        self.current_path = None
        self.current_recipe = None
        self.errors = 0
        self.warnings = 0
        self.fatal = False
        self.home = None
        self.g = {} 
        self.opts = opts

        try:
            self.db = canoedb.default()
        except Exception as e:
            uu.tombstone(uu.type_and_text(e))
            raise
        else:
            if not self.db:
                raise Exception("Unable to open default database.")

        files = []
        for r, ds, fs in os.walk(opts.config, followlinks=True):
            files.extend(
                [ os.path.join(r, _) for _ in fs if _.endswith(opts.ext) ]
                )

        i = num_processed = 0
        for i, _ in enumerate(files):
            result = self.add_config(_)
            if result is not None: num_processed += 1
            for k in list(result.keys()): self.g[k] = result[k]

        self.g = uu.deepsloppy(self.g)
        if not opts.quiet: uu.tombstone('{} config files loaded.'.format(num_processed))


    @trap
    def add_config(self, filename:str) -> int:
        """
        Open a file, assume it is JSON, and build an object. Combine
        the keys from the object with the existing one.

        filename -- a FQN
        returns: -- 1 if file was added
                    0 if file was not a data object
                    None if there was a syntax error.
        """
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        o = None
        try:
            json_reader = jp.JSONReader()
            o = json_reader.attach_IO(filename, True).convert()
            return o

        except Exception as e:
            uu.tombstone('{} contains a syntax error.'.format(filename))
            uu.tombstone(uu.type_and_text(e))
            return None


    @trap
    def fatal_error(self, s:str) -> None:
        uu.tombstone(uu.blind(f"ERROR: {s}"))
        self.errors += 1
        self.fatal = True


    @trap
    def non_fatal_error(self, s:str) -> None:
        uu.tombstone(uu.blind(f"WARNING: {s}"))
        self.warnings += 1


    @trap
    def compile(self, 
            source:dict, 
            current_path:str) -> Tuple[Recipe, int, int]:

        """
        Compiles a data object that may have been read from a .json
            file. After compilation it returns the new Recipe or None.

        source -- a JSON object.
        """

        if len(list(source.keys())) != 1:
            print("There appears to be more than one recipe in the source file.")
            print("The names are {}".format(list(source.keys())))
            print("Unfortunately, this is currently illegal.")
            sys.exit(os.EX_DATAERR)

        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        timer = uu.Stopwatch()
        source = uu.deepsloppy(source)

        self.errors = self.warnings = 0
        self.home = self.current_path = current_path
        self.current_recipe = recipe = Recipe()
  
        # We need to know where we came from and when.
        recipe['origin'] = self.current_path
        recipe['compiled_time'] = ( int(time.time()), uu.now_as_string() )
        recipe['compiler_info'] = (this_commit, compiler_mod_time)

        ################################################################
        # 1: separate the name & body, create a new home dir if needed,
        #    and export the name of the home dir as an env variable so
        #    that we can substitute tokens later on.
        ################################################################
        try:
            recipe['name'], body = source.popitem()
            for key in list(body.keys()):
                if key not in DOCUMENTARY_SECTIONS and uu.notlikeanyof(key, ACTIONS):
                    self.warnings += 1
                    uu.tombstone(uu.blind(f"Found a removed {key=} with value << {body[key]} >>"))
                    del body[key]

            self.current_name = recipe.name
            os.environ['mydir'] = self.home = recipe['this_dir'] = os.path.join(
                os.environ.get('CANOE_DATA', '/sw/canoe/var/data'), recipe.name
                )
            self._validate_local_directory(self.home)

        except ValueError as e:
            uu.tombstone(uu.type_and_text(e))
            uu.tombstone('No name to recipe.')
            return None, 1, 0

        ################################################################
        # 2. copy/rearrange all the items in the recipe.
        ################################################################
        recipe = uu.deepsloppy({
            **recipe, 
            **{k:v for k, v in body.items() if k not in ['name']}
            })

        # Make sure we have documentary sections so that we will check them.
        for k in DOCUMENTARY_SECTIONS:
            if k not in recipe: recipe[k] = DOCUMENTARY_DEFAULTS[k]

        ################################################################
        # 3. Discard obsolete and obsolescent keys in legacy recipes.
        ################################################################
        for k in DISCARD_THESE_KEYS: 
            try:
                recipe.pop(k)
                self.non_fatal_error(f"WARNING: found and removed obsolete element {k}")
            except KeyError as e:
                pass

        if 'rerun_ok' not in recipe.keys():
            recipe['rerun_ok'] = True
        if 'frequency' not in recipe.keys():
            recipe['frequency'] = 'unknown'

        ################################################################
        # 4. Roster. This element is generated by the compiler, and 
        #    consists of a sequence of steps that represent the order
        #    of execution of the recipe.
        ################################################################
        recipe['roster'] = []

        ################################################################
        # 5. Schedule!
        ################################################################

        # This could be a non-executable recipe; i.e., a data object,
        # that got here by accident. In that case, we just return the 
        # data we were passed.
        try:
            _ = recipe.schedule
        except:
            uu.tombstone(uu.blind('This is not a recipe.'))
            return None, 1, 0

        # OK, this is a potentially valid recipe. Let's see about its schedule.
        # If the schedule is hosed, we will still look at the rest of the recipe
        # for errors.
        try:
            recipe.schedule = uu.parse_schedules(recipe.schedule)
        except Exception as e:
            self.fatal_error(f"schedule is malformed: {recipe.schedule}")

        # The default recipe-level debug is False.
        if 'debug' not in recipe: recipe['debug'] = False
        self.current_recipe = recipe

        ################################################################
        # 6. Check the recipe and apply the tables and rules. We want to
        #    check the keys that can be checked.
        ################################################################
        for section in [ x for x in recipe.keys() if x not in IGNORED_SECTIONS]: 
            if self.fatal: break

            section_type    = self.root_name(section)
            section_checker = f"check_{section_type}"

            try:
                # Get the transforms from the table, and apply them in order 
                # from the grammar definition.
                for transform in KEYWORD_TRANSFORMS.get(section_type, []):
                    recipe[section] = transform(recipe[section])

                # If the text of the section_type is a literal, don't check anything.
                if self.is_literal(recipe[section]): continue

                # Any section_type can have a 'debug' parameter, which we will set
                # to False if it is not specified. The exact meaning of the 
                # debug flag might vary from section_type to section_type; i.e., a boolean
                # or perhaps an integer for a debug level. If there is nothing explicit,
                # set the debug level to True if either the recipe's debug is True or
                # the compiler has been invoked with debug.
                if isinstance(recipe[section], dict) and 'debug' not in recipe[section]:
                    recipe[section]['debug'] = self.opts.debug or recipe.debug

                # Now call the check_ function on the transformed source code.
                foo = getattr(self, section_checker, self.null_transform)
                recipe[section] = foo(recipe[section]) 
                if section_type not in DOCUMENTARY_SECTIONS: recipe.roster.append(section)
                
            except Exception as ex:
                uu.tombstone(uu.type_and_text(ex))
                raise


        for section in ('pgpinspect', 'cleanup'):
            if uu.nothinglikeit(section, recipe.keys()):
                recipe[section] = {}
                recipe[section]['debug'] = self.opts.debug
                recipe[section]['local_dir'] = self.home
                recipe[section]['on_error'] = 'proceed'
                if section == 'cleanup': recipe[section] = uu.listify(recipe[section])
                recipe.roster.append(section)


        # Set the default value for missing keys. We provide the key-value pair
        # in the cases where they do not exist. This prevents constantly looking
        # to see if they are present later on.
        for k in TOP_LEVEL_KEYS:
            if recipe.get(k, math.nan) == math.nan and TOP_LEVEL_DEFAULTS[k] is not None: 
                recipe[k] = TOP_LEVEL_DEFAULTS[k]

        ################################################################
        # 7. Proofread the recipe for 'on_error' values in the sections, and supply
        #    a default directive if the section doesn't have a prescribed action.
        ################################################################
        for k in recipe.keys():
            if isinstance(recipe[k], dict):
                recipe[k]['on_error'] = self._validate_on_error(
                    recipe[k].get('on_error', ERROR_ACTION.default_name())
                    )

            elif isinstance(recipe[k], list):
                for j in range(0, len(recipe[k])):
                    if isinstance(recipe[k][j], dict):
                        recipe[k][j]['on_error'] = self._validate_on_error(
                            recipe[k][j].get('on_error', ERROR_ACTION.default_name())
                            )
            else:
                pass

        ################################################################
        # 8. Figure out whom to notify.
        ################################################################
        recipe['mailto'] = f'{XFORM_OPS.mail} -s "{recipe.name} failed {{}}"'
        recipe['affirmation'] = f'{XFORM_OPS.mail} -s "{recipe.name} completed w/o error."'

        # Failure notifications go to the devlead(s) and the owner(s)
        if 'owner' in recipe.devlead:
            recipe.devlead = recipe.devlead | set(recipe.owner)

        # Append these folks to the command.
        for dev in recipe.devlead:
            if '@' not in dev:
                dev = f"{dev}@richmond.edu"
            recipe.mailto += f" {dev}"

        recipe.mailto      += " < /dev/null"

        ###
        # Generally, devleads do not need to be notified about
        # success, so we just add the owners who want the confirmation.
        ###
        for o_ in recipe.owner:
            recipe.affirmation += f" {o_}" if '@' in o_ else f" {o_}@richmond.edu"
        recipe.affirmation += " < /dev/null"

        timer.stop()

        if not self.opts.quiet: print(str(timer))
        global from_command_line
        if self.errors == 0 or from_command_line:
            return recipe, self.errors, self.warnings
        else:
            return None, self.errors, self.warnings


    @trap
    def is_literal(self, o:Any) -> bool:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        if isinstance(o, dict): 
            return LITERAL in o.keys() 
        elif isinstance(o, list):
            return LITERAL in o
        else:
            return False


    @trap
    def null_transform(self, o:Any) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        return o


    @trap 
    def root_name(self, section_name:str) -> str:
        """
        Return the section_name without the numerical suffix on the end.
        """
        """
        Return the section_name without the numerical suffix on the end.
        """
        try:
            location = section_name.rindex('_')
            plugin_name = section_name[:location]
            return plugin_name if plugin_name in ITERABLE_SECTIONS else section_name

        except ValueError as e:
            return section_name


    #***********************************************************
    # check_* functions from here on.
    #***********************************************************
    @trap
    def check_allowed_environments(self, o:Any) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        # self.errors += __check_type(inspect.stack()[0][3])
        if set(o) - ENVIRONMENTS != set(): 
            self.non_fatal_error(f"Unknown environment: {o - ENVIRONMENTS}")

        return o
            

    @trap
    def check_bunzip2(self, o:Any) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        if not self._validate_elements(BUNZIP2_KEYS, o): return None
        o['local_dir'] = self.home
        return o
        
        
    @trap
    def check_comment(self,o:Any) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        # self.errors += __check_type(inspect.stack()[0][3])

        for _ in o:
            if not isinstance(_, str):
                self.non_fatal_error(f"Comments must be strings: {_}")

        return o


    @trap
    def check_dashboard(self, o:Any) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        o = self.__set_defaults(o, DASHBOARD_DEFAULTS)
        o.local_dir = self.home
        
        return uu.SloppyDict(o)


    @trap
    def check_date_offset(self,o:Any) -> object:
        # self.errors += __check_type(inspect.stack()[0][3])
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        try:
            i = int(o)
        except:
            self.fatal_error("date_offset must be an integer")

        return o


    @trap
    def check_dbload(self,o:Any) -> object:
        # self.errors += __check_type(inspect.stack()[0][3])
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        # See if the required ones are present.
        missing = DBLOAD_KEYS_REQ - set(o.keys)
        if missing != PHI:
            self.errors += 1
            self.fatal = True
            uu.tombstone('dbload is missing key[s] {}'.format(missing))
            return

        o.db = _validate_db(o.db)
        o.tables = uu.listify(o.tables)
        if 'remap' not in o: o['remap'] = {}
        if 'badfile' not in o: o['badfile'] = None        
        if 'format' not in o: o['format'] = {'sep':'|', 'header':True}
        if 'splits' not in o: o['splits'] = []

        # Ensure the lack of strays.
        strays = set(o.keys) - DBLOAD_KEYS
        if strays != PHI:
            self.warnings += 1
            uu.tombstone(uu.blind('WARNING: dbload contains unrecognized keys {}'.format(strays)))

        return o


    @trap
    def check_destination(self, o:Any) -> object:
        # self.errors += __check_type(inspect.stack()[0][3])
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        new_o = []

        for e in o:
            e = self.__assign_debug(e)
            which_endpoint, e = self._validate_endpoint(e)
            metachars = uu.contains_metachars(e.file)

            # If it is an absolute path, leave it be. Otherwise, prepend
            # the integration's working directory.
            if not e.file.startswith(os.sep):
                e.file = os.path.join(self.home, e.file)
            else:
                this_dir = os.path.dirname(e.file)
                if not os.path.isdir(this_dir):
                    uu.tombstone(f"{this_dir} does not exist. Attempting to create it.")
                    try:
                        os.mkdirs(this_dir)
                    except:
                        uu.tombstone(f"Unable to create {this_dir}.")
                        self.errors += 1
                        self.fatal = True

            # Apply slightly different rules to the three destinations.
            if which_endpoint == 'box': 
                # The default zipping for box is to do it.
                if e.zip is None: e.zip = ZIP_OPTS['gzip']
                e.box = self._validate_box(e.box)
                e.directory_alias = e.get('directory')
                e.directory = self.__box_folder_idstr_from_name(e.directory)

            elif which_endpoint == 'host':
                e.host = self._validate_host(e.host, e.password)
                if e.zip is None: e.zip = ZIP_OPTS[False]
                e.overwrite = True
                
            elif which_endpoint == 's3':
                e.opts = self._validate_s3(e.s3)
                e.overwrite = True
                s3_sub_cmd = 'mv' if e.delete is True else 'cp'
                if metachars:
                    print("using S3_DEST_CMD_MANY")
                    e['ops'] = S3_DEST_CMD_MANY.format(
                        s3_sub_cmd, os.environ.get('mydir'), 
                        e.opts.name, e.directory, 
                        e.file, e.opts.name
                        )
                else:
                    e['ops'] = self._validate_ops_block(
                        S3_DEST_CMD_1.format(
                            s3_sub_cmd,
                            e.file, e.opts.name, 
                            e.directory, 
                            e.opts.name
                            )
                        )

            elif which_endpoint == 'sharefile':
                e.sharefile = self._validate_sharefile(e.sharefile)
                e.overwrite = True
                if e.zip is None: e.zip = ZIP_OPTS[False]
                
            elif which_endpoint == 'azure':
                pass

            elif which_endpoint == 'curl':
                e.curl = self._validate_curl(e.curl)

            else:
                self.fatal_error(f"this version of the compiler does not support {which_endpoint}")       

            del e.password
            new_o.append(e)

        ###
        # Because of unreliability of Box, and the fact that we are by default
        # gzipping the files that are uploaded to it, we need to correct the
        # author, and put box last. We want to preserve the order of the non-box
        # steps, and the order of the box ops, so we will go with this order
        # preserving separation-sort.
        ###
        sorted_ops = []
        box_ops = []
        for o in new_o:
            if 'box' not in o: sorted_ops.append(o)
            else: box_ops.append(o)
        return sorted_ops + box_ops

        

    @trap
    def check_devlead(self, developers:list) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        developers = set(developers)
        if not developers: 
            uu.tombstone(uu.blind('WARNING: No devlead for this task.'))
            self.warnings += 1

        not_approved = set([ netid for netid in developers if netid not in ADMINSYS ])
        if not_approved != PHI:
            self.warnings += 1
            uu.tombstone(uu.blind(f"WARNING: Notifications outside of group: {not_approved}"))

        developers.add('canoe')
        developers.add('banner')

        return developers


    @trap
    def check_encryptpics(self, o:dict) -> uu.SloppyDict:
        """
        This is a plugin that encrypts pictures for delivery
        to vendors.
        """        
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        
        new_o = copy.copy(ENCRYPTPICS_DEFAULTS)
        for k in o: new_o[k] = o[k]
        if any(new_o[k] is None for k in new_o):
            self.errors += 1
            self.fatal = 1
            uu.tombstone(f"Missing a key from this list {ENCRYPTPICS_KEYS}")
            return new_o

        new_o['local_dir'] = self.home
        new_o['exe'] = XFORM_OPS['gpg']
        new_o['signature'] = GPG_SIGNING_KEYS
        new_o = uu.SloppyDict(new_o)
        new_o.publickey = uu.listify(new_o.publickey)
        new_o.publickey = [ 
            self.___resolve_recipient(_).split()[1] for _ in
            new_o.publickey 
            ]
        new_o.publickey.extend(GPG_RECIPIENTS)

        return new_o


    @trap
    def check_flags(self, o:Any) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        Look through the list of flags.
        """
        return o


    @trap
    def check_framediff(self, o:Any) -> object:
        """
        framediff is an original plugin, and this function is something
        of a proof of concept for how to check plugins.
        """
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        o['dirr'] = self.home
        o = uu.sloppy(o)
        if not self._validate_elements(FRAMEDIFF_KEYS, o): return None

        if not os.path.isdir(o.dirr): 
            uu.make_dir_or_die(o.dirr)
            uu.tombstone("created directory {}.".format(o.dirr))
        
        try:
            o.sep = o.sep
        except:
            o['sep'] = ','

        if 'debug' not in o: o['debug'] = self.opts.debug
        return o        


    @trap 
    def check_frequency(self, v:str) -> str:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        if v not in FREQUENCY_KEYS:
            self.warnings += 1
            uu.tombstone(uu.blind(f"WARNING: unknown time interval {v}"))
        
        return FREQUENCY_TRANSLATIONS.get(v, '?')


    @trap
    def check_fusionpics(self, o:Any) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        """
        This is a basic keep what is written, and supply any defaults
        from the grammar file.
        """
        o = self.__set_defaults(o, FUSIONPICS_DEFAULTS)
        o.local_dir = self.home
        return o


    @trap
    def check_grap(self, o:Any) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        
        new_o = uu.SloppyDict()
        new_o['box'] = self._validate_box('urbox')
        for k, v in GRAP_DEFAULTS.items():
            new_o[k] = o.get(k, v)

        if new_o.box_stage is None:
            uu.tombstone(f"A value for {new_o.box_stage} is required.")
            self.errors += 1
            self.fatal = True

        new_o.box_stage = self.__box_folder_idstr_from_name(new_o.box_stage)
        if new_o.box_backup: 
            new_o.box_backup = self.__box_folder_idstr_from_name(new_o.box_backup)

        if new_o.destination_dir is not None: 
            new_o.destination_dir = uu.expandall(new_o.destination_dir)
        new_o['local_dir'] = self.home

        return None if self.fatal else new_o


    @trap
    def check_keymaint(self, o:dict) -> object:
        """
        This special operation does not contain any required or 
        checkable parameters.
        """
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        
        new_o = uu.SloppyDict()
        if isinstance(o, dict):
            for k in o:
                new_o[k] = o[k]

        new_o['debug'] = self.opts.debug if 'debug' not in new_o else new_o.debug
        new_o['on_error'] = 'proceed' if 'on_error' not in new_o else new_o.on_error
        new_o['local_dir'] = self.home

        print(new_o)
        
        return new_o


    @trap
    def check_chromefilter(self, o:Any) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        Separate out the inbound CSV into one CSV per transaction 
        file name.
        """
        allowed_dispositions = ['keep', 'move', 'remove']
        o = uu.sloppy(o)
        if 'debug' not in o: o['debug'] = self.opts.debug
        if not self._validate_elements(CHROMEFILTER_KEYS, o): return None
        o['local_dir'] = self.home
        if 'column_order' not in o: o['column_order'] = None
        if 'original' not in o: o['original'] = 'move'
        if o.original not in allowed_dispositions:
            self.warnings += 1
            uu.tombstone(uu.blind(
                'WARNING: Allowable values for "original" are {}'.format(allowed_dispositions)))

        return o        


    @trap
    def check_metadata(self, o:Any) -> object:
        """
        The metadata are an arbitrary list of terms, either space or
        comma delimited. Underscores and hyphens are removed, and the
        terms are converted to lower case.
        """
        # self.errors += __check_type(inspect.stack()[0][3])
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        if isinstance(o, str): 
            o = o.strip().replace(',',' ').lower().split()

        if not isinstance(o, list):
            uu.tombstone(uu.blind('WARNING: Unusuable metadata: {}'.format(o)))
            self.warnings += 1
    
        for i, _ in enumerate(o):
            o[i] = _.replace('_','').replace('-','')

        for i, _ in enumerate(o):
            if _ in DATA_CLASS_TAGS: 
                o.append(DATA_CLASS_TAGS[_])     
                break       

        return o


    @trap
    def check_next_job(self,o:Any) -> object:
        """
        Note: next_job is vestigial, and may be removed.
        """
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        uu.tombstone(uu.blind('WARNING: next_job is obsolete and has no effect.'))
        self.warnings += 1

        return o


    @trap
    def check_notifications(self, o:Any) -> object:
        """
        Note: notifications relating to Nagios are vestigial, and
        have no effect. However, other notifications are OK. The
        code below corrects/flips the keys and values in the 
        dictionary that most people used in times past.
        """
        # self.errors += __check_type(inspect.stack()[0][3])
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        # if isinstance(o, dict): o = [ i for j in o for i in j.values ] 

        targets = []

        for target in o:
            try:
                i_target = int(target)
                """
                From NANP: the first digit is never less than two, and 9 is never
                    the second digit. 
                """
                if 1999999999 < i_target < 9900000000:
                    gateway = self.g.phones.get(str(i_target), None)
                    if gateway is None:
                        self.warnings += 1
                        uu.tombstone(uu.blind("WARNING: {} may not be callable -- no gateway".format(i_target)))
                    else:
                        targets.append("{}@{}".format(i_target, gateway))
                else:
                    uu.tombstone('unusable NANP phone number {}'.format(target))
                    self.errors += 1

                continue
        
            except:
                pass

            if isinstance(target, str):
                if '@' not in target:
                    target = "{}@{}".format(target, self.g.sys_params['default_domain'])
                targets.append(target)
            else:
                self.errors += 1
                uu.tombstone("invalid item in notifications: {}".format(target))
                self.fatal = True
        
        return targets


    @trap
    def check_owner(self,o:Any) -> object:
        """
        Owner is just a piece of metadata at the moment.
        """
        # self.errors += __check_type(inspect.stack()[0][3])
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        if not isinstance(o, (str, list)):
            self.errors += 1
            print("owner must be a string or a list of strings")
            return o

        o = uu.listify(o)
        return o


    @trap
    def check_password(self,o:str) -> object:
        """
        The password should /not/ be coded in the recipe definition. Instead,
        one should find a placeholder, and go look it up in the encrypted
        database.
        """
        # self.errors += __check_type(inspect.stack()[0][3])
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        if o != self.g.sys_params['CREDENTIAL']: return o

        # the get_credentials_by_name function returns an error message if
        # no password is found, so we check for a space in the result.
        result = self.db.get_credentials_by_name(self.db.sn, self.current_name, 'password')
        # if ' ' in result: self.warnings += 1
        return result


    @trap
    def check_pgpinspect(self, o:Any) -> object:
        """
        This lets us be a little flexible in specifying this operation
        """
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        if not o: o = uu.SloppyDict({'local_dir':self.home})
        elif isinstance(o, str): o = uu.SloppyDict({'local_dir':o})
        elif isinstance(o, dict): o['local_dir'] = self.home
        else:
            self.errors += 1
            self.fatal = True
            uu.tombstone(f'pgpinspect <{o}> must be empty, a dict, or a string.')

        return o
        

    @trap
    def check_studentpics(self, o:object) -> uu.SloppyDict:
        """
        Simple check of a few keys.
        """
        o = uu.deepsloppy(o)
        for k in STUDENTPICS_KEYS:
            if k not in o:
                self.errors += 1
                uu.tombstone(f"Missing key {k} in studentpics")
                self.fatal = True

        if not os.path.isdir(o.images):
            self.warnings += 1
            uu.tombstone(f"{o.images} does not (yet?) exist on this machine.")

        o.input = f"{self.home}/{o.input}"

        o['local_dir'] = self.home
        o['debug'] = o.get('debug', False)

        return o

    @trap
    def check_xml2csv(self, o:object) -> uu.SloppyDict:
        """
        Do the appropriate checking of the xml2csv transformations, and
        generate the correct code.
        """
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        o = uu.listify(uu.SloppyDict(o)) 

        if not self._validate_elements(XML2CSV_KEYS, o): return None

        o['local_dir'] = self.home
        if 'input' not in o.keys(): o['input'] = 'tempfile'
        o.xpath = uu.listify(o.xpath)
        for i, component in enumerate(o.xpath):
            if not isinstance(component, str):
                uu.tombstone('xpath component #{} is not a string.'.format(i))
                self.errors += 1
                self.fatal = True
                return None

            if component.startswith('/'):
                uu.tombstone('xpath component {} starts with a slash'.format(component))
                self.errors += 1
                self.fatal = True
                return None

        if isinstance(o.columns, dict):
            pass
        elif isinstance(o.columns, list):
            o.columns = uu.sloppy(dict(zip(o.columns.keys(), o.columns.keys())))
        else:
            uu.tombstone('columns must be either a dict or a list.')
            self.errors += 1
            self.fatal = True
            return None
            
        return o


    @trap
    def check_xmlscrub(self, o:object) -> uu.SloppyDict:

        o['local_dir'] = self.home
        if 'input' not in o.keys(): o['input'] = '*.xml'
        if 'output' not in o.keys(): o['output'] = self.home
        if 'debug' not in o.keys(): o['debug'] = self.opts.debug
    
        return uu.SloppyDict(o)


    @trap
    def check_cr_images(self, o:object) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        
        if not self._validate_elements(CR_IMAGES_KEYS, o): return None

        o = uu.SloppyDict(o)
        o.db = self._validate_db(o.db)
        o.host = self._validate_host(o.host)
        o['local_dir'] = self.home
        if 'debug' not in o: o['debug'] = self.opts.debug

        return o


    @trap 
    def check_cr_mastercard(self, o:object) -> object:
        """
        Processing instructions for the cr (Chrome River) mastercard
        transaction file contained in the cr_mastercard plugin.
        """
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        o = self.__set_defaults(o, CR_MASTERCARD_DEFAULTS)
        o.db = self._validate_db(o.db)
        o.local_dir = self.home
        if not o.exceptions.startswith(os.sep): 
            o.exceptions = os.path.join(o.local_dir, o.exceptions)
        return o
                

    @trap
    def check_randomfile(self, o:object) -> object:
        keys = ['prefix', 'output']
        for i, element in enumerate(o):
            element['local_dir'] = self.home
            for k in keys:
                if k not in element:
                    uu.tombstone("No {} in {}".format(k, element))
                    self.errors += 1
                    self.fatal = True
                    return None
            o[i] = element
        if 'debug' not in o: o['debug'] = self.opts.debug

        return o


    @trap
    def check_remote_ops(self, o:Any) -> object:
        """
        Note: this code is also called for cleanup. Anything that can be
        done on a remote or local host can be done at the end of the
        execution.
        """
        # self.errors += __check_type(inspect.stack()[0][3])
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        for _ in o:
            _ = self.__assign_debug(_)
            _['local_dir'] = self.home
            if 'unique' in _: 
                _['unique'] = uu.boolify(_['unique']) 
            else:
                _['unique'] = False

            if 'debug' not in _: _['debug'] = self.opts.debug
            keys = set(_.keys())
            if 'db' not in keys and 'host' not in keys:
                self.fatal_error("Either 'db' or 'host' is required.")

            if 'ops' not in keys:
                self.fatal_error("You must have an 'ops' clause")

            else:
                _['ops'] = uu.listify(_['ops'])
                    
            if 'host' in keys:
                _.host = self._validate_host(_.host)
                try:
                    _ = uu.deepsloppy(_)
                    is_local = _.host.hostname == 'localhost'
                except:
                    return None

                if is_local:
                    for i, ops_block in enumerate(_.ops):
                        _.ops[i] = self._validate_ops_block(ops_block)

            if 'db' in keys:
                _['db'] = self._validate_db(_['db'])

        return o


    @trap
    def check_roster(self,o:Any) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        roster is a list of the actions, in sequential order.
        """
        # self.errors += __check_type(inspect.stack()[0][3])
        if not isinstance(o, list):
            self.fatal_error(f"{o}. roster must be a list")
    
        return o


    @trap
    def check_cleanup(self, o:Any) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        Cleanup allows the same operations as remote_ops. 
        """
        # Now we get down to doing a little true cleanup. CLEANUP_OPS
        # is in the $grammar file, and it consists mainly of zipping
        # the plain text and elimating the various temporary files that
        # are insurance against mid-integration crashes.
        o.extend(CLEANUP_OPS)
        return self.check_remote_ops(o)


    @trap
    def check_slateupload(self, o:Any) -> object:
        """
        Validate the parameters and supply defaults.
        """
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        new_o = uu.SloppyDict(SLATEUPLOAD_DEFAULTS)
        for k in o: new_o[k] = o[k]
        if not all([new_o[k] for k in SLATEUPLOAD_KEYS]):
            self.fatal_error('A file name and a format id are both required.')

        new_o.file = self._update_filename(new_o.file)
        new_o['local_dir'] = self.home
        if 'debug' not in new_o: new_o['debug'] = self.opts.debug
        
        return new_o

    @trap
    def check_source(self, o:Any) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        Go get files.
        """
        # self.errors += __check_type(inspect.stack()[0][3])

        new_o = []
        for e in o:
            e = self.__assign_debug(e)
            which_endpoint, e = self._validate_endpoint(e)
            metachars = uu.contains_metachars(e.file)

            # We always overwrite, and zip is only when we are creating
            # a file.
            e.overwrite = True
            e.zip = None

            if which_endpoint == 'box':
                e.box = self._validate_box(e.box)
                e.directory_alias = e.get('directory')
                e.directory = self.__box_folder_idstr_from_name(e.directory)
                if os.sep in e.file:
                    self.fatal_error(f"box file {e.file} should not have a folder name in it.")
                e.empty = self._validate_empty(e.empty)
                e.required = self._validate_required_clause(e.required)
                e.wait = self._validate_wait(e.wait)

            elif which_endpoint == 'host':
                password = None if 'password' not in e else e.password
                e.host = self._validate_host(e.host, password)
                e.empty = self._validate_empty(e.empty)
                e.required = self._validate_required_clause(e.required)
                e.wait = self._validate_wait(e.wait)

            elif which_endpoint == 's3':
                if os.sep in e.file:
                    self.fatal_error(f"s3 file {e.file} should not have a directory name in it.")

                e.opts = self._validate_s3(e.s3)
                e.delete = True if e.delete in (None, True) else False
                s3_sub_cmd = 'mv' if e.delete is True else 'cp'
                if metachars:
                    e['ops'] = S3_SOURCE_CMD_MANY.format(
                        s3_sub_cmd, e.opts.name, e.directory,
                        os.environ.get('mydir'), e.file, e.opts.name
                        )
                else:
                    e['ops'] = self._validate_ops_block(
                        S3_SOURCE_CMD_1.format(
                            s3_sub_cmd, e.opts.name, e.directory, 
                            e.file, os.environ.get('mydir'), e.opts.name 
                            )
                        ) 
                e.empty = self._validate_empty(e.empty)

            else:
                self.fatal_error(f"this version of the compiler does not support {which_endpoint}")       
            
            new_o.append(e)

        return new_o


    @trap
    def check_techlead(self, o:object) -> object:
        for i, netid in enumerate(o):
            if self._validate_netid(netid): continue
            self.non_fatal_error(f'netid #{i}, {netid} does not appear to be correct.')


    @trap
    def check_testconnect(self, o:object) -> object:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        
        try:
            new_o = uu.SloppyDict(o)
        except Exception as e:
            self.fatal_error(f"Argument to testconnect must be dict-like, not {o=}")
            
        new_o['local_dir'] = self.home
        if 'on_error' not in new_o: new_o['on_error'] = 'proceed'
        if 'debug' not in new_o: new_o['debug'] = self.opts.debug

        return new_o


    @trap
    def _box_parts(self, boxname:str) -> tuple:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        folder_part, file_part = os.path.split(boxname)
        folder_part = self.__box_folder_idstr_from_name(folder_part)
        return folder_part, file_part


    @trap
    def check_xforms(self, o:Any) -> object:
        """
        Transformations are manipulations that are done to data that are
        retrieved from either a database SELECT or the acquisition of a
        file-like data container.
        """

        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        
        new_o = []
        for i, xform in enumerate(o):
            xform = self.__assign_debug(xform)
            if not self._validate_elements({"input", "output"}, xform): return None

            # We don't require a 'ops' clause because the output could
            # be something as simple as creating a CSV file.
            if 'ops' not in xform: xform['ops'] = {}
            if 'debug' not in xform: xform['debug'] = self.opts.debug

            xform.input  = self._validate_xform_input(xform)
            xform.output = self._validate_xform_output(xform)
            xform.ops    = self._validate_xform_ops(xform)

            xform['local_dir'] = self.home

            new_o.append(xform)

        return new_o


    @trap
    def check_XML(self, o:Any) -> object:   
        """
        this is check for the main XML plugin.
        """
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        new_o = []
        o = uu.listify(o)

        available_grammars = self._valid_xml_grammars()
        if not available_grammars:
            self.fatal_error("No available translation grammars found.")
            return None

        for i, xml_ops in enumerate(o):
            xml_ops = uu.SloppyDict(xml_ops)
            if not self._validate_elements(XML_KEYS, xml_ops): return None
            try:
                xml_ops.grammar = next(_ for _ in available_grammars if xml_ops.grammar in _)
            except StopIteration as e:
                self.fatal_error(f"No grammar found matching {xml_ops.grammar}") 

            xml_ops['local_dir'] = self.home
            new_o.append(xml_ops)
            
        return uu.deepsloppy(new_o)   


    @trap
    def _validate_elements(self, 
            required_elements:set, 
            o:object) -> bool:
        """
        required_elements -- a set/frozenset-like object containing
            items that are required to be present.
        o -- an iterable of some flavor that needs to be checked.

        returns -- True if all required_elements are present and accounted for,
            False otherwise.

        This function also bumps the self.errors count, and sets self.fatal to True.
        """
        
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        
        if not isinstance(required_elements, (set, frozenset)): 
            required_elements = set(required_elements)

        if isinstance(o, dict): 
            found_elements = set(o.keys())
        elif isinstance(o, (list, str, set)):
            found_elements = set(o)
        else:
            found_elements = PHI
            
        missing = required_elements - found_elements
        if missing == PHI: return True

        self.fatal_error(f'missing these required elements: {missing}')
        return False


    @trap
    def _validate_empty(self, empty_clause:uu.SloppyDict) -> uu.SloppyDict:
        if empty_clause is None:
            return EMPTY_DEFAULTS

        # The assumption here is that with the programmer specifying the 
        # info that defines empty, they are likely anticipating non-whitespace
        # characters. 
        if 'whitespace' not in empty_clause: 
            empty_clause['whitespace'] = False

        for element in ('lines', 'bytes'):
            if element not in empty_clause:
                empty_clause[element] = EMPTY_DEFAULTS[element]
            
        return empty_clause


    @trap
    def _validate_endpoint(self, endpoint:object) -> (str, uu.SloppyDict):
        """
        An endpoint is a place we get or deliver files. The rules for
        validating these connections are sufficiently self similar that
        we can use one function to do the job.
        """
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        endpoint = self.__set_defaults(endpoint, ENDPOINT_DEFAULTS)
        
        endpoint.local_dir = self.home
        if endpoint.file is None:
            self.fatal_error('file is required.')
            
        ###
        # As of 26 March 2020, overwrite can be other things
        # than true and false. The additional value is 'replace',
        # which only has meaning with destinations where versioning
        # is done in the file system (Box, Sharefile, etc.).
        ###
        w_mode = endpoint.overwrite
        if isinstance(w_mode, str): w_mode = w_mode.lower()
        try:
            endpoint.overwrite = OVERWRITE_TRANSLATIONS[w_mode]
        except KeyError as endpoint:
            self.fatal_error(f"{w_mode} is not a valid value for overwrite.")

        ###
        # There should be exactly one endpoint.
        ###
        keys = set(endpoint.keys())
        which_endpoint = set(ENDPOINTS) & keys
        if which_endpoint == PHI:
            self.fatal_error(f"Must contain one of {ENDPOINTS}")

        elif len(which_endpoint) != 1:
            self.fatal_error(f"This block contains an extra endpoint, {which_endpoint}")

        else:
            which_endpoint = which_endpoint.pop()
            
        if endpoint.zip is None:
            endpoint.zip = ZIP_OPTS.get(which_endpoint, (None, None))
                                
        return which_endpoint, endpoint

    
    @trap
    def _valid_xml_grammars(self) -> List[str]:
        try:
            grammars = glob.glob(os.path.join(XML_GRAMMAR_LOCATION, '*grammar.py'))
            if not grammars: 
                raise Exception(f"no XML grammars found in {XML_GRAMMAR_LOCATION}")
        except:
            raise Exception('env variable <plugins> not set.')

        return [ os.path.splitext(os.path.basename(_))[0] for _ in grammars ]
        


    @trap
    def _validate_ops_block(self, ops_block:str) -> str:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)

        ###
        # We are examining a command for the localhost's environment 
        # that is something like "command --opts target"
        ###
        try:
            command, therest = ops_block.split(maxsplit=1)
        except ValueError as e:
            command = ops_block
            therest = ""

        ###
        # Check to see if the author of the IJKL has already specified
        # something absolute.
        ###
        if not command.startswith(os.sep):
            ###
            # Anything in the XFORM_OPS transform takes priority over
            # the result of which. If we find nothing, it is an error.
            ###
            command = XFORM_OPS.get(command, shutil.which(command))
            if not command: 
                self.fatal_error(f'Unable to resolve <{command}> operation.')

        ###
        # Expand the environment variables in the arguments at the end.
        ###
        therest = os.path.expandvars(os.path.expanduser(therest))
        command = os.path.expandvars(os.path.expanduser(command))

        ###
        # Put it all back together.
        ###
        ops_block = " ".join((command, therest))
        return ops_block


    @trap
    def _validate_required_clause(self, clause:Union[str, int, list]) -> Iterable:
        """
        Cases:

            None -- use the defaults.
            str  -- various text-based ranges.
            int  -- a particular number of files that is required.
            list -- a given number of files to expect.
        """
        if clause is None: 
            return REQUIRED_DEFAULTS

        # Here is our working copy.
        r_val = uu.SloppyDict()
        r_val['count'] = None

        if isinstance(clause, list): 
            try:
                r_val.count = tuple([int(i) for i in clause])

            except Exception as e:
                self.fatal_error(f"One or more values in {clause} are not integers")
                return None

        elif isinstance(clause, int):
            r_val.count = range(clause, clause+1) 

        elif isinstance(clause, str):
            r_val.count = self.__parse_required_clause(clause)
        
        else:
            self.fatal_error(f"The object {clause} could not be converted.")
            return None

        return r_val


    @trap
    def _validate_xform_input(self, xform:dict) -> dict:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        return self._validate_xform_io(xform, 'input')
        

    @trap
    def _validate_xform_output(self, xform:Union[str,dict]) -> dict:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        if isinstance(xform.output, str) and not xform.output: 
            xform.output=xform.input.name
        return self._validate_xform_io(xform, 'output')
        

    @trap
    def _validate_xform_io(self, xform:uu.SloppyDict, io_name:str) -> dict:
        """
        Validate the input/output clauses of xform
        """
        if self.opts.verbose > 1: print(inspect.stack()[0][3])
        
        # This is one case where we have a specialization of the grammar.
        # Either of these forms is valid:
        #   "input":"/path/filename"
        #   "input":{"name":"/path/filename"}
        # It seems that it is just too easy to forget the subordinate dict 
        # construction.

        element = xform.get(io_name, None)
        if isinstance(element, str): 
            element_name = element 
            element=uu.SloppyDict()
            element['name'] = os.path.expandvars(os.path.expanduser(element_name))
            element.name = self._update_filename(element.name)
            element['type'] = 'txt'
            if io_name == 'input':
                return uu.deepsloppy(element)

        # This is the output. We have to pay a little more attention because there
        # are more output options than input options.
        element['type'] = element.get('type', 'txt')
        element['format'] = element.get('format', {})
        element['name'] = os.path.expandvars(os.path.expanduser(element.get('name', xform.input.name)))
        element['name'] = self._update_filename(element.name)

        if element.type != 'txt':
            element = uu.deepsloppy(element)
            try:
                foo = getattr(self, '_validate_xform_'+element.type)
                element.format = foo(element.format, xform.input.name, xform.output.name)
            except Exception as e:
                self.non_fatal_error(f"Unknown element.type {element.type}. Message {e}")
            
        return element

    @trap
    def _validate_xform_ops(self, xform:uu.SloppyDict) -> List[list]:
        if self.opts.verbose > 1: print(inspect.stack()[0][3])
        newops = uu.deepsloppy(uu.listify(xform.get('ops')))

        name_clause = None
        for i, step in enumerate(newops):
            if not isinstance(step, dict):
                self.fatal_error(f"cannot process xform-ops {step}")
                return newops

            k, v = step.popitem()
            try:
                resolved_k = XFORM_OPS[k]
            except Exception as e:
                self.fatal_error(f"Unknown transform {k}")
                return newops
                
            # Encryption requires some special attention because
            # it is customized to the UR keys and environment.

            name_clause = None
            try:
                if k == 'gpg': v, name_clause = self.__gpg_parse(v)
            except Exception as e:
                uu.tombstone(uu.type_and_text(e))
                break
            else: 
                newops[i] = {resolved_k: v}

        if name_clause:
            newops.append({ XFORM_OPS['rename']:
                " -o {}/*.asc {}/#1.{} ".format(self.home, self.home, name_clause)})

        return uu.deepsloppy(newops)


    @trap
    def _validate_xform_csv(self, format:dict, 
            input_name:str="", 
            output_name:str="") -> dict:
        if self.opts.verbose > 1: print(inspect.stack()[0][3])

        format = uu.deepsloppy(format)
        format = self.__set_defaults(format, XFORM_CSV_DEFAULTS)

        # To cleanly support tab delimited designations, if the
        # sep is an integer (like 9 in the case of the tab) here
        # is where we convert from the 9 to the tab char. If it is
        # not an integer, we leave it alone. 
        try:
            format.sep = int(format.sep)
        except:
            pass

        # Similarly, we write 1 for single quotes, 0 for none,
        # 2 for double, and 3 for backquote. Here is where we
        # convert.
        try:
            format.quote = XFORM_CSV_QUOTES[format.quote]
            if not format.quote: format.qforce = csv.QUOTE_NONE 
        except:
            pass

        format.fixed_header = uu.expandall(format.fixed_header)
        if format.fixed_header and not fname.Fname(format.fixed_header):
            self.non_fatal_error(f'Unable to find fixed header file {format.fixed_header}')

        format.footer = uu.expandall(format.footer)
        if format.footer and not fname.Fname(format.footer):
            self.non_fatal_error(f'Unable to find footer file {format.footer}')

        return format
            

    @trap
    def _validate_xform_xml(self, format:dict, 
            input_name:str, 
            output_name:str) -> dict:
        if self.opts.verbose > 1: print(inspect.stack()[0][3])

        format = self.__set_defaults(format, XFORM_XML_DEFAULTS)

        format.frame = input_name
        format.output = output_name

        # Determine if the remap is a filename or the data 
        # structure itself.
        if isinstance(format.remap, str):
            verb = ''
            try:
                f = open(uu.expandall(format.remap))
                format.remap = json.loads(f.read())
            except FileNotFoundError as e:
                verb = 'found'
            except PermissionError as e:
                verb = 'opened'
            except json.decoder.JSONDecodeError as e:
                verb = 'parsed'
            
            if verb:
                self.fatal_error(f'remap file {format.remap} could not be {verb}.')
                
        return format

        

    ###### 
    # This section contains _helper functions of various types.
    ######

    @trap
    def _update_filename(self, f:str) -> str:
        """
        Deal with absolute vs. contextual file names. A file name like
        x.y is construed to be /path/to/recipe/files/x.y, whereas a file
        like /x.y is left unchanged. 
        """
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        return f if f.startswith(os.sep) else os.path.join(self.home, f)


    @trap
    def _validate_netid(self, netid:str) -> bool:
        """
        Should we change the nature of a netid, this function must be
        updated. 
        """
        return ( len(netid) < 9 and 
                 netid.lower().isalnum() and 
                 netid[0:2].isalpha() )


    @trap
    def _validate_box(self, box:str) -> dict:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        NOTE: At this time, we only have one 'box', so the calling parameter
        is ignored. That may change.
        
        returns -- a dict with the required information about the box repo.
        """        
        try:
            box_info = self.g.box
        except Exception as e:
            self.fatal_error('Unable to get box info.')
            return {}

        if BOX_KEYS - set(box_info.keys()):
            self.fatal_error(f'Valid box interface must contain all of these keys: {BOX_KEYS}')
            return None

        return { _:box_info[_] for _ in box_info if _ in BOX_KEYS }


    @trap
    def _validate_curl(self, curl_name:str) -> dict:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        We cannot completely validate a curl definition because
        there are so many ways it can work. But we can proofread it
        a little bit.
        """
        
        curl_info = None
        try:
            curl_info = uu.SloppyDict(getattr(self.g, curl_name, None))
        except:
            self.fatal_error(f'nothing named {curl_name} found')
            return curl_info

        for k in CURL_KEYS:
            if k not in curl_info:
                self.fatal_error(f'global object named {curl_name} exists, but it is not a curl definition.')
                return curl_info

        if curl_info.type not in CURL_TYPES:
            self.fatal_error(f'Unknown curl type {curl_info.type}')

        if curl_info.type == 'SFTP':
            host_info = uu.get_ssh_host_info(curl_info.host)
            curl_info.host = host_info.hostname
            curl_info['user'] = host_info.user if not curl_info.get('user') else curl_info.user
            curl_info['port'] = host_info.port if not curl_info.get('port') else curl_info['port']
            curl_info['key'] = host_info.get('identityfile')
            if curl_info.key is None and curl_info.get('password') is None:
                self.fatal_error(f"Cannot connect to {curl_name} without a key or password.")

            # NOTE: ssh (v. curl) always presents a list of keys, even if there is only one key
            # in the list. curl only works with one key at a time, so we go with the first one
            # and hope it works.
            curl_info.key = curl_info.key[0]
            
        return curl_info       
        

    @trap
    def _validate_db(self, name:str) -> dict:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        name -- name of something supposed to be a database.

        returns -- connection info or None.
        """
        info = getattr(self.g, name, None)
        if info is None: 
            self.fatal_error(f'nothing named {name} found')
            return name

        for k in DB_OBJECT_KEYS:
            if k not in info:
                self.fatal_error(f'global object named {name} exists, but it is not a DB')
                return name
        
        # info['host'] = self._validate_host(info['host']).hostname

        try:
            x_item = self.db.get_credentials_by_name(0, name, 'password')

        except Exception as e:
            self.non_fatal_error(f'No known password for {name}.')
            return info

        try:
            info['password'] = info['password']
        except Exception as e:
            self.fatal_error(uu.type_and_text(e))
            return None

        return info        
        

    @trap
    def _validate_host(self, name:str, password:str=None) -> dict:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        name -- name of some host, possibly known to Canøe.

        returns -- dict of scrubbed info, or the bare name if no info found.
        """

        info = uu.get_ssh_host_info(name, self.opts.ssh_config)
        if info is None:
            self.fatal_error(f"No details for host {name}")
        if password is not None: 
            info['password'] = password

        return name if info is None else uu.SloppyDict({ x:info[x] for x in info if x in HOST_KEYS })
        

    @trap
    def _validate_local_directory(self, s:str) -> bool:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        NOTE: there is no way to validate remote directories without 
        connecting. Yes?
        """
        s = uu.expandall(s)
        if not os.path.isdir(s):
            self.non_fatal_error(f"{s} not found; creating it.")
            uu.make_dir_or_die(s, mode=0o770)

        return s


    @trap
    def _validate_on_error(self, s:str) -> int:
        """
        Check to see if it is in the list of allowed values.
        """
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        try:
            return ERROR_ACTION.by_name(s)
        except Exception as e:
            self.fatal_error(f'unknown on_error action {s}')
            return None


    @trap
    def _validate_s3_old(self, s3_name:Any) -> dict:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        "s3":"richmonddatatransfer",
                ^^^^^^ 
                    \- name of the s3 bucket.
                    /- desc of the bucket
                vvvvvv
        {
        'persistence_seconds': 660, 
        'comment': 'This is the StarRez bucket', 
        'aws_access_key_id': '###', 
        'aws_secret_access_key': '###'
        }
        
        returns -- a dict of the resolved info or None.
        """
        info = {}
        info['name'] = s3_name
        bucket_desc = getattr(self.g, s3_name)
        for k in bucket_desc: 
            info[k] = bucket_desc[k]

        try:
            _, creds = self.db.get_credentials_by_name(
                0, info['name'], ''
                ).popitem()
        except Exception as e:
            _, creds = None, None

        if creds is not None:
            for k in info:
                if info[k] == self.g.sys_params['CREDENTIAL']:
                    try:
                        info[k] = creds[k].decode()

                    except Exception as e:
                        uu.tombstone(uu.type_and_text(e))
                        uu.tombstone('credential {} cannot be found or decrypted.'.format(k))

        return info


    @trap
    def _validate_s3_revised(self, s3_name:Any) -> dict:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        Validate the bucket name against the AWS credentials file.
        """

        aws_creds = configparser.ConfigParser()
        aws_creds.read(os.environ.get('AWS_SHARED_CREDENTIALS_FILE'))
        if s3_name not in aws_creds:
            self.fatal_error(f"No credentials found for {s3_name}")

        for k in S3_KEYS:
            if k not in aws_creds[s3_name]:
                self.fatal_error(f"No {k} found for {s3_name}")

        aws_creds[s3_name]['name'] = s3_name
        return uu.SloppyDict(aws_creds[s3_name])


    _validate_s3 = _validate_s3_revised


    @trap
    def _validate_sharefile(self, sharefile_name:str) -> dict:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        Translate and validate the Sharefile definition.
        """
        info = {}
        info['name'] = sharefile_name
        sharefile_desc = getattr(self.g, sharefile_name)
        for k in SHAREFILE_KEYS:
            info[k] = sharefile_desc.get(k, None)

        if not all([_ for _ in info]):
            self.fatal_error(f"A sharefile endpoint requires all of these keys to be present {SHAREFILE_KEYS}")

        return uu.SloppyDict(info)


    @trap
    def _validate_wait(self, clause:uu.SloppyDict) -> uu.SloppyDict:
        
        if clause is None:
            return WAIT_DEFAULTS

        r_val = uu.SloppyDict(dict.fromkeys(WAIT_KEYS))

        try:
            r_val.time = int(clause.time)
        except Exception as e:
            self.fatal_error("wait clause must contain an integer value for 'time'")
            return None

        if 'until' in clause and 'use' in clause:
            self.fatal_error("You must specify exactly one of 'use' and 'until'")
            return None

        if 'until' in clause:
            hours = clause.until // 100
            minutes = clause.until % 100
            if ( 0 <= hours <= 23 and 0 <= minutes <= 59 ):
                r_val.until = 60*hours + minutes
                r_val.use = 0
                return r_val

            else:
                self.fatal_error(f"Invalid time spec {clause.until}")
                return None
                
        else:
            try:
                r_val.use = int(clause.use)
                r_val.until = -1
                return r_val

            except Exception as e:
                self.fatal_error(f"'use' must be an integer, not {clause.use}")
                return None
                

    """ ********************************************
                        __helpers
    ******************************************** """

    @trap
    def __assign_debug(self, o:object) -> object:
        if self.opts.verbose: print(inspect.stack()[0][3])
        """
        Assign a debug value to each pair of opcode and argument.
        """
        if not isinstance(o, dict): return o
        if 'debug' not in o: o['debug'] = self.current_recipe['debug']
        return o


    @trap
    def __box_folder_id_from_name(self, folder_name:str) -> int:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        returns a rather long int if the folder name is known to the box 
        config. Check for its being an int already to guard against this
        function being called twice on the same value during a recursive
        descent.
        """
        try:
            folder_number = int(folder_name)
        except:
            pass
        else:
            return folder_number
        
        try:
            return int(self.g.box['folder-names'][folder_name])
        except KeyError as e:
            self.fatal_error(f'No known Box folder named {folder_name}.')
        return 0


    @trap
    def __box_folder_idstr_from_name(self, folder_name:str) -> str:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        return str(self.__box_folder_id_from_name(folder_name))


    @trap
    def __bucket_from_name(self, bucket_name:str) -> dict:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        Whether sending or receiving, let's do this bucket stuff 
        correctly. Based on the name, go get the information about
        the bucket.
        """
        bucket_info = None
        try:
            bucket_info = self.g[bucket_name] 
        except:
            self.fatal_error(f"no info about bucket {bucket_name}")

        return bucket_info


    @trap
    def __gpg_parse(self, data:object) -> str:
        this_fcn = inspect.stack()[0][3]
        if self.opts.verbose > 1: print(this_fcn)
        """
        3 July 2019: there is currently no way to change the default file
        extension for encrypted archives from .asc. Consequently, this is a kludge
        to allow specifying it when we need to. The most usual case is to
        coerce it to .gpg.

        "gpg":"vendor1" -> will produce a .gpg styled archive as output.

        "gpg":["vendor1", "pgp"] -> will produce a .pgp styled archive.

        "gpg":"vendor1 vendor2" -> will produce a .gpg archive readable by both
            recipients.

        """
        if data == 'decrypt': return " --yes --batch ", ""

        if isinstance(data, str): 
            recipients, ext = [data, GPG_EXT]
        else:
            recipients, ext = data[0], data[1] 

        recipients = recipients.strip().replace(" ",",").split(',')
        recipients = list(set([_ for _ in recipients if _])) 

        # Determine the recipient from the partial data.
        recipients = [ self.___resolve_recipient(_) for _ in recipients]
        return " ".join(GPG_OPTIONS + recipients), ext  


    @trap
    def __parse_required_clause(self, s:str) -> Iterable:
        parts = s.partition('-')
        lower, sep, upper = ( _.strip() for _ in parts )
        full_spec = numpy.prod(tuple(len(_) for _ in parts)) > 0

        if full_spec:
            try:
                lower = int(lower)
                upper = int(upper)+1
                return range(lower, upper)

            except Exception as e:
                self.fatal_error("correct format for a range is N-M, where N and M are integers.")
                return s

        try:
            lower = int(lower)
        except:
            lower = 0

        try:
            upper = int(upper)
        except:
            upper = sys.maxsize

        return range(lower, upper)


    @trap
    def ___resolve_recipient(self, shred:str) -> str:
        if self.opts.verbose > 1: print(inspect.stack()[0][3])
        """
        To do encryption, we need the key fingerprints. If the user has
        provided something else .. an account name? .. we must look up
        the fingerprint.
        """
        global gpg_obj
        s = ""

        all_recipients = filter_keys(gpg_obj, 
            min_len=GPG_MIN_LEN, 
            max_age=GPG_MAX_YEARS,
            grace_period=GPG_GRACE_PERIOD) 

        for _ in all_recipients:
            if shred == _.keyid or shred in _.uids.lower(): 
                s += f" --recipient {_.keyid} "
                if _.short: 
                    self.non_fatal_error(GPG_MSG_SHORTKEY.format(shred))
                if _.old: 
                    self.non_fatal_error(GPG_MSG_OLDKEY.format(shred))
                if _.expiring_soon: 
                    self.non_fatal_error(GPG_MSG_GETTINGOLD.format(shred))

        if not s: self.fatal_error(f"Unknown gpg recipient {shred}")
        return s


    @trap
    def __set_defaults(self, o:dict, defaults:dict) -> uu.SloppyDict:
        """
        Set default keys and values for any keys not explicitly present
        in o. Return the original object with the new key-value pairs
        as a SloppyDict.

        o - The thing we are modifying.
        defaults - a collection of key-value pairs

        returns o, as a SloppyDict
        """
        for k in defaults.keys():
            o[k] = o.get(k, defaults[k])
        if o.get('debug') is None: o['debug'] = self.opts.debug

        return uu.deepsloppy(o)


def diagnostics() -> int:
    """
    Canøe19 is a fairly complex environment. Make sure that all the pieces
    are present.

    returns -- a member of the os.EX_* family of constants. EX_OK if all 
        is well.
    """
    r_val = os.EX_OK

    print("Checking PYTHONPATH")
    pypath = os.environ.get('PYTHONPATH').split(':')
    for segment in pypath:
        if not os.path.isdir(segment):
            print(uu.blind("{} does not appear to be a directory.".format(segment)))
            r_val = os.EX_CONFIG

    print("Checking the PATH")
    if ':/sw/canoe/bin:' not in os.environ.get('PATH'):
        print(uu.blind("/sw/canoe/bin not found in $PATH"))
        r_val = os.EX_CONFIG


    print("Checking other environment variables.")
    canoe_vars = ('cconfig', 'compiled', 'CANOE_HOME', 
        'CANOE_LOG', 'plugins', 'recipes', 
        'AWS_SHARED_CREDENTIALS_FILE')

    for _ in canoe_vars:
        it = os.environ.get(_)
        if it is None:
            print(uu.blind("Environment var {} is not set.".format(_)))
            r_val = os.EX_CONFIG
            
        elif not os.path.exists(it):
            print(uu.blind("Environment var {} is set to {}, which does not exist.".format(_, it)))
            r_val = os.EX_CONFIG


    location = os.environ.get('plugins')
    if not location:
        print("{} is not set.".format(location))
        r_val = os.EX_CONFIG
    else:
        print("Searching for plugins in {}".format(location))
        for plugin in ACTIONS:
            source_code = os.path.join(location, plugin + '.py')
            if not os.path.isfile(source_code):
                print(uu.blind("Cannot find {}".format(source_code)))
                r_val = os.EX_CONFIG

    print("Checking XFORMS")
    for k, v in XFORM_OPS.items():
        v1 = v.split()[0]
        if not v:
            print(uu.blind("XFORMS_OPS item {} is not mapped to anything.".format(k)))
            r_val = os.EX_CONFIG

        elif v and k != v:
            if not os.path.isfile(v1):
                print(uu.blind("{}, associated with {} not found.".format(v1, k)))
                r_val = os.EX_UNAVAILABLE
                continue

        elif os.access(v1, os.X_OK):
                print(uu.blind("{} is not executable by {}".format(v1, os.getuid())))
                r_val = os.EX_NOPERM
                continue

    if r_val == os.EX_OK: 
        print("All executables, plugins, and environment variables are present.")

    return r_val


def compiler_main() -> int:
    """
    Compile IJKL to executable code.
    """
    SOURCE         = os.environ.get('SOURCE',           "/sw/canoe/recipesourcecode")
    COMPILERCONFIG = os.environ.get('COMPILERCONFIG',   "/sw/canoe/compilerconfig")
    COMPILEROUTPUT = os.environ.get('COMPILEROUTPUT',   "/sw/canoe/compiledrecipes20")
    HEADER_KEYS    = ['name', 'comment', 'schedule', 'owner', 'compiled_time', 'compiler_info']

    p = argparse.ArgumentParser(description='Compile IJKL to executable code.')

    p.add_argument('--all', action='store_true', 
        help='compile all IJKL in the working directory')

    p.add_argument('-c', '--config', default=COMPILERCONFIG, 
        help='Specify a non-default location for the config files.')

    p.add_argument('--debug', default=False, action='store_true',
        help='Value of the debug variable to be placed in the object code.')

    p.add_argument('--ext', type=str, default='.json', 
        help='file ext for input files.')

    p.add_argument('filenames', type=str, action='append', nargs='?',
        help='The name of the file[s] to compile.')

    p.add_argument('-i', '--source', default=SOURCE,
        help='Specify a non-default location for the source to compile.')

    p.add_argument('--no-diag', action='store_true', 
        help='do NOT create a decompiled version of the fully parsed input.')

    p.add_argument('-o', '--output', default=COMPILEROUTPUT,
        help='Specify a non-default location for the compiled code.')

    p.add_argument('-O', '--opt', type=str, default='size',
        help='Optimize. Currently the only option is size, and it is the default.')

    p.add_argument('-p', '--prod', action='store_true',
        help='Use the production host definitions when compiling.')

    p.add_argument('-q', '--quiet', action='store_true',
        help='only report problems, and the number of items compiled. NOTE: quiet is higher priority than verbose.')

    p.add_argument('-S', '--ssh-config', type=str, default=None,
        help="the name of an alternate SSH config file to use.")

    p.add_argument('-v', '--verbose', action='count', 
        help='be chatty. add more v-s for more loquacious output.')

    p.add_argument('-x', type=str, default='.jsc',
        help='file ext for compiled files.')

    p.add_argument('--check-config', action='store_true',
        help='run a self check of the grammar, plugins, and environment.')

    print("\n\n")
    quick_check = diagnostics()
    if quick_check != os.EX_OK: sys.exit(quick_check)
 
    opts = p.parse_args()
    if opts.check_config and diagnostics() != os.EX_OK: 
        sys.exit(os.EX_CONFIG)

    opts.debug = True if opts.debug else False
    opts.source = uu.expandall(opts.source)
    opts.config = uu.expandall(opts.config)
    opts.output = uu.expandall(opts.output)
    if opts.quiet or opts.verbose is None: opts.verbose = 0

    if not opts.quiet:
        print(f'CANOE 20 IJKL compiler version {compiler_mod_time} based on commit ID {this_commit}')

        print('options in use:\n')
        print(f"compile {uu.args_to_str(opts)}")

    if not all(opts.filenames) and not opts.all:
        print('ERROR: You must either specify a file to compile, or use the --all option.')
        return os.EX_NOINPUT

    jreader = jp.JSONReader()       # To read the source code.
    recipe = Recipe()               # So that this var is init-ed.
    compiled_recipes = {}           # Temp storage to look for conflicts.
    failures = []                   # List of recipes that failed to compile.
    supersedures = []               # List of recipes hiding other recipes.
    file_list = []                  # The list of fqn-s of source files.
    compiled_file_list = []         # Where they wind up.
    diagnostic_file_list = []       # Where any diagnostic files might be found.

    stats = canoestats.CanoeStats(
        os.path.join(os.environ.get('CANOE_HOME', '.'), 'canoestats.db')
        )

    compiler = RecipeCompiler(opts)

    if opts.all: 
        opts.filenames = glob.glob(os.path.join(opts.source, "*.json"))
        file_list.extend(opts.filenames)
    else:
        for e in opts.filenames:
            file_list.extend(glob.glob(os.path.join(opts.source, e)))

    print("list of files to compile: {}".format(file_list))

    mypid = os.getpid()
    process_bytes_used = uu.mymem()

    for i, f in enumerate(file_list, start=1):
        packer = urpacker.URpacker()
        if not f.startswith(os.sep): f = os.path.join(opts.source, f)
        f = uu.expandall(f)

        errors = 1
        warnings = 0

        try:
            parser = IJKLparser(opts.debug)
            opts.debug and print(f"parser built")
            s = parser.attachIO(f).parse()
            if s is not None:
                recipe, errors, warnings = compiler.compile(s, f)
            else:
                uu.tombstone("No source code found in {}".format(f))

        except Exception as e:
            uu.tombstone(str(e))

        summary = f"{errors} errors and {warnings} warnings."
        if errors or warnings:
            print(uu.blind(summary))
        else:
            print(summary)

        if errors:
            print(uu.blind(f"Compilation of {f} failed."))
        elif warnings:
            print(uu.blind(f"Compilation of {f} succeeded with warnings.")) 
        else:
            print(f"Compilation of {recipe.name} SUCCEEDED.")
            stats.new_integration(recipe.name, recipe.frequency)

        # For informational purposes, we need to keep track of one recipe hiding another one.
        try:
            _ = compiled_recipes[recipe.name]
            recipe['supersedes'] = _.origin
            supersedures.append((recipe.origin, _.origin))

        except KeyError as e:
            # The recipe compiled, and this is the first time we have seen it.
            compiled_recipes[recipe.name] = recipe
            recipe['supersedes'] = None

        except AttributeError as e:
            # recipe was None.
            failures.append(f)
            continue

        finally:
            recipe = uu.deepsloppy(recipe)

        outputfile = uu.expandall(
            os.path.join(COMPILEROUTPUT, fname.Fname(f).fname_only + '.jsc')
            )
        packer.attachIO(outputfile, s_mode='write')
        packer.write(recipe, show_stats=True, object_code=uu.canoe_version())
        compiled_file_list.append(outputfile + '.jsc')

        if not opts.no_diag:
            diag_file = outputfile + '.diagnostic.json'
            with open(diag_file, "w") as f:
                printable=recipe.reorder(HEADER_KEYS + recipe.roster)
                pprint.pprint(printable, stream=f, indent=4, width=100, compact=False, sort_dicts=False)
            diagnostic_file_list.append(diag_file)

    if len(compiled_recipes) and not opts.quiet:
        print("\ncompiled {} recipes.\n".format(len(compiled_recipes)))
        print(sorted(list(compiled_recipes.keys())))

        print("\nCompiled files\n" + 60*"-")
        print("\n".join(sorted(compiled_file_list)))

        if not opts.no_diag:
            print("\nDiagnostic files\n" + 60*"-")
            print("\n".join(sorted(diagnostic_file_list)))
    
    
    if len(failures) and not opts.quiet:
        print("\n{} recipes failed to compile. And they came from these files:\n".format(len(failures)))
        print(60*"-")
        print("\n".join(sorted(failures)))

    if len(supersedures) and not opts.quiet:
        print("\n{} recipes superseded other recipes.\n".format(len(supersedures)))
        for tup in supersedures:
            print("{} superseded {}".format(tup[0], tup[1]))

    if not opts.quiet:
        print("\n\n{} bytes of memory used compiling recipes.".format(uu.mymem()-process_bytes_used))

    return os.EX_OK if not len(failures) else os.EX_DATAERR


if __name__ == '__main__':
    sys.exit(compiler_main())
else:
    pass

