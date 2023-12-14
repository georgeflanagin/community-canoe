# -*- coding: utf-8 -*-

# Added for Python 3.5+
import typing
from typing import *

#pylint: disable=anamolous-backslash-in-string

import datetime
import gnupg
import os
import shlex
import subprocess
import time

import fname
import urobject as uo
import tombstone as tomb
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


def get_canoe_home() -> str:
    """ 
    Reliably determines the value of $CANOE_HOME 
    """

    try:
        home = os.environ['CANOE_HOME']
    except KeyError:
        home = os.getcwd()
    return home


def normalize_file_fragment(s: str) -> str:
    """
    s : a fragementary file name. Suppose you get handed a name like
        "bob/is/your/uncle". Clearly, "bob" is a directory of some ilk,
        and rather than using the value of $PWD (which will not make much
        sense for a daemon or a process with no shell access), we sub 
        $CANOE_HOME/bob/is/your/uncle 

        Of course, if you really meant for "bob" to be at the top of 
        the food chain, then you can solve it all by starting with 
        os.sep (a.k.a., "/" on Linux).
    """
    return (s 
            if s.startswith(os.sep) 
            else (os.sep).join([get_canoe_home(), s]))


def dict_of_schedules(cfg:object) -> dict:
    """
    This little bit of code creates/returns a somewhat complicated
    data structure. The outermost element is a dict where the keys
    are the names of the recipes. The values are lists of lists of
    sets. Huh?

    the sets are valid matching times/dates/months/daysofweek ... whatever
    a list consists of one set each for each date element.
    and the list of lists is because each recipe may have more than one
    schedule.
    """
    never = '@adhoc'
    todo_list = {
        _ : uu.parse_schedules(cfg[_].get('schedule',never)) 
        for _ in cfg.get_recipe_names() 
        }
    return todo_list


def whats_next(todo_list: object, t:float=None) -> list:
    """
    For a given time t, return the list of recipes that should be run
    at that time. The time does not have to be in the future ...
    """
    up_next = []
    t = t if t is not None else time.time()

    t_stamp = datetime.datetime.fromtimestamp(t)
    for recipe in sorted(todo_list.keys()):
        for _ in todo_list[recipe]:
            if uu.time_match(t_stamp, _):
                up_next.append(recipe)
                break;
    return up_next


class CanoeGPG(uo.URObject):
    """ A simplifying wrapper around GPG. After all, Canoe doesn't do much with
    GPG if you look at the thousands of options it has. """

    decryption_pass = None

    def __init__(self, g:object, verbose:bool=False):
        uo.URObject.__init__(self, g)
        # self.verbose = 8 if verbose else 0
        self.verbose = False
        try:
            exe = g.sys_params.gpg
        except Exception as e:
            raise Exception('Encryption not possible with out a definition of gpg in globals file.')

        gpg_home = os.path.expanduser('~') + os.sep + ".gnupg"

        self._gpg = gnupg.GPG(binary=exe, 
                homedir=gpg_home, 
                verbose=self.verbose)
        self._gpg.encoding = 'utf-8'


    def decrypt(self, filename:str, outfile:str=None) -> bool:
        """
        Note the assumption that this is for a file sent to Canøe.
        """
        f_name = fname.Fname(filename)
        if not f_name: 
            tomb.tombstone("cannot find " + str(f_name))
            return False
        
        try:
            return not subprocess.check_output(
                shlex.split('/usr/bin/gpg --yes --batch --quiet ' + str(f_name))
                )
        except ProcessLookupError as e:
            tomb.tombstone('gpg disappeared. problem noted and forgiven.')
        except Exception as e:
            raise Exception('Decryption of ' + str(f_name) + ' failed. ' + str(e))


    def encrypt(self, filename:str, 
                    recipients:str, 
                    return_output:bool=True,
                    outfile:str='') -> bool:
        """ 
        The workhorse. 

        filename -- The name of the file-like-object you want to encrypt.
        recipients -- a string containing list of fragments of the key-owners
            known to GPG. The function accepts a list, and will string-ify it
            for you.
        return_output -- if present, returns a buffer with the ASCII-armored
            encrypted data.
        outfile -- if present, the data are written to this file.

        Examples:
        
            [1] Return the data, and write a file.
            data = o.encrypt('abc.xyz', ['dbroome','lparker'], True, 'abc.gpg')

            [2] Return the data, write no file.
            data = o.encrypt('abc.xyz', ['dbroome','lparker'], True)

            [3] Skip the return, just write the file.
            o.encrypt('abc.xyz', ['dbroome','lparker'], False, 'abc.gpg')

            [4] Skip the return and no write. This is the ultimate in security
                as all it does is test that the encryption works.
            o.encrypt('abc.xyz', ['dbroome', 'lparker'], False, None)
             .... now what do you do? ....
        """

        gpg_cmd = '/usr/bin/gpg --sign -u 0x21B90C99 -r 21B90C99 \
--encrypt --armor --yes --batch --quiet --trust-model always '

        # Let's see if we can find the file....
        f = fname.Fname(filename)
        if not f: raise Exception("Cannot find file named " + str(f))
        
        # If outfile is not supplied
        if outfile == '': 
            outfile = str(f) + '.gpg'
            embedded_name = str(f)
        else:
            embedded_name = fname.Fname(outfile).all_but_ext

        gpg_cmd += " --output " + str(outfile)
        gpg_cmd += " --set-filename " + embedded_name + " " 

        # Always add ourselves to the recipient list so that we can back out
        # the encryption if we need to. The following code only works for the
        # case where canoe@richmond.edu is a unique recipient, and .. well ..
        # it probably is.
        recipients = recipients.strip().replace(" ",",").split(',')
        if 'canoe' not in recipients: recipients.append('canoe')

        # strip out the directives.
        directives = [ _ for _ in recipients if '=' in _ ]
        recipients = list(set(recipients) - set(directives))
        print(recipients)

        # squeeze out the duplicates in a very pythonic way. This case also takes care
        # of typographic accidents like "canoe,,symplicity" that would expand to
        # ["canoe", "", "symplicity"]
        recipient_list = list(set([ self.resolve_recipient(_) for _ in recipients if _ ]))
        
        if len(recipient_list) < len(recipients):
            raise Exception("One or more duplicate, nonsense, or ambiguous recipients in ",
                str(recipients))
        
        for recipient in recipient_list:
            gpg_cmd += " --recipient " + recipient
        # print(gpg_cmd)


        for _ in directives:
            lhs, rhs = _.split('=')
            gpg_cmd += " " + lhs + " " + rhs

        # print(gpg_cmd)
        gpg_cmd += " " + str(f)
        print(gpg_cmd)

        try:
            result = subprocess.check_output(shlex.split(gpg_cmd))
        except ProcessLookupError as e:
            tomb.tombstone('gpg disappeared. problem noted and forgiven.')

        print("result = " + str(result))

        tomb.tombstone("Encrypted data in " + outfile)
            
        return str(f)+".gpg" if not outfile else outfile


    def resolve_recipient(self, shred:str) -> str:
        """
        To do the encryption, we must use the key fingerprints. If the
        user has passed in something else ... an account name? ... we 
        need to find the key fingerprint.
        """        
        if not shred: return shred
        shred = shred.strip().lower()
        tomb.tombstone("finding encryption key for recipient " + str(shred))

        key_data = self.list_possible_recipients()
        for _ in key_data:
            # Note that we don't want the resolution of the recipient
            # to fail if the fingerprint was given.
            if shred in str(_['uids']).lower(): return str(_['keyid'])
            if shred == str(_['keyid']): return shred

        # Retuning the empty string is harmless.
        print(str(shred) + " not found")
        return ''             


    def list_possible_recipients(self):
        return self._gpg.list_keys()


if __name__ == "__main__":

    import canoeconfig
    g = canoeconfig.CanoeConfig()
    g.get_all_configs()
    # True ... for verbose mode
    o = CanoeGPG(g, True)

    if len(sys.argv) < 2:
        tomb.tombstone("Usage: canoelib.py infile outfile ...")
        exit(0)
    
    if len(sys.argv) < 3:
        infile = sys.argv[1]
        tomb.tombstone(uu.fcn_signature("o.decrypt", infile))
        o.decrypt(infile)
        exit(0)

    plain_text_file = fname.Fname(sys.argv[1])
    for_whom = sys.argv[2:]

    result = o.encrypt(str(plain_text_file), 
        for_whom, 
        True, 
        str(plain_text_file) + ".gpg")
    # print(result)

else:
    # print(str(os.path.abspath(__file__)) + " compiled.")
    print("*", end="")
