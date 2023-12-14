#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from typing import *

""" 

"""

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2020, University of Richmond'
__credits__ = None
__version__ = '0.4'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Working Prototype'
__required_version__ = 3.6

__license__ = 'MIT'
import license


# These are system packages, and there are no python environments
# that do not have them present.
import math
import os
import shlex
import signal
import subprocess
import sys

import gnupg
import shutil

import hop
import jparse
import urbox 
import tombstone as tomb
from   urdecorators import show_exceptions_and_frames as trap
import urutils as uu
# OK, let's see if we can start up.

box_info = uu.SloppyDict(
    next(iter(
        jparse.JSONReader().attach_IO(os.environ['box']).convert().values()
        ))
    )


box_folders = box_info['folder-names']

box_connector = urbox.URBoxHOP(box_info)

banner = """
    Canøe's ad hoc file delivery service. You will be prompted for the
    following six facts:

    [1] The name of the file as it appears in the BOX folder. Note: we 
        do not have a way to check for the right spelling of the name until
        we try to retrieve it!! So, if your spelling is not too good, you
        might have to make more than one attempt.

    [2] The name of the BOX folder as the Canøe system understands it. You
        can type "?" in response, and Canøe will print a list of the known-to-it
        folders.

    [3] The name of the server where the file is headed. These names are
        the ones that appear in ~/.ssh/config. Again, if you type "?" in 
        response, Canøe will print a list of the known server names.

    [4] Directory at the destination where the file is to be put. The 
        default is the home directory at the connection end.

    [5] The name of the encryption key. The default value is the name
        of the destination because they often agree. If you want to 
        send a file unencrypted, you must type NONE, and yes, it is
        case sensitive. If you type "?" in response, Canøe will print
        a list of the keys it knows about. **Any UNIQUE part of the
        email address associated with the key is enough to allow Canøe
        to choose the right key.**

    [6] The extension to be added to the filename for the encrypted copy.
        Usually this is ".pgp", and sometimes ".gpg", but it can be
        anything.

"""

confirmation_text = """

Let's make sure we have this correct.

You want to send {}
From BOX folder {}
To {}
Encrypted for {}

"""

encryption_exe = shutil.which('gpg')

encryption_command = f"{encryption_exe} --encrypt --armor --yes -u 21B90C99 --trust-model always --batch --quiet --force-mdc --recipient C5A7C17D21B90C99" 

gpg_obj = gnupg.GPG(binary=encryption_exe, homedir=os.path.join(uu.expandall('~'), '.gnupg'))
all_recipients = gpg_obj.list_keys()

@trap
def get_boxfolder() -> tuple:
    """
    Prompt for a boxfolder name, and return the name and id.
    """
    global box_folders

    folderid = None
    while folderid is None:
        boxfolder = input('Name of the BOX folder containing the file? ')
        if boxfolder == '?':
            tabular_print(sorted(list(box_folders.keys())))
            continue

        folderid = box_folders.get(boxfolder)
        if folderid is None: 
            print(uu.blind(f"Nothing found for folder {boxfolder}"))

    return boxfolder, folderid
        

@trap
def get_destination() -> tuple:
    """
    Prompt for a destination (host name), and return the host
        name and the connection info.
    """

    hostinfo = None

    while not hostinfo:
        destination = input('Where is the file going? ')
        if destination == '?':
            hosts = uu.get_ssh_host_info('all')
            tabular_print(sorted([_ for _ in hosts if _ != '*']))
            continue
        
        hostinfo = uu.get_ssh_host_info(destination)
        if not hostinfo: 
            print(uu.blind(f"No info for a host named {destination}"))

    return destination, hostinfo


@trap
def get_encryption_key(default:str) -> str:
    """
    Prompt for a uid (email associated with an encryption key), and 
        return the appropriate key id.
    """    
    global all_recipients
    encryption_key = None

    while not encryption_key:
        encryption_key = input(f'Encrypted for whom? Default:{default} ')
        if encryption_key == '?':    
            tabular_print([ x for _ in all_recipients for x in _['uids'] ], 1)
            encryption_key = None
            continue

        if not encryption_key: encryption_key = destination
        
    return encryption_key
    
        

@trap
def tabular_print(data:list, row_len:int=10) -> None:
    """
    print a long list as a table.

    data    -- a 1-D list to printed as a table.

    row_len -- how many you want to print per row. It is
        just a suggestion. If the software can detect
        the number of columns on the screen, it will
        reduce the columns.

    """

    screen_width = uu.columns()
    
    longest_item = max([len(_) for _ in data])
    max_columns = screen_width // (longest_item + 1)
    row_len = min(row_len, max_columns)

    print_element = " {: <" + str(longest_item) + "}" 
    print_element = print_element*row_len
    rows = math.ceil(len(data)/row_len)
    for i in range(rows):
        chunk = data[i*row_len : (i+1)*row_len]
        while len(chunk) < row_len:
            chunk.append("")
        print(print_element.format(*chunk))


@trap
def do_work() -> int:

    global encryption_command
    global confirmation_text

    print(banner)

    # Fact 1
    filename = input('Name of the file you are moving? ')

    # Fact 2
    boxfolder, boxfolder_id = get_boxfolder()

    # Fact 3
    destination, hostinfo = get_destination()

    # Fact 4
    remote_dir = input('Directory (if any) at destination? ')

    # Fact 5
    encryption_key = get_encryption_key(destination)

    # Fact 6
    if encryption_key != 'NONE':
        encrypted_ext = input('File extension on encrypted copy? Default: pgp')
        if not encrypted_ext: encrypted_ext = 'pgp'
        local_filename = ".".join([filename, encrypted_ext])
    else:
        local_filename = filename

    print(confirmation_text.format(filename, boxfolder, destination, encryption_key))
    OK = input("Type 'yes' to confirm: ")
    if OK.lower() != "yes":
        print("stopping before any damage is done.")
        return os.EX_DATAERR
    
    if box_connector.get(filename, boxfolder_id, os.getcwd(), True):    
        print(f"file {filename} downloaded.")
    else:
        print(f"No file named {filename} was found.")
        return os.EX_NOINPUT

    if encryption_key != 'NONE':
        shred = ""
        print(f"encrypting file")
        for _ in all_recipients:
            if encryption_key == str(_['keyid']): 
                shred = "--recipient " + shred
                break

            if encryption_key.lower() in str(_['uids']).lower(): 
                shred = "--recipient " + str(_['keyid'])
                break

        if not shred:
            print(f"no key matching {encryption_key}.")
            return os.EX_USAGE

        encryption_command = f"{encryption_command} {shred} -o {local_filename} {filename}"
        command = shlex.split(encryption_command)
        result = subprocess.run(command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            timeout=3)
        if result.returncode != 0:
            print("encryption failed.")
            print(f"{result}")
            tomb.tombstone(uu.fcn_signature('run', command))
            sys.exit(-1)


    print(f"attempting delivery to {destination}")
    handle = hop.HOP(hostinfo)
    result = handle.send_one_file(local_filename, local_filename, True, remote_dir)
    if result:
        print(f"delivered {local_filename}")
        return os.EX_OK

    else:
        print(uu.blind(f"delivery failed. result = {result}"))
        return os.EX_IOERR
     

@trap
def deliver_main():
    try:
        sys.exit(do_work())

    except EOFError:
        print("Whoa! You pressed control-D")
        
    except KeyboardInterrupt:
        print("Whoa! You asked to exit.")

    sys.exit(os.EX_OK)


if __name__ == "__main__":
    deliver_main()
