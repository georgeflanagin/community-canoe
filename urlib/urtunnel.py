#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This code is based on a demo routine in the paramiko project. I am not
pretending to have written it, but I have adapted it for University of
Richmond's purposes. I have also clarified the names of the objects,
and added comments. There is also a wrapper, and I have converted it to
Python3. 

Most of my comments are prefaced with -g->. All functions except for
drill_baby_drill() start with a leading underscore and are not imported
except if they are explicitly listed.

Come to think of it .... I guess it is a rewrite. 
    -- George Flanagin 12 Feb 2016
"""

__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2016, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Prototype'

# Builtins
import argparse
import os
import socket
import sys
import typing
from   typing import *

# Paramiko
import logging
import paramiko 
from   paramiko import SSHClient, SSHConfig, SSHException

# University of Richmond libraries
import tombstone as tomb
import urutils as uu
from   urdecorators import show_exceptions_and_frames as trap

import pdb
run_debugger = False

def drill_baby_drill(remote_host_info:uu.SloppyDict, password:str=None) -> SSHClient:
    """

     {'connectionattempts': '3',
      'connecttimeout': '2',
      'controlpath': '/tmp/ssh-canoe@starr.richmond.edu:22',
      'hostname': 'starr.richmond.edu',
      'identityfile': ['/home/canoe/.ssh/id_rsa'],
      'port': '22',
      'serveraliveinterval': '59',
      'user': 'canoe'}

    """
    return _getSSHConnection(remote_host_info, None, password)


@trap
def _getSSHConnection(info:uu.SloppyDict, sock=None, password:str=None) -> object:
    """
    Return an SSH client connection given the name of some host that
    is mentioned in the ~/.ssh/config file.

    sock -- If this argument is None, then we simply use one on
        the local side that is available. In most cases, we don't
        care about the value.

    returns -- the connected SSH client.
    """

    client = SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # See if we must use a password.
    if password:
        try:
            uu.tombstone("Attempting to connect with password.")
            client.connect(info.hostname, int(info.port), 
                username=info.user, password=password,
                allow_agent=False, look_for_keys=False)

        except Exception as e:
            uu.tombstone(uu.type_and_text(e))
            uu.tombstone("While trying password, an exception was raised.")

        else:
            uu.tombstone("Password accepted.")
            return client
        
    # If we get to here (i.e., we did not try a password, and either failed or
    # or succeed) then we are doing key based authentication.
    try:
        client.connect(info.hostname, 
            int(info.port), 
            username=info.user, 
            key_filename=info.identityfile,
            sock=sock)
        return client
    
    except AttributeError as e:
        uu.tombstone(f"There is no key for {info.hostname}")

    except FileNotFoundError as e:
        uu.tombstone(f"Could not find a file like {info.identityfile}")

    except paramiko.ssh_exception.AuthenticationException as e:
        uu.tombstone(f'keys in the list {info.identityfile} were rejected.')

    except paramiko.ssh_exception.BadAuthenticationType as e:
        uu.tombstone(f'Server {info.hostname} does not support keys.')

    except socket.gaierror as e:
        uu.tombstone(["no info on",info.hostname])
        uu.tombstone(uu.type_and_text(e))
        raise e

    except paramiko.ssh_exception.NoValidConnectionsError as e:
        uu.tombstone("Multiple failures. Here is what was tried.")
        for k,v in e.errors.items():
            uu.tombstone(str(k) + " -> " + str(v))
        if password is None: 
            uu.tombstone("No password available. Giving up.")
            raise 

    except Exception as e:
        uu.tombstone(uu.type_and_text(e))
        uu.tombstone("The keys do not work.")
        if password is None: 
            uu.tombstone("No password available (unknown exception). Giving up.")
            return None

