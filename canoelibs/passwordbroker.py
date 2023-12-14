# -*- coding: utf-8 -*-
""" Module comment. """

import typing
from   typing import *

""" Standard Python imports """

import json
import os
import socket
import sys

""" Installed imports """

import msgpack

""" Canøe imports """

import tombstone as tomb
import urutils as uu

""" Credits """

__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2019, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagi@richmond.edu'
__status__ = 'Production'

""" License """

__license__ = 'MIT'
import license

""" ******************** BEGIN ******************** """

class PasswordBroker:
    """
    This object presents a callable Python interface to the 
    daemon that provides items (usually passwords) from the 
    Cyberark instance. The details of invocation are shown
    in the relevant functions below.
    """
    def __init__(self, **info) -> None:
        """
        Make a connection to the canoearkd process at the given address.

        info -- keywords containing IP, port, timeout, etc.

        Example:
            x = PasswordBroker({'IP':'8.8.4.4', etc.})

        """

        info = uu.sloppy(info)
        try:
            self.ip_address = info.get('IP', '127.0.0.1')
            self.port = int(info.get('port', 4000))
            self.timeout = int(info.get('timeout', 1))
            self.bufsize = int(info.get('bufsize', 4096))

        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            raise e from None

        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(self.timeout)

        except Exception as e:
            tomb.tombstone('Unable to connect to canoearkd')
            raise


    def __del__(self) -> None:
        """
        Be polite and disconnect.
        """
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()

        except Exception as e:
            pass


    def __lshift__(self, some_args:Union[str, tuple]) -> Union[dict, str, None, bool]:
        """
        Make a request of the PasswordBroker, and await the answer.

        some_args -- folder and credential that you want. There are subcases
            of allowable items. 

        1. The default folder is root/.
        1a. A single credential stored in root/

            password = broker << name_of_password

        1b. Several passwords stored in root/

            password_dict = broker << list_of_password_names

        2. Some other folder.
        2a. A single password.
    
            password = broker << (folder_name, name_of_password)

        2b. Several passwords.

            password_dict = broker << (folder_name, list_of_password_names)         

        returns --  object on success.
                    False indicates a denial.
                    None suggests that there was no response.
        """
        try:

            # We need to supply the name of the folder if the caller did not.
            if isinstance(some_args, str) or isinstance(some_args, list):
                some_args = {"folder":"root", "object":some_args}
            elif isinstance(some_args, tuple):
                some_args = {"folder":some_args[0], "object":some_args[1]}
            else:
                raise TypeError('<< called with bad argument type')

            self.sock.connect((self.ip_address, self.port))
            self.sock.setblocking(True)
            self.sock.sendall(self._encode_message(some_args))
            return self._decode_message(self.sock.recv(self.bufsize))

        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            return None


    def _encode_message(self, info:dict) -> bytes:
        """
        Transform the kwargs into a message to be written to the socket to request a 
        credential.
        """
        return msgpack.packb(info)


    def _decode_message(self, msg:bytes) -> tuple:
        """
        Unpack the answer[s]. Lists of things are returned as Python tuples rather
        than mutable lists.
        """
        return msgpack.unpackb(msg, use_list=False, encoding='utf-8') 


if __name__ == "__main__":
    info = {
        'IP':'127.0.0.1',
        'port':4000,
        'timeout':3,
        'bufsize':4096
        }

    broker = PasswordBroker(**info)
    x = {'safe':'canoe', 'folder':'root', 'object':['caone-1', 'x', 'y']}
    print("{}".format(broker << x))

else:
    # Branch taken when imported.
    pass

