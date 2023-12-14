# -*- coding: utf-8 -*-
""" 
Canøe VM component to handle 'file' transfers to destinations (including
localhost.
"""

import typing
from   typing import *

import os
import requests
import sys
import tempfile

import fname
import tombstone as tomb
import urutils as uu
from   urdecorators import show_exceptions_and_frames as trap


# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2019, University of Richmond'
__credits__ = None
__version__ = '0.9'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'testable'

__license__ = 'MIT'
import license

class ShareFile:
    """
    Put a file up on a ShareFile endpoint.
    """
    
    __slots__ = ( 'grant_type', 'client_id', 'client_secret', 
        'username', 'password', 'root_folder', 'host', 
        'url', 'response', 'headers', 'connected', 'postURL' )

    __values__ = ('password', "", "", 
        "", "", "home", "localhost", 
        None, None, None, False, None)

    __defaults__ = uu.SloppyDict(dict(zip(__slots__, __values__)))

    def __init__(self, args:uu.SloppyDict) -> None:
        """
        Set up the object.
        """
        for slot in ShareFile.__slots__:
            setattr(self, slot, args.get(slot, ShareFile.__defaults__[slot]))
        self.url = f"https://{self.host}/oauth/token"
        self.postURL = f"https://{self.host}/sf/v3/Items({self.root_folder})/Upload2"


    def __str__(self) -> str:
        return "\n".join([ f"{_}:{str(getattr(self, _))}" for _ in ShareFile.__slots__ ])


    @trap
    def _connect(self) -> bool:
        """
        Get connected.
        """
        elements = ( 'grant_type', 'client_id', 'client_secret', 'username', 'password' )
        data = {}
        for e in elements: data[e] = getattr(self, e)
        print(f"{self.url} {data}")
        self.response = requests.post(self.url, data = data)
        self.headers = {'Authorization': f"Bearer {self.response.json()['access_token']}"}
        self.connected = self.response.status_code == 200
        return self.connected
                
         
    @trap
    def send(self, filename:str) -> bool:
        """
        Send a file with as little fuss as possible.
        """
        f = fname.Fname(filename)

        if not self.connected: self.connected=self._connect() 
        if not self.connected:
            tomb.tombstone('Cannot connect. Could not connect to do the send.')
            return False
        
        
        data={"Method":"Method", "Raw":False},
        self.response = requests.post(url=self.postURL, 
            data=data,
            headers=self.headers)         

        if self.response.status_code != 200:
            tomb.tombstone(f'Could not get upload URL {self.postURL}\n data={data}\n headers={self.headers}')
            return False

        files = {'File1': (f.fname, open(str(f), 'rb'), 'text/plain')}
        self.response = requests.post(url = self.response.json()['ChunkUri'], files = files)
        
        if self.response.status_code < 300 and self.response.text != "OK":
            tomb.tombstone(f"Some error occurred: {self.response.text}")

        if self.response.status_code > 200:
            tomb.tombstone(f'Error: {self.response.status_code}')
        
        tomb.tombstone(f"{filename} successfully transferred.")
        return self.response.status_code == 200


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage sharefile.py {filename-to-upload}")
        sys.exit(os.EX_NOINPUT)
 

    args= { 'host':'presence.sharefile.com',
            'client_id':'KYVDZFvw4SjErWFxmBXit4XgkUVtqHPs',
            'client_secret':'fKhpDFld0s8MS9uHVdMcXQcK89WKzVq8Q8jCPLsmrconBmOL',
            'user_name':'canoe@richmond.edu',
            'user_password':'54owsenICMarv' }
            
    
    sharefile_connection = ShareFile(**args)
    for f in sys.argv[1:]: sharefile_connection.send(f)
