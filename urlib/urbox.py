#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
urbox provides write access to a folder in UR's Box Enterprise account.
"""
import typing
from typing import *

import pdb

# Credits
__author__ = 'Douglas Broome'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin, Douglas Broome'
__email__ = '{gflanagin | dbroome}@richmond.edu'
__status__ = 'Prototype'


__license__ = 'MIT'
import license

# Builtin packages.
import enum
import fnmatch
import glob
from http import HTTPStatus
import os
import pprint
import requests
import sys
import time
import uuid

# UR imports
import tombstone as tomb
import urtcsp as tcsp
import urobject as uo
import urexception as ue
import urutils as uu
import fname

from urtypes import *
from urdecorators import show_exceptions_and_frames as trap

# Note: generally, we don't alias so many things, but a smart person will
# guess that these things with cryptograpic names probably came from a
# cryptographic module.

from Crypto.PublicKey import RSA

# Import JWT and register the desired
# algorithms. Switching the algorithms is covered at:
# https://pyjwt.readthedocs.org/en/latest/installation.html#legacy-deps.
# This section is marked off because it has caused some trouble on
# development machines. Due to the nature of our production environment,
# we can't take the default encryption alogrithms from the cryptography
# module that PyJWT wants to use. Instead we will be using algorithms
# from pycrypto. Some machines seem to throw exceptions when we call
# the jwt.unregister_algorithm function saying that the algorithm is
# already registered so in the code below we call jwt.unregister_algorithm
# defensively within a specialized exception handler to make sure our path
# to registering our preferred algorithms is clear.

# Import PyJWT
import jwt

# Didn't want to write a complicated exception handler so instead the code
# below uses a template function to generate the same code block twice
# for the RS256 and ES256 algorithms

def generate_jwt_unregister_func(alg:str):

    def jwt_unregister_func():
        try:
            jwt.unregister_algorithm(alg)
        except KeyError as ke: 
            if ke.args[0] == 'The specified algorithm could not be removed because it is not registered.':
                pass
            else:
                raise tcsp.URTCSPException(exception=ke)
        except Exception as e:
            raise tcsp.URTCSPException(exception=e)

    return jwt_unregister_func

# Use the template function to generate the code blocks and call them 
# right away with ()
[ generate_jwt_unregister_func(alg)() for alg in ['RS256','ES256'] ]

from jwt.contrib.algorithms.pycrypto import RSAAlgorithm
from jwt.contrib.algorithms.py_ecdsa import ECAlgorithm
jwt.register_algorithm('RS256', RSAAlgorithm(RSAAlgorithm.SHA256))
jwt.register_algorithm('ES256', ECAlgorithm(ECAlgorithm.SHA256))

BOXCODES = uu.SloppyDict({
    "OK":       [HTTPStatus.OK],
    "OK_CREATE":[HTTPStatus.OK,HTTPStatus.CREATED],
    "CREATE":   [HTTPStatus.CREATED],
    "OK_DELETE":[HTTPStatus.NO_CONTENT, HTTPStatus.OK],
    "DELETE":   [HTTPStatus.NO_CONTENT]
    })

class URBoxHOP(tcsp.URTCSPHOP):
    """
    Box specific operations modeled on the base class.
    """
    
    def _register_service_name(self):
        self._service_name = "box"


    def _register_required_keys(self):
        self._required_keys = ["client-id", "client-secret", "enterprise-id", 
            "jwt-key-id", "rsa-private-key-file", "rsa-private-key-passphrase", 
            "app-user-id"]


    @trap
    def _build_jwt_assertion(self, header:dict, body:dict, key_file_path:str, 
                             key_passphrase:str=None) -> bytes:
        """
        Uses the PyJWT module to build a JWT assertion, sign it with an RSA
        key, and base64url encode the result.
    
        header -- a string for the JWT.
        body -- what you are really trying to say, so to speak.
        key_file_path -- valid path name with the signing key.
        key_passphrase -- an optional passphrase.
    
        returns -- the JWT assertion as a base64url encoded string.
        """
        #Read RSA key information  
        f = fname.Fname(key_file_path)
        if not f: 
            raise Exception("Key file path {f.fqn} not found or inaccessible.")
        elif not header: 
            raise Exception("Missing JWT header.")
        elif not body: 
            raise Exception("Missing JWT body.")
    
    
        try:
            key_contents = f()
            rsa_obj = RSA.importKey(key_contents,passphrase=key_passphrase)

        except Exception as e:
            raise
        
        return jwt.encode(body, rsa_obj, "RS256", headers=header)


    @trap
    def _build_jwt_body(self, client_id:str, subject_id:str, subject_type:str, 
        audience:str, duration:int=30) -> dict:
        """
        Construct a dict representing the JWT body with client, subject, 
        and audience information
    
        client_id -- base64 encoded rep of the client_id
        subject_id -- base64 encoded rep of the subject_id
        subject_type -- string
        audience -- base64url string of the URL associated with the destination.
    
        returns -- a dictionary with the correct information for the JWT body.
        """
        if not all([client_id, subject_id, subject_type, audience]):
            raise Exception("Missing information to build JWT body.")
            
        return { "iss": client_id, 
                 "sub": subject_id, 
                 "box_sub_type": subject_type,
                 "aud": audience, 
                 "jti": str(uuid.uuid4()), 
                 "exp": int(time.time()) + int(duration) }


    @trap
    def _build_jwt_header(self, key_id:str) -> dict:
        """
        Construct a dict representing the JWT header with our Box App's public key id
        """
        return dict.fromkeys(['kid'], key_id)


    @trap
    def _get_access_token(self)-> str:
        """
        Make a call to get an oauth2 token.

        returns -- a string representation of the access token.
        """
        subject_type = 'user'
        subject_id = self._config['app-user-id']
        
        d_jwt_header = self._build_jwt_header(key_id=self._config['jwt-key-id'])
        d_jwt_body = self._build_jwt_body(client_id=self._config['client-id'],
                        subject_id=subject_id,
                        subject_type=subject_type,
                        audience='https://api.box.com/oauth2/token')

        assertion = self._build_jwt_assertion(header=d_jwt_header, 
                        body=d_jwt_body,
                        key_file_path=self._config['rsa-private-key-file'],
                        key_passphrase=self._config['rsa-private-key-passphrase'])

        url = 'https://api.box.com/oauth2/token'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = { "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", 
                 "assertion": assertion, 
                 "client_id": self._config['client-id'], 
                 "client_secret": self._config['client-secret'] }

        response = self._do_post(url, expected_codes=BOXCODES.OK, headers=headers, data=body)
        return response.json()['access_token']


    @trap
    def _get_folder_info(self, box_folder_id:str) -> uu.SloppyDict:
        """
        Fetches the JSON description of a Box folder from the Box Content API and returns
        it as a dict.

        box_folder_id -- unique Box folder identifier

        returns -- a dict containing the JSON description of the folder from Box
        """

        url = f'https://api.box.com/2.0/folders/{box_folder_id}'
        return uu.deepsloppy(self._do_get(url, expected_codes=BOXCODES.OK).json())
   
     

    @trap
    def _delete_single_file(self, 
            box_folder_id: str, 
            filename:str) -> TCSPOpResult:

        # If the info exists, no need to ask for it again.
        if self.box_folder_info is None:
            self.box_folder_info = self.browse(cloud_folder_id=cloud_folder_id)

        url = f"https://api.box.com/2.0/files/{self.box_folder_info[filename]}"
        response = self._do_delete(url, expected_codes=BOXCODES.DELETE)
        return True if response.status_code in BOXCODES.DELETE else False


    @trap
    def _get_single_file(self, 
            box_folder_id:str, 
            filename: str, 
            local_dir:str,
            klobber:bool=False) -> bool:
        
        uu.tombstone(uu.fcn_signature(
            '_get_single_file', box_folder_id, filename, local_dir, klobber
            ))
        local_filename = uu.path_join(local_dir, filename)

        # Don't do anything if klobber is off and the file is already present
        if not klobber and os.path.isfile(local_filename):
            uu.tombstone(f"{local_filename} exists and will not be overwritten.")
            return True

        # OK let's do this.
        box_file_id = self.box_folder_info[filename]
        uu.tombstone(f"{box_file_id} corresponds to {filename}")
        url = f'https://api.box.com/2.0/files/{box_file_id}/content'
        response = self._do_get(url, expected_codes=BOXCODES.OK)
        uu.tombstone(f"_do_get yields {response}")

        try:
            with open(local_filename,'wb') as download_file:
                download_file.write(response.content)
            uu.tombstone(f"wrote {local_filename}")
            return True

        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            return False
        

    @trap
    def _put_single_file(self, 
            box_folder_id: str, 
            filename: str, 
            klobber: bool) -> bool:

        """
        Uploads or updates a file in Box.

        box_folder_id  -- string representation of a box folder's integer ID.
        filename       -- local, fully qualified name, of what we are moving.
        klobber        -- True: upload if new file, update version if file exists.
                          False: don't overwrite an existing file.
                          None: blow away any existing file and all its old 
                            versions, then upload.

        returns -- whether or not the operation succeeded.     
        """

        filename_in_box = fname.Fname(filename).fname

        # if we don't want to overwrite, check to see if the file is already there.
        if klobber is False and filename_in_box in self.box_folder_info:
            uu.tombstone(f"{filename} exists and cannot be overwritten.")
            return False

        # we only need to remove the existing file if it is there.
        if klobber is None and filename_in_box in self.box_folder_info:
            try:
                self._delete_single_file(box_folder_id, filename_in_box)
            except Exception as e:
                uu.tombstone(f'existing {filename} cannot be deleted.')
                return False

        # decide if we are updating or uploading
        foo = ( self._update_file if 
                    filename_in_box in self.box_folder_info and klobber is not None 
                else self._upload_file )
        try:
            return foo(box_folder_id, filename)

        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            return False


    @trap
    def _update_file(self, 
        box_folder_id:str, 
        filename: str) -> bool:
        """
        Create a new version of an existing file.

        box_folder_id -- where it goes.
        filename      -- the local, fully qualified filename.

        returns -- True if it worked, false otherwise.
        """

        if not fname.Fname(filename):
            uu.tombstone("{filename} cannot be found.")
            return False

        if self.box_folder_info is None:
            self.box_folder_info = self.browse(box_folder_id)

        filename_in_box = fname.Fname(filename).fname

        try:
            box_file_id = self.box_folder_info[filename_in_box]
            with open(filename, 'rb') as upload_file:
                url = f'https://upload.box.com/api/2.0/files/{box_file_id}/content'
                response = self._do_post(url,
                    expected_codes=BOXCODES.OK_CREATE,
                    files={'file': upload_file})
            return True if response.status_code in BOXCODES.OK_CREATE else False

        except Exception as e:
            uu.tombstone(uu.type_and_text(e))
            return False


    @trap
    def _upload_file(self, 
            box_folder_id: str, 
            filename:str) -> bool:
        """
        uploads a 'new' file to Box.
        
        box_folder_id -- where it goes.
        filename      -- the local, fully qualified filename.
        
        returns -- True if it worked, False otherwise.
        """

        filename_in_box = fname.Fname(filename).fname

        try:
            with open(filename,'rb') as upload_file:
                url = 'https://upload.box.com/api/2.0/files/content'
                body = { "attributes": '{ "name":"' + filename_in_box + '", "parent": {"id": "' + box_folder_id  + '"} }' }

                response = self._do_post(url, 
                    expected_codes=BOXCODES.CREATE, 
                    files={'file': upload_file}, 
                    data=body)
                return True if response.status_code in BOXCODES.CREATE else False

        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            return False


    @trap
    def _rename_file(self, 
        filename:str, 
        cloud_folder_id:str=None,
        new_filename:str=None, 
        new_folder_id:str=None) -> int:
        """
        Rename one file. Note that this function is only called by rename(), and
        it is there that the arguments are examined for validity. And browse() has
        been called to get the folder's contents before the move begins.

        returns -- 0 or 1, the number of files renamed.
        """

        #Snippet of JSON that conforms to Box API call for file renaming
        fmt_json = '{{"name":"{0}","parent":{{"id":"{1}"}}}}'

        try:
            url = f'https://api.box.com/2.0/files/{self.box_folder_info[filename]}'
        except KeyError as ke:
            tomb.tombstone(f"Did not find a file named {filename} in Box folder.")
            return 0

        target_folder = cloud_folder_id if new_folder_id is None else new_folder_id
        target_filename = filename if new_filename is None else new_filename
        
        try:
            self._do_put(url, expected_codes=BOXCODES.OK, 
                data=fmt_json.format(target_filename,target_folder))
            return 1
        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            return 0


    ######################
    ### Public methods ###
    ######################

    @trap
    def browse(self, cloud_folder_id: str) -> uu.SloppyDict:
        
        tomb.tombstone(f'browsing {cloud_folder_id}')
        folder_info = self._get_folder_info(cloud_folder_id)
        item_count = int(folder_info.item_collection.total_count)
        folder_items = {}
        
        offset = 0
        original_block_size = 997
        block_size = 997 # This is tunable. 

        while len(folder_items) < item_count:

            uu.tombstone(f"folder_items: {len(folder_items)} item_count: {item_count}")
            url = f'https://api.box.com/2.0/folders/{cloud_folder_id}/items?limit={block_size}&offset={offset}'
            response = self._do_get(url, expected_codes=BOXCODES.OK)

            folder_items.update({ e['name']: e['id'] 
                for e in response.json()['entries'] })
            offset += original_block_size
        else:
            uu.tombstone(f"Exiting while loop with offset={offset}")
    
        folder_keys = sorted(list(folder_items.keys()))
        folder_items = {k:folder_items[k] for k in folder_keys}

        tomb.tombstone(f'{len(folder_items)} files in {cloud_folder_id} browsed.')
        return uu.deepsloppy(folder_items)


    @trap
    def create_folder(self, folder_name:str, parent_folder_id:str='0') -> str:
        """
        Create a child folder.

        folder_name -- the name of the new folder.
        parent_folder -- the stringized id of the parent folder.

        returns -- the id of the new folder, or False if the call fails.
        """
        try:
            if not int(parent_folder_id):
                tomb.tombstone("Cannot create toplevel folders.")
                return False
        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            return False

        url = 'https://api.box.com/2.0/folders'
        body = f'{{ "name":"{folder_name}", "parent": {{"id":"{parent_folder_id}"}} }}'
        response = self._do_post(url, expected_codes=BOXCODES.CREATE, data=body)
        return str(response.json()['id'])


    @trap
    def delete_folder(self, cloud_folder_id: str, recursive: bool = False):
        url = f'https://api.box.com/2.0/folders/{cloud_folder_id}'
        response = self._do_delete(url, expected_codes=BOXCODES.DELETE,
                                   params={"recursive": str(recursive).lower()})
        return response.status_code in BOXCODES.DELETE


    @trap
    def get(self, 
            filename:str, 
            box_folder_id:str, 
            local_dir:str,
            klobber:bool=None) -> int:
        """
        Get a file from Box.

        filename -- The filename, as it is known to Box.
        box_folder_id -- The string representing the integer folder id.
        local_dir -- Where we are downloading to.
        klobber -- True or False whether we overwrite an existing file.

        returns  -- number of files gotten.
        """

        if self.box_folder_info is None:
            self.box_folder_info = self.browse(box_folder_id)
        source_files = fnmatch.filter(self.box_folder_info.keys(), filename)

        result = sum([ self._get_single_file(box_folder_id, filename, local_dir, klobber) 
            for f in source_files ])
        if not result: self.box_folder_info = None
        return result

    
    @trap
    def put(self, 
            filename:str, 
            box_folder_id:str=None, 
            klobber:bool=None) -> int:
        """
        Determines how many files there are, and then calls _put_single_file in
        a loop to move them. 

        filename -- a possibly wildcard, not fully qualified local filename.
        klobber  -- True:  overwrite existing
                    False: don't overwrite existing
                    None:  remove existing, then upload (prevents versions).

        returns  -- number of files put.
        """

        if self.box_folder_info is None:
            self.box_folder_info = self.browse(box_folder_id)
            
        return len([ self._put_single_file(box_folder_id, fname.Fname(f).fqn, klobber) 
            for f in glob.glob(filename) ])
        

    @trap
    def rename(self, filename:str, 
                cloud_folder_id:str, 
                new_filename:str=None, 
                new_folder_id:str=None, 
                klobber:bool=None) -> int:
        """
        Rename or move (effectively) a file in Box.

        filename -- the file we are trying to move/rename.
        cloud_folder_id -- the location of the original file.
        new_filename -- the new name.
        new_folder_id -- the destination folder.

        returns -- number of items renamed or moved.
        """
        
        if self.box_folder_info is None:
            self.box_folder_info = self.browse(cloud_folder_id)
        source_files = fnmatch.filter(self.box_folder_info.keys(), filename)
        
        if len(source_files) == 0:
            tomb.tombstone("{filename} not found; cannot rename it.")
            return 0

        if all([ _ is None for _ in (new_filename, new_folder_id)]):
            tomb.tombstone("Nothing to rename with {filename}")
            return 0

        # If there is only one file, just rename it.
        if len(source_files) == 1:
            return self._rename_file(filename, cloud_folder_id, new_filename, new_folder_id)

        # Ensure we don't have conflicting rules.
        if new_filename or not new_folder_id:
            tomb.tombstone("Moving many files requires a new folder id only.")
            return 0

        # Do it.
        return sum(self._rename_file(_, cloud_folder_id, new_filename, new_folder_id) 
            for _ in source_files)
