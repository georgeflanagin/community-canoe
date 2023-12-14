#!/usr/bin/python3# -*- coding: utf-8 -*-

import typing
from typing import *

import pdb

"""
Welcome to TCSP. This is the T(ourtured | oken) Cloud Service Provider
class. This base class and its derivatives use the requests package
to communicate with cloud storage providers whose API calls depend on
acquiring a magic token that must be submitted with each call. The magic
tokens usually have a shelf life after which they expire and fresh tokens
must be acquired in order to make further API calls.  """

# Credits
__author__ = 'Douglas Broome'
__copyright__ = 'Copyright 2017, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin, Douglas Broome'
__email__ = '{ gflanagin | dbroome }'
__status__ = 'Prototype'

# Builtin packages.
from abc import *
import enum
import functools
import glob
from http import HTTPStatus
import os
import pprint
import requests
import time
#import typing
#from typing import *
import uuid
import urtypes as ut

# UR imports
from urdecorators import show_exceptions_and_frames as trap
# from urdecorators import null_decorator as trap
import tombstone as tomb
import urobject as uo
import urexception as ue
import urutils as uu
import fname as fn

class URTCSPHOP(uo.URObject,metaclass=ABCMeta):
    """
    """   
    @trap
    def __init__(self, g:uu.SloppyDict, default_klobber:bool=False):
        uo.URObject.__init__(self, g)
        #Call register methods. These should initialize variables needed for the upcoming sanity check.
        self._register_service_name()
        self._register_required_keys()
        #Sanity check for instance configuration. Let's see if we have everything we need to have
        #a chance at communicating with the cloud API.
        if not g:
            raise ue.URException("No config info supplied.")
        self._config = g
      
        #Now we are going to build some keyword arguments for calls to the requests package. We are
        #doing this because we will make a number of method calls with the requests package across 
        #which some keyword arguments (proxy,verify) will will be identical. We will build the list
        #in self._rkwargs.
        self._rkwargs = {}
     
        if self._config.get('web_proxy'):
            self._rkwargs.update({'proxies': { k:self._config['web_proxy'] for k in ['http','https'] } })

        if self._config.get('verify-ssl-certs') == 'False':
            self._rkwargs['verify'] = False
            #If we are turning off SSL cert verification, we also need to tell the requests
            #package to ignore errors produced by "bad" certs
            from requests.packages.urllib3.exceptions import InsecureRequestWarning
            requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
       
        #Set the klobber option
        self._default_klobber = default_klobber
        self.box_folder_info = None


    ### Register methods

    @abstractmethod
    def _register_service_name(self) -> None:
        """
        Derived classes must override this method and set self._service_name to a string representing 
        the cloud storage ("box" or "sharefile") or the sanity checks in __init__ will fail
        __init__ will fail.
        """
        pass
       

    @abstractmethod 
    def _register_required_keys(self) -> None:
        """
        Derived classes must override this method and set self._required_keys to a list of strings
        indicating what config items are needed for the cloud storage ("private-key", "tenant-name")
        or the sanity checks in __init__ will fail.
        """
        pass


    ### Utility methods
    
    def _build_bearer_token_header(self) -> Dict:
        access_token = self._get_access_token()
        return { "Authorization": "Bearer {0}".format(access_token) } 

    
    def _do_request(self, requests_method: Callable, url: str, expected_codes: List[HTTPStatus], 
                    **kwargs) -> requests.models.Response:
        kwargs.update(**self._rkwargs)
        #If there are no headers in the provided args, set the headers to the bearer token.
        #If there are headers included, it is the responsibility of the caller to include
        #a bearer token
        if not 'headers' in kwargs: kwargs['headers'] = self._build_bearer_token_header() 
        try:
            #tomb.tombstone(uu.fcn_signature('requests_method', url, kwargs))
            response = requests_method(url, **kwargs) 
        except Exception as e:
            raise URTCSPException(exception=e)

        #tomb.tombstone('response.status_code='+str(response.status_code))
        if not HTTPStatus(response.status_code) in expected_codes:
            raise URTCSPException(response=response)
        return response


    def _do_get(self, url: str, expected_codes:  List[HTTPStatus], **kwargs) -> requests.models.Response:
        return self._do_request(requests.get, url, expected_codes, **kwargs)


    def _do_post(self, url: str, expected_codes: List[HTTPStatus], **kwargs) -> requests.models.Response:
        return self._do_request(requests.post, url, expected_codes, **kwargs)

    
    def _do_delete(self, url: str, expected_codes: List[HTTPStatus], **kwargs) -> requests.models.Response:
        return self._do_request(requests.delete, url, expected_codes, **kwargs)
    
    def _do_put(self, url: str, expected_codes: List[HTTPStatus], **kwargs) -> requests.models.Response:
        return self._do_request(requests.put, url, expected_codes, **kwargs)
   
    @abstractmethod 
    def _get_access_token(self) -> str:
        """
        Returns the next magic token to use for API calls
        """
        pass
    
    ### Public methods


    @abstractmethod
    def browse(self, cloud_folder_id: str) -> dict:
        """
        Returns a dict containing information about the folder hosted by the cloud storage provider

        cloud_folder_id -- a string containing the unique identifier  of the folder whose contents 
            you wish to investigate.

        returns -- a (possibly empty) dictionary containing data about the names of the items in the
            folder 
        """
        pass


    # @abstractmethod
    def delete(self, cloud_folder_id:str, filename:str=None) -> ut.TCSPResults:
        """
        Delete a file from a cloud storage folder.

        cloud_folder_id -- a (possibly empty) folder where the item to
            be deleted is found.

        item_name -- the name of the file in in the folder. 

        returns -- a boolean value indicating whether we successfully deleted an item
                   from the folder
        """
        pass


    @abstractmethod
    def get(self, filename:str, filename2:str=None, cloud_folder_id:str=None, 
            klobber:bool=None) -> ut.TCSPResults:
        """
        Download a file from a cloud storage providr.
        
        filename -- name of the file to get from Box.

        filename2 -- the name of the file on the local filesystem after it has been 
            downloaded, if different from the name of the file in the folder.

        cloud_folder_id -- the id of the folder containing the file
        
        klobber -- whether or not to overwrite a local file if present.

        returns -- True on success; False otherwise. Also returns False if klobber is off
            and file already exists at the target path on the local filesystem
        """
        pass


    @abstractmethod
    def put(self, filename:str, filename2:str=None, cloud_folder_id:str=None, 
            klobber:bool=None) -> ut.TCSPResults:
        """
        Move a file to the cloud storage

        filename -- name of the file to be loaded. This name must be in the local file system (i.e., it 
            cannot be in Box.
        
        filename2 -- name of the file once it gets to box. NOTE: this parameter is ignored
            if filename is a wildcard.

        cloud_folder_id -- the id of the target folder
        
        klobber -- whether or not to update something that is already
            present.

        returns -- True on success; False otherwise. Also returns False if klobber is off and a file with
            the filename already exists in Box.
        """
        pass


class URTCSPException(ue.URException):
    """ Base exception for any TCSP errors """
    def __init__(self, msg:str='TCSP Error',line:int=None,response:requests.models.Response=None,exception:Exception=None):
        self.status = None
        if exception:
            if exception is OSError:
                msg = exception.strerror
                if exception.filename:
                    msg += ": " + filename
                if exception.filename2:
                    msg += ", " + filename2
            else:
                msg = str(exception)
        elif response != None:
            self.status = HTTPStatus(response.status_code)
            try: 
                msg = '|'.join([" {} : {} ".format(k,response.json()[k]) for k in response.json()])
            except:
                #comment this (or change it)
                json_dict = ''
                self._error_message = ''
        #Now call super with appropriate msg 
        ue.URException.__init__(self, msg, line)
