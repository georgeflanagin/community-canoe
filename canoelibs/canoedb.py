# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from typing import *

import base64
from   base64 import encodebytes, decodebytes
import binascii
from   Crypto.Cipher import AES
import doctest
import re
import os
import random
import re
import signal
import sqlitedb
import socket
import sys
import time

import pandas

from   urdecorators import show_exceptions_and_frames as trap
import urutils as uu

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.0'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Prototype'


__license__ = 'MIT'
import license

class CanoeCrypter: pass

class CanoeDB(sqlitedb.SQLiteDB):
    """ 
    CanoeDB is nothing but a gossamer shell to keep the code
    from being junked up. The only database you can open is the one defined
    in the configuration file. 
    """

    @trap
    def add_credential(self, sn:int, 
            k_family:str, k:str, v:str, desc:str, plain_text:bool=False) -> None:
        """
        Add a credential, encrypted or not, to the database.

        sn -- serial_number associated with requestor.
        k_family -- generally a recipe/event name
        k -- the key, a name to retrieve
        v -- the value ... the whole point of this exercise.
        desc -- just some text to help you remember what this thing is.
        plain_text -- True if you want to just store it, skipping the encryption.
        """

        SQL1 = """DELETE FROM credentials WHERE k_family = ? AND k = ?"""  
        SQL2 = """INSERT INTO credentials VALUES (?, ?, ?, NULL)"""
        SQL3="""insert into canoe_log
        (serial_number, sequence_number, microtime, recipe, message)
        values (?, ?, ?, ?, ?)"""
        
        v = v if plain_text else CanoeCrypter.get_instance().encrypt(v)
        self.execute_SQL(SQL1, k_family, k)
        self.execute_SQL(SQL2, k, k_family, v)
        self.commit()
        self.execute_SQL(SQL3, sn, 0, time.time(), 'cred-change', f"cred-change {k_family}:{k}")


    @trap
    def get_credentials_by_name(self, 
            sn:int, 
            k_family:str, 
            k:str,
            and_decrypt:bool = True) -> Dict[str,str]:
        """
        Show credentials matching the source criteria.

        sn -- only used for logging the access.
        k_family -- the job the credential belongs to
        k -- the credential's name
        and_decrypt -- if false, the raw data are returned.
        """
        
        gotten = "ALL" if not k_family else k_family
        gotten += ":ALL" if not k else (":"+k)

        results = self.get_credentials(and_decrypt)

        if not k_family and not k: return results
        elif not k:  
            try:
                return {k_family: results[k_family]}
            except:
                return "Nothing matching " + str(k_family)

        else:        
            try:
                return {k_family: {k: results[k_family][k]}}
            except:
                return "Nothing matching " + str(k_family) + " and " + str(k)
        


    @trap
    def get_event(self, serial_number:int) -> List[List[Dict]]:
        """ 
        Return the event records for a given serial number. 
        """

        if not self.sn: self.sn = uu.new_serial_number()
        return self.execute_SQL(
            """SELECT * FROM canoe_log WHERE serial_number = ?
             ORDER BY microtime ASC)""", self.sn)


    @trap
    def get_key(self, key:str) -> str:
        """ Retrieve the value of a key by its unique name. """

        if not self.sn: self.sn = self.new_serial_number()
        SQL = f"SELECT v FROM credentials WHERE k = {key}"
        return str(self.row_one(SQL, 'V'))


    @trap
    def get_keys(self, key_family:str) -> List[List[Dict]]:
        """ 
        Return a list of key-value pairs for a collection of keys
        identified by name. 
        """

        if not self.sn: self.sn = self.new_serial_number()
        SQL = f"SELECT k, v FROM credentials WHERE k_family = {key_family}"
        return self.execute_SQL(SQL)


    @trap
    def get_credentials(self,
            and_decrypt:bool=True) -> dict:
        """
        Retrieve the credentials from the table, sorted and then structured
        for ease of lookup.
        """

        self.sn = os.environ.get('sn', uu.new_serial_number())
        creds = {}
        SQL = "SELECT k, v, k_family FROM credentials ORDER BY k_family, k"
        frame = self.execute_SQL(SQL)
        
        for i in range(frame.shape[0]):
            row = frame.iloc[i]
            # The temp variables are to ease comprehension.
            family = row['K_FAMILY']
            key = row['K']
            value = row['V'][2:-1]

            try:
                newvalue = CanoeCrypter.get_instance().decrypt(value)

            except Exception as e:
                # This must have been an un-encrypted value.
                newvalue = row['V']

            finally:
                value = newvalue

            # This is a bit of future proofing and roll-forward/backward compatibility.
            # If the decrypted value is a text encoding of a byte encoding, then
            # we need to strip it down.
            #
            # Now that we allow plain text values, let's not attempt to trim
            # anything that is too short.
            if len(value) > 3 and value[:2] == "b'" and value[-1] == "'":
                value = value[2:-1]

            if family not in creds: 
                creds[family] = {}
            creds[family][key] = value

        return creds    


    @trap
    def record_packet_data(self, 
            serial_number: int, 
            service: str, 
            *,
            code: int=0, 
            shred1: str=0, 
            shred2: str="") -> bool:
        """
        This function saves in the database the same info that is in each Nagios
        packet.
        """
        SQL = """INSERT INTO nagios_packets (serial_number, service, code, shred1, shred2) 
            VALUES (?, ?, ?, ?, ?)"""
        return self.execute_SQL(SQL, *(serial_number, service, code, shred1, shred2))


    @trap
    def get_sn(self, sn:str):
        """
        return the events associated with the given serial number.
        """
        SQL="""SELECT * FROM canoelog WHERE sn = ?"""
        return self.execute_SQL(SQL, (sn,))



class CanoeCrypter:
    """
    An instance of this class (operationally speaking a singleton) can be
    used to encrypt text shreds.
    """
    
    kkey = base64.b64decode('b6U7hQGWrvA44E3VQqV1svRPzqriUI4ylwVdwZNRwIw=')
    block_size = 16
    key_size = 256

    def __init__(self, 
        key:str=None,
        key_representation:str='base64'):
        """
        This function should be treated as a protected constructor, and almost
        all uses of this class should call CanoeCrypter.get_instance() instead.

        key -- a representation of the key for a symmetric cipher.
        key_representation -- how the key is represented.
        """

        # Case 1: Woops! 
        if key is None and CanoeCrypter.kkey is None:
            raise Exception("Crypter not set up.")

        # Case 2: use a key we already have, a.k.a., the test key.
        if key is None and CanoeCrypter.kkey is not None:
            if __name__ == "__main__": 
                print("using default key")
            self.key = CanoeCrypter.kkey
            return

        # Case 3: Initialize the key by argument.
        if key_representation == 'base64' and len(key) != 44:
            raise Exception(
                "Bad input key length " + str(len(key)) + 
                " (should be 44 base64 chars ending in one '=') "
                )

            try:
                temp_k = base64.b64decode(str(key))
            except binascii.Error as e:
                raise Exception(str(e) + " :: Bad input key " + str(key))
            else:
                CanoeCrypter.kkey = temp_k[:32]
            self.key = CanoeCrypter.kkey
        


    @classmethod
    def get_instance(CanoeCrypter_cls): 
        h = CanoeCrypter_cls()
        return h
        

    def _pad_to(self, s:str, multiple:int) -> str:
        """
        As this function is not a part of the public interface to the class, 
        there is no real need to check its arguments.
        """
        return (s + 
            (multiple - len(s) % multiple) * 
            chr(multiple - len(s) % multiple)
            )

    def _slice_off_padding(self, s:str) -> str:
        """
        As this function is not a part of the public interface to the class, 
        there is no real need to check its arguments.
        """
        pad_len = s[-1]
        return s[:len(s)-pad_len]


    def encrypt(self, s:str) -> str:
        """
        Encrypt an initialization vector + the message padded to the correct
        length. Then base64 it for printing safety.
        """
        padded_input    = self._pad_to(s, CanoeCrypter.block_size)
        iv              = os.urandom(CanoeCrypter.block_size)
        engine          = AES.new(self.key, AES.MODE_CBC, iv)

        return base64.b64encode(iv + engine.encrypt(padded_input))


    def decrypt(self, s:str) -> str:
        """
        Base64 decode the string, then strip off the initialization 
        vector and decrypt the remainder.
        """
        t = base64.b64decode(s)
        iv      = t[:CanoeCrypter.block_size]
        message = t[CanoeCrypter.block_size:]
        engine  = AES.new(self.key, AES.MODE_CBC, iv)

        return self._slice_off_padding(engine.decrypt(message))


@trap
def default(mode:int=0) -> CanoeDB:
    """
    Hack to use the CanoeDB object to connect to the database defined
    in the /sw/canoe/databases/canoelog.db file in one no-argument method.  
    """

    verbose = mode > 0
    try:
        #constructor will use the information in the dynamic db property of the object.
        return CanoeDB('/sw/canoe/databases/canoelog.db')
        

    except Exception as e:
        uu.tombstone("unable to open default database.")
        sys.exit(os.EX_UNAVAILABLE)

