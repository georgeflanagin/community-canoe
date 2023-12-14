# -*- coding: utf-8 -*-
"""
Generic encapsultion of the encryption processes within Canoe. This
module may be easily replaced in the future.
"""

import base64
from   Crypto.Cipher import AES
import random
import os
import sys

import canoeobject as co
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

the_pad = [
    'igZmTew1IbYt/Wx5/vfuEImmBjdMtaguEqRLUraB8uITnniZGEq5fxQaiscFPuvc',
    'VnGsXMdBseqIHVbEyVN7A/y3m2rIbG4mDT23h2SXGIhJBBgRAgAJBQI/8KT2AhsM',
    'AAoJENldNQaAnRbduoYAoOTRuxas8/81ErzmvBu6VmdrRQuDAKDFo/wLzz6HVS7g',
    'K3By49i9iTi5t5kBDQRWMiW0AQgAuvmiEu+QZk6SvOLX4B38aLDTelJ1/6m+L9zH',
    'azYXl4DC3jvpveIBGrI17vxxS4XAwhWKxWnqs5HulzBFDDYJ58Jfo4usKhspaJS7',
    'lai9OG33h6zVOtWJB5sD6guFbc8LYsb3XvAIT9b+h73OgM8T5vOQmjhcPbCngddm',
    'kspbFBu/JuBW42Et30xF/38O0Mq+tSQpc9aE1U+wbeGRrF2iM3ZkXdL6NMfGChwk',
    'VgAEnPZG9a/2b18beUU232eH+P/zTvhom9J7LRwVrMJd5aD/Ga85X0noX6Mf699/',
    'Xe+TIFd7yMa3iSFZ6N0vB35K+BkKK/EFjasEBPpj+febBS2rUwARAQABtFhTQ0xv',
    'Z2ljUmljaG1vbmRQdWJsaWNLZXkgKFB1YmxpYyBLZXkgZm9yIGZpbGUgdHJhbnNm',
    'ZXIgdG8gU0NMb2dpYykgPGJ3YW5uZXJAc2Nsb2dpYy5jb20+iQE5BBMBCAAjBQJW',
    'MiW0AhsDBwsJCAcDAgEGFQgCCQoLBBYCAwECHgECF4AACgkQWDQZJuNtfxs5AQf/',
    'fIXQuPgF03d3TFjmr9qe++OkIhUKxrloIHZdRYYyB2y+jhW+OmRDzjyWa9HVmyFO',
    '6REoSpSig7PQCqU+QhBtCn0u4jbQwiNTxO+CgmhS4DCsJanQDz8FLTYAIyea1oVY',
    'ReZhY4mox3aYTrPMuFFgJ1KL/apn4Z2/LNN+J5gERpPEO3McSo0ORU3ILycZHZO2',
    'z4rUm6ZVwk9KPIyvkXDvxav3tDcDm7S8sIRsBboIXYPgWH+Fg1PGyWcoXU9ScS6o'
    ]

class CanoeCrypter:
    """
    An instance of this class (operationally speaking a singleton) can be
    used to encrypt text shreds.
    """
    
    kkey = base64.b64decode('b6U7hQGWrvA44E3VQqV1svRPzqriUI4ylwVdwZNRwIw=')
    block_size = 16
    key_size = 256

    def __init__(self, key: str=None):
        """
        This function should be treated as a protected constructor, and almost
        all uses of this class should call CanoeCrypter.get_instance() instead.
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
        if len(key) != 44:
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
        

    # def decript(self, s:str) -> str: return 'sp1derone'
    def decript(self, s:str) -> str: return 'ysv7kjfxLS+ZUEiC'
    # def decript(self, s:str) -> str: return 'c4k34i[f5nBNor_['


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


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: canoecrypter.py {text to be encrypted}")
        exit(0)

    test_key = base64.b64encode(os.urandom(32))
    # print("Using key = " + str(CanoeCrypter.kkey))
    o = CanoeCrypter()
    
    encrypted_name = o.encrypt(" ".join(sys.argv[1:]))
    print("Encrypted datum is " + str(encrypted_name) + 
          ' padded to length ' + str(len(encrypted_name)))

    decrypted_name = o.decrypt(encrypted_name)
    print("Decrypted datum is " + str(decrypted_name))
else:
    pass
