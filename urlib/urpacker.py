# -*- coding: utf-8 -*-

import typing
from   typing import *

import bz2
import json
import math
import os
import pickle
import sys
import tempfile

import cx_Oracle
import pandas

# Canøe imports

import fname
import grammar
from   grammar import *
from   urdecorators import show_exceptions_and_frames as trap
import urutils as uu

# Credits
__longname__ = "University of Richmond"
__acronym__ = " UR "
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2019, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Prototype'

__license__ = 'MIT'
import license

def timestamp_to_str(o:object) -> object:
    if isinstance(o, cx_Oracle.Timestamp):
        return str(o)
    return o


def set_to_list(o:object) -> object:
    if isinstance(o, (set, frozenset)): 
        return list(o)
    return o


def enum_to_int(e:object) -> object:
    if isinstance(e, grammar.ERROR_ACTION):
        return int(e)
    return e


built_in_hooks = [enum_to_int, set_to_list, timestamp_to_str]
null_key = bytes.fromhex('00')

class URpacker: 
    pass

class URpacker:
    """
    Class to read and write packed files without having to
    know much about how to do it. Effectively, this class restricts
    the options available, provides the hooks for our non-standard
    data types, and handles the exceptions that are raised.
    """
    
    super_modes = {
        "create":"wbx",
        "read":"rb",
        "write":"wb",
        "append":"ab"
        }

    
    @staticmethod
    def null_cryptor(b_data:bytes, *,
        i_vec:bytes=b'', 
        key:bytes=null_key) -> bytes:
        """
        This function is for testing the logic. It is not 
        intended to make anything secure.
        """

        chunk_size = len(key)
        n_chunks = math.ceil(len(i_vec)+len(b_data))/chunk_size
        out_data = bytearray(n_chunks*chunk_size)
        in_data  = i_vec + bdata + bytearray(len(out_data)-len(in_data))

        for i in range(n_chunks):
            for j in range(chunk_size):
                out[i*chunk_size+j] = in_data[i*chunk_size+j] ^ key[j]

        out.append(len(b_data)) 
        out.append(len(i_vec))


    @staticmethod
    def null_decryptor(b_data:bytes, *,
        key:bytes=null_key) -> bytes:

        pass



    def __init__(self, *,
        encoding:str='utf-8',
        hooks:list=[]) -> None:
        """
        encoding -- essential to converting bytes to strings
        hooks -- a list of functions to do serializations for types
            not supported in the default packing. These are appended to
            the default list of conversions.
        """

        self.encoding = encoding
        self.unit = None
        self.name = None
        self.hooks = built_in_hooks + hooks
        

    def _hooks(self, o:object) -> object:
        """
        Execute the hooks.
        """
        for fcn in self.hooks:
            o_prime = fcn(o)
            if id(o_prime) == id(o): continue
            return o_prime
        return o


    @trap
    def attachIO(self,
        filename:str, *,
        mode:str='w+b',
        s_mode:str=None) -> bool:
        """
        Open a unit of some kind for use by our program. 

        filename   -- unit for the I/O operations
        mode       -- how to open the unit. The default is read/write,
                        with no truncation for an existing unit.
        s_mode     -- if present, overrides any mode values.
        klobber    -- whether or not to continue if the file exists.

        returns    -- true on success, false otherwise.
        """

        self.name = filename
        if s_mode is not None:
            mode = URpacker.super_modes.get(s_mode, mode)            

        try:
            self.unit = open(filename, mode)
            return True

        except FileExistsError as e:
            uu.tombstone(f'Cannot create {filename} because it already exists.')

        except FileNotFoundError as e:
            uu.tombstone(f'File {filename} does not exist.')

        except PermissionError as e:
            if 'r' in mode:
                uu.tombstone(f'File {filename} exists, but you have no rights to it.')
            else:
                uu.tombstone(f'You cannot write to {filename}')

        return False


    @trap
    def write(self, o:object, *, 
            show_stats:bool=False,
            object_code:bytes=None) -> bool:
        """
        serialize the argument, and write the serialization to the 
        current file and close the file.

        o           -- the Python object to be written.
        show_stats  -- whether to write the progress/results to stderr.
        object_code -- whether the object being written is Canøe's object code.

        returns -- true on success, false otherwise.
        """
        try:
            it = pickle.dumps(o)
            show_stats and uu.tombstone(f'pickled {len(it)} bytes.')
            it = bz2.compress(it)
            show_stats and uu.tombstone(f'BW Xform to {len(it)} bytes.')
            
            if object_code is not None:
                it = object_code + it[10:]
                show_stats and uu.tombstone(f'Canoe version: {object_code}')
            
        except pickle.PicklingError as e:
            uu.tombstone(uu.type_and_text(e))
            return False   

        except Exception as e:
            uu.tombstone(uu.type_and_text(e))
            return False

        x = 0
        try:          
            x = self.unit.write(it)
            return True

        except Exception as e:
            uu.tombstone(uu.type_and_text(e))
            return False   

        finally:
            self.unit.close()
            self.unit = None

    
    @trap
    def read(self, format:str='python') -> object:
        """
        Read and unpack the info in the object in the unit.

        format  -- how to present the returned data object. The 
            options are 'python' and 'pandas'. Not everything
            can be transformed to a pandas.DataFrame

        returns -- the decoded contents or None. Raises an
            Exception on an unsupported data format. 
        """
        format = format.lower()
        if format not in ('raw', 'python', 'pandas'):
            raise Exception(f'unsupported data {format=}')

        if self.unit is None:
            uu.tombstone('No unit attached.')
            return None

        self.unit.seek(0, 0)
        as_read_data = self.unit.read()
        header = as_read_data[:20]

        # If it is a raw read, we hope the caller knows what to
        # do with the data.
        if format == 'raw': return as_read_data
        data = None

        try:
            is_obj_code, data = uu.is_canoe_code(as_read_data)
            new_header = data[:20]
            # print(f"{is_obj_code=} {new_header=}") 
            if is_obj_code:
                data = bz2.decompress(data)
            else:
                data = bz2.decompress(as_read_data)

            try:
                pyobj = pickle.loads(data)
                if format == 'python': 
                    return pyobj
                elif format == 'pandas': 
                    return pandas.DataFrame.from_dict(pyobj)
                else:
                    raise Exception(f"This should never happen: {format=}")

            except Exception as e:
                uu.tombstone(f"The data in {self.name} were improperly pickled.")

        except Exception as e:
            uu.tombstone(f"{str(e)}")
            uu.tombstone(f"Found uncompressed data in {self.name}")
            return as_read_data

        finally:
            self.unit.close()
            self.unit = None
            
        return data if data else as_read_data


if __name__ == '__main__':

    import argparse

    p = argparse.ArgumentParser('read and write URpacked files.')

    p.add_argument('--raw-input', type=str, default="")
    p.add_argument('-i', '--input', type=str, default="")
    p.add_argument('-o', '--output', type=str, default='')

    opts = p.parse_args()

    if not opts.input or opts.raw_input:
        print('At least one of --input and --raw-input is required.')
        sys.exit(os.EX_DATAERR)

    ###
    # If we have raw input, the output is a packed version of the same.
    # If we have ordinary input, we assume it is packed, and the output 
    # the unpacked version of the same.
    ###
    if not opts.output: 
        opts.output = ( f"{opts.input}.jsc" 
            if opts.raw_input 
            else f"{opts.input}.unpacked" )

    uu.dump_cmdline(opts, True)


    data = None
    p = URpacker()

    if opts.raw_input:
        with open(opts.raw_input) as f:
            data = f.read()
        p.attachIO(opts.output, s_mode='write')
        result = p.write()

    else:
        p.attachIO(opts.input, s_mode='read')
        data = p.read('python')
        with open(opts.output, 'w') as f:
            f.write(str(data))

    sys.exit(os.EX_OK)
