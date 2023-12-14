# -*- coding: utf-8 -*-
""" 
A plugin to scrub Credit Card numbers from XML files from JPMC.
"""

import typing
from   typing import *

import os
import os.path
import sys
import xml.etree.ElementTree as ET

# Installed imports

import pandas

# Canoe imports

import csv
import fname
from   grammar import *
import hashlib
import hop
import pluginlib
import tombstone as tomb
import urbox as ux
import urdb
import urpacker
import urutils as uu

if not uu.in_production():
    from urdecorators import show_exceptions_and_frames as trap
else:
    from urdecorators import null_decorator as trap

__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2019, University of Richmond'
__credits__ = None
__version__ = '0.9'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'testable'

__license__ = 'MIT'
import license

@trap
def xmlscrub_main(opcodes:uu.SloppyDict) -> ERROR_ACTION:

# these are the nodes to remove.
    removals = [
        "/".join(['IssuerEntity', 'CorporateEntity', 'AccountEntity', 'FinancialTransactionEntity', 'FinancialTransaction_5000', 'AlternateAccount']),
        "/".join(['IssuerEntity', 'CorporateEntity', 'AccountEntity', 'FinancialTransactionEntity', 'FinancialTransaction_5000', 'AlternateAccount2']),
        "/".join(['IssuerEntity', 'CorporateEntity', 'AccountEntity', 'AccountInformation_4300', 'AlternateAccount']),
        "/".join(['IssuerEntity', 'CorporateEntity', 'AccountEntity', 'AccountInformation_4300', 'AlternateAccount2']),
        "/".join(['CDFTransmissionFile', 'IssuerEntity', 'CorporateEntity', 'AccountEntity', 'FinancialAdjustmentRecord_5900', 'ReversalFlag']),
        "/".join(['IssuerEntity', 'CorporateEntity', 'AccountEntity', 'FinancialAdjustmentRecord_5900', 'AlternateAccount']),
        "/".join(['IssuerEntity', 'CorporateEntity', 'AccountEntity', 'FinancialAdjustmentRecord_5900', 'AlternateAccount2'])
    ]


    # these are the text shreds/attributes to hash.
    hashables = {
        "/".join(['IssuerEntity', 'CorporateEntity', 'AccountEntity', 'FinancialTransactionEntity', 'FinancialTransaction_5000', 'ProcessorTransactionId']) : None,
        "/".join(['IssuerEntity', 'CorporateEntity', 'AccountEntity', 'FinancialTransactionEntity']) : 'ProcessorTransactionId',
        "/".join(['IssuerEntity', 'CorporateEntity', 'AccountEntity', 'FinancialAdjustmentRecord_5900', 'ProcessorTransactionId']) : None
        }

    # tomb.tombstone("begin")
    files = uu.build_file_list(opcodes.input) 

    # For each XML file.
    for i, f in enumerate(files):
        try:
            tomb.tombstone('Reading {}'.format(f))
            tree = ET.parse(f)

        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            tomb.tombstone('{} is not a valid XML file.'.format(f))
            # Might as well try the next one and ignore this mistake.
            continue

        tomb.tombstone('{} is open and parsed.'.format(f))
    
        root = tree.getroot()
        for n in removals:
            tomb.tombstone("removing nodes of type {}".format(n))
            nodes = root.findall(n)
            for _ in nodes:
                if opcodes.debug: print(_.text)
                _.clear()

        for n, a in hashables.items():
            tomb.tombstone("hashing items of type {}->{}".format(n,a))
            nodes = root.findall(n)
            for _ in nodes:
                hasher = hashlib.sha1()
                if a is not None:
                    if opcodes.debug: print(_.attrib[a])
                    hasher.update(_.attrib[a].encode('utf-8'))
                    _.attrib[a] = hasher.hexdigest()
                else:
                    if opcodes.debug: print(_.text)
                    hasher.update(_.text.encode('utf-8'))
                    _.text = hasher.hexdigest()
                    
        root.insert(0, ET.Comment('This file was edited by Canoe at {}'.format(uu.now_as_string())))
        edited_filename = '{}/mastercard.{}.{}.xml'.format(opcodes.output, uu.now_as_string()[:10], i) 
        tomb.tombstone('writing {}'.format(edited_filename))
        tree.write(edited_filename)
                   
    return ERROR_ACTION.proceed


if __name__ == '__main__':
    """
    Universal test program for all Canøe plugins.
    """
    if len(sys.argv) < 2: sys.exit(os.EX_DATAERR)

    loader = urpacker.URpacker()
    loader.attachIO(sys.argv[-1], s_mode='read')
    opcodes = uu.deepsloppy(loader.read())
    if not opcodes:
        tomb.tombstone("{} was not a Canøe program.".format(sys.argv[-1]))
        sys.exit(os.EX_DATAERR)

    tomb.tombstone("\n")
    tomb.tombstone("Compiler version {}".format(uu.compiler_info(opcodes)))
    tomb.tombstone("Compiled on      {}".format(uu.compiled_time(opcodes)))
    tomb.tombstone(80*'-')

    # The following awkwardness allows us to use the same test program for every
    # plugin in the virtual machine.
    # 
    # Basic name of this file, which corresponds to the name of the plugin/executive.
    vm_function = os.path.basename(__file__)[:-3]

    # The _main function's name
    vm_callable = "{}_main".format(vm_function)

    # Get our opcodes from the compiled program because these opcodes have
    # the same name as the plugin, and execute them.
    # 
    try:
        error_action = globals()[vm_callable](opcodes[vm_function])
        tomb.tombstone("ERROR_ACTION is {}".format(ERROR_ACTION(error_action).name))
        sys.exit(os.EX_OK if error_action is ERROR_ACTION.proceed else os.EX_DATAERR)

    except KeyError as e:
        tomb.tombstone("The program {} has no opcodes for the {} operation.".format(sys.argv[-1], vm_function))

    except Exception as e:
        tomb.tombstone(uu.type_and_text(e))

    finally:
        tomb.tombstone(80*'=')
    
    
