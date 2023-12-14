# -*- coding: utf-8 -*-
""" 
A generalized XML extractor.
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

import canoestats
from   canoestats import LED
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

# Renaming maps.

new_column_names = {
    'PostingDate':'POSTINGDATE',
    'TransactionDate':'TRANSACTIONDATE',
    'DebitOrCreditIndicator':'DEBITORCREDITINDICATOR',
    'AmountInBillingCurrency':'AMOUNTINBILLINGCURRENCY',
    'CardAcceptorName':'CARDACCEPTORNAME',
    'EmployeeId':'EMPLOYEEID',
    'MasterCardFinancialTransactionId':'PROCESSTRANSID',
    'AccountNumber':'ALTERNATEID',
    'hashvalue':'UNIQUEID'
    }

new_5900_column_names = {
    'PostingDate':'POSTINGDATE',
    'TransactionDate':'TRANSACTIONDATE',
    'DebitOrCreditIndicator':'DEBITORCREDITINDICATOR',
    'AmountInBillingCurrency':'AMOUNTINBILLINGCURRENCY',
    'AdjustmentDescription':'CARDACCEPTORNAME',
    # 'EmployeeId':'EMPLOYEEID',
    'MasterCardFinancialTransactionId':'PROCESSTRANSID',
    # 'AccountNumber':'ALTERNATEID',
    'hashvalue':'UNIQUEID'
    }

# These nodes have to be scrubbed from the FinancialAdjustmentRecord_5900 nodes
# because they are sometimes empty, or contain a variable number of sub-items.
t_5900_removals = ['FinancialRecordHeader', 'AlternateAccount', 'AlternateAccount2', 'ReversalFlag']

@trap
def cr_mastercard_main(opcodes:uu.SloppyDict) -> ERROR_ACTION:

    global new_column_names
    global new_5900_column_names
    global t_5900_removals
    myname = uu.name_from_dirname(opcodes.local_dir)
    mytype = 'xforms_custom'
    stats = canoestats.default()
    stats.update(myname, mytype, LED.ON)

    # tomb.tombstone("begin")
    files = uu.build_file_list(uu.path_join(opcodes.local_dir, opcodes.file))
    if pluginlib.test_empty(files, opcodes.on_error):
        stats.update(myname, mytype, LED.GREEN)
        return ERROR_ACTION.stop

    num_exceptions = 0

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
        # get the list of credit cards.
        # AccountNumber is the attribute, and the stuff we are skipping just identifies
        # it as our report. 
        credit_cards = root.findall('IssuerEntity/CorporateEntity/AccountEntity')
        tomb.tombstone("This file contains info about {} credit cards.".format(len(credit_cards)))

        # We will fill these lists as we go.
        bad = []
        all_transactions = []
        all_adjustments = []

        # So for all the cards in this file ...
        for credit_card in credit_cards:
            cc_number = credit_card.attrib['AccountNumber']
            # tomb.tombstone("Processing AccountNumber {}".format(cc_number))

            # We need these sibling level records. The first is what we generally 
            # think of as a "charge," and the second is an adjustment to a previously
            # completed transaction.
            t_big = credit_card.findall('FinancialTransactionEntity')
            t_5900 = credit_card.findall('FinancialAdjustmentRecord_5900')

            # FinancialTransactionEntity records.
            for t1 in t_big:
                xaction = {'AccountNumber':cc_number}
                # xaction.update({ _.tag.strip() : _.text.strip() for _ in t1.findall('AlternateAccount/AlternateAccountNumber')})
                try:
                    # We need some of the nodes below; and it is easier to get them all.
                    xaction.update({ _.tag.strip() : _.text.strip() for _ in t1.findall('FinancialTransaction_5000/*')})
                    xaction.update({ _.tag.strip() : _.text.strip() for _ in t1.findall('CardAcceptor_5001/*') })
                    all_transactions.append(xaction)  

                except Exception as e:
                    tomb.tombstone(uu.type_and_text(e))
                    tomb.tombstone("bad node associated with {}".format(cc_number))
                    bad.append(cc_number)
                    pass

            # FinancialAdjustmentRecord_5900
            for t1 in t_5900:
                xaction = {'AccountNumber':cc_number}
                # xaction.update({ _.tag.strip() : _.text.strip() for _ in t1.findall('AlternateAccount/AlternateAccountNumber')})
                try:
                    # Unfortunately, these seem to have (possibly) empty nodes that
                    # must be removed so that we can parse the rest of them. Not all 
                    # files have these.
                    for _ in t_5900_removals: 
                        try:
                            t1.remove(t1.find(_))
                        except:
                            pass

                    # The .update() is not really needed. We could do it with an assignment 
                    # statement alone, but requirements may change. 
                    xaction.update({ _.tag.strip() : _.text.strip() for _ in t1.findall('*')})
                    all_adjustments.append(xaction)

                except Exception as e:
                    tomb.tombstone(uu.type_and_text(e))
                    tomb.tombstone('bad 5900 node associated with {}'.format(cc_number))
                    bad.append(cc_number)
                    pass

        # Let the logfile know the good and bad news.
        tomb.tombstone("{} good nodes".format(len(all_transactions)))
        tomb.tombstone("{} bad nodes".format(len(bad)))

        # The data columns that populate Banner could be repetitions of each
        # other. So we have to use the plenum data to construct a hash to use as
        # a unique key.
        plenum_frame = pandas.DataFrame(all_transactions)
        plenum_frame['hashvalue'] = pandas.Series('', dtype=object, index=plenum_frame.index)
        for j, row in plenum_frame.iterrows():
            hasher = hashlib.sha1()
            hasher.update("".join([str(_) for _ in row]).encode('utf-8'))
            plenum_frame['hashvalue'][j] = hasher.hexdigest()
        
        # for debugging, or a later change in plans.
        plenum_frame.to_csv('/tmp/'+str(j)+'.plenum.mc.csv', index=False)

        # Same thing as we did above. 
        if len(all_adjustments):
            plenum_5900_frame = pandas.DataFrame(all_adjustments)
            plenum_5900_frame['hashvalue'] = pandas.Series('', dtype=object, index=plenum_5900_frame.index)
            for j, row in plenum_5900_frame.iterrows():
                hasher = hashlib.sha1()
                hasher.update("".join([str(_) for _ in row]).encode('utf-8'))
                plenum_5900_frame['hashvalue'][j] = hasher.hexdigest()
            plenum_5900_frame['AmountInBillingCurrency'] = plenum_5900_frame['AmountInBillingCurrency'].astype(float).divide(10000)
            plenum_5900_frame.to_csv('/tmp/{}.{}.mc.csv'.format(i, 5900), index=False) 

        # There are a lot more regular transactions than adjustments, so
        # let's make a separate frame for more direct debugging.
        smaller_frame = plenum_frame.filter(list(new_column_names.keys()))
        smaller_frame.columns = list(new_column_names.values())
        smaller_frame.to_csv(uu.path_join(opcodes.local_dir, 'tempfile.csv'), index=False)
        smaller_frame['AMOUNTINBILLINGCURRENCY'] = smaller_frame['AMOUNTINBILLINGCURRENCY'].astype(float).divide(10000)

        smaller_frame.to_csv('/tmp/'+str(i)+'.mc.csv', index=False)


        # Open the database
        db = urdb.URdb(opcodes.db)

        # Load the regular records.
        SQL = "insert into {} ({}, {}, {}, {}, {}, {}, {}, {}, {}) ".format(
            opcodes.table, *(list(new_column_names.values()))
            )
        SQL += "values ({}, {}, {}, {}, {}, {}, {}, {}, {})"

        for row_num, row in smaller_frame.iterrows():
            try:
                SQL1 = SQL.format(*[uu.q1(str(_)) for _ in row.values])
                db.execute_SQL(SQL1)
                tomb.tombstone("row {} inserted!".format(row_num))
            except Exception as e:
                num_exceptions += 1
                tomb.tombstone("row {} failed: {}".format(row_num, SQL1))
                tomb.tombstone(uu.type_and_text(e))

        # Load the adjustment records.
        if len(all_adjustments):
            SQL = "insert into {} ({}, {}, {}, {}, {}, {}, {}) ".format(
                opcodes.table, *(list(new_5900_column_names.values()))
                )
            SQL += "values ({}, {}, {}, {}, {}, {}, {})"

            plenum_5900_frame = plenum_5900_frame.filter(list(new_5900_column_names.keys()))
            for row_num, row in plenum_5900_frame.iterrows():
                try:
                    SQL1 = SQL.format(*[uu.q1(str(_)) for _ in row.values])
                    db.execute_SQL(SQL1)
                    tomb.tombstone("5900_row {} inserted!".format(row_num))
                except Exception as e:
                    num_exceptions += 1
                    tomb.tombstone("5900_row {} failed: {}".format(row_num, SQL1))
                    tomb.tombstone(uu.type_and_text(e))

    if not num_exceptions: 
        stats.update(myname, 'xforms_custom', LED.GREEN)
    else:
        stats.update(myname, 'xforms_custom', LED.YELLOW)
        
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
    
    
