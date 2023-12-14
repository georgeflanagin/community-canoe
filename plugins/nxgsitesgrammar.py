# -*- coding: utf-8 -*-
""" 
This grammar file is associated with the data used for the 'nxg-vendors' datafeed,
particularly the Sites.  
"""

import typing
from   typing import *

import datetime
import random

import tombstone as tomb
import urutils as uu

##########################################
# From the VIEW NXGSITE in Banner        #
##########################################

oracle_column_names = [
    "UID","ErpId","ParentUID","ModifiedDate","ModifiedByUserId",
    "State","SupplierName","LocationLine1","LocationLine2","LocationLine3",
    "LocationLine4","LocationPostCode","BankName","AccountName",
    "AccountNr","SortCode","IBAN","SWIFT","Currency"
]

# Other things we want to add to the XML. Note that "pseudo_columns" is 
# a well known name, so don't use something else.

pseudo_columns = tuple()

# tag_data is also a well known name, and is used by the rules engine
# in XML.py
all_columns = (*oracle_column_names, *pseudo_columns)
tag_data = uu.SloppyDict(
    zip(
        (*all_columns, 'global_impute'),
        [ uu.SloppyTree() for _ in range(len(all_columns)) ]
        )
    )    


file_data = uu.SloppyTree()

# This is the name of the outermost tag.
file_data.frame_name = 'Sites'

# The name of each "thing" that is wrapped by the outermost tag.
file_data.row_name = 'Site'

file_data.encoding = 'utf-8'
file_data.nodotzero = False

do_not_impute = oracle_column_names


#################################################################
# ************     This is the grammar.     ******************* #
#################################################################
# Briefly, these are rules. 
#   1. The column name from the view is a key in tag_data.
#   2. "tag" means the token that follows is the name of an 
#       inner tag.
#   3. "attribute" means that the following token is the name
#       of a attribute for the preceding tag. "= something" tells
#       us the value of the attribute.
#   4. "value" means that some kind of value is to be placed in
#       this terminal position. It can be one of three things:
#    a. if nothing follows "value", then this is where we put a
#           datum obtained from the view.
#    b. if "value" is followed by "= something", then it is just
#           like the attribute processing, and we put the string
#           representation of the literal that follows the =. Note
#           that the literal can be of any type.
#    c. finally, "value" can be a lambda function, in which case
#           the rules engine will evaluate the expression at 
#           runtime. This allows us to do things such as 
#           referencing today.
################################################################

tag_data.global_impute = "missing_data"

# For this view's translation, we are not renaming any of the 
# columns/tags. We are going to impute UID and ParentUID, and
# we can fix them up in a second step.

tag_data.UID.tag.UID.value = lambda : int( random.random() * 100000000 )
tag_data.ErpId.tag.ErpId.value
tag_data.ParentUID.tag.ParentUID.value
tag_data.ModifiedDate.tag.ModifiedDate.value
tag_data.ModifiedByUserId.tag.ModifiedByUserId.value
tag_data.State.tag.State.value
tag_data.SupplierName.tag.SupplierName.value
tag_data.LocationLine1.tag.LocationLine1.value
tag_data.LocationLine2.tag.LocationLine2.value
tag_data.LocationLine3.tag.LocationLine3.value
tag_data.LocationLine4.tag.LocationLine4.value
tag_data.LocationPostCode.tag.LocationPostCode.value
tag_data.BankName.tag.BankName.value
tag_data.AccountName.tag.AccountName.value
tag_data.AccountNr.tag.AccountNr.value
tag_data.SortCode.tag.SortCode.value
tag_data.IBAN.tag.IBAN.value
tag_data.SWIFT.tag.SWIFT.value
tag_data.Currency.tag.Currency.value

