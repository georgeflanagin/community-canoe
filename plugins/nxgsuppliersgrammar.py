# -*- coding: utf-8 -*-
""" 
This grammar file is associated with the data used for the 'nxg-vendors' datafeed,
particularly the Suppiers. These are the top-level elements that we used to think
of as the Vendors.
"""

import typing
from   typing import *

import datetime

import tombstone as tomb
import urutils as uu

##########################################
# From the VIEW NXGSITE in Banner        #
##########################################


oracle_column_names = [
    'UID', 'ParentUID', 'ErpId', 'ModifiedDate', 'ModifiedByUserID', 'SupplierName',
    'CompaniesHouseRef', 'AlternateLicenseNumber', 'VATNumber', 'State', 'LocationLine1',
    'LocationLine2', 'LocationLine3', 'LocationLine4', 'LocationPostCode' 
    ]

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
file_data.frame_name = 'Suppliers'

# The name of each "thing" that is wrapped by the outermost tag.
file_data.row_name = 'Supplier'

file_data.encoding = 'utf-8'
file_data.nodotzero = False

do_not_impute = oracle_column_names

#################################################################
# ************     This is the grammar.     ******************* #
#################################################################

tag_data.global_impute = "missing_data"

# For this view's translation, we are not renaming any of the 
# columns/tags. We are going to impute UID and ParentUID, and
# we can fix them up in a second step.


tag_data.UID.tag.UID.value
tag_data.ParentUID.tag.ParentUID.value
tag_data.ErpId.tag.ErpId.value
tag_data.ModifiedDate.tag.ModifiedDate.value
tag_data.ModifiedByUserID.tag.ModifiedByUserId.value
tag_data.SupplierName.tag.SupplierName.value
tag_data.CompaniesHouseRef.tag.CompaniesHouseRef.value
tag_data.AlternateLicenseNumber.tag.AlternateLicenceNumber.value
tag_data.VATNumber.tag.VATNumber.value
tag_data.State.tag.State.value
tag_data.LocationLine1.tag.LocationLine1.value
tag_data.LocationLine2.tag.LocationLine2.value
tag_data.LocationLine3.tag.LocationLine3.value
tag_data.LocationLine4.tag.LocationLine4.value
tag_data.LocationPostCode.tag.LocationPostCode.value
