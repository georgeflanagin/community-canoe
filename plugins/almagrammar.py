# -*- coding: utf-8 -*-
""" 
This grammar file is associated with the data used for the 'alma' datafeed.
"""

import typing
from   typing import *

import datetime

import tombstone as tomb
import urutils as uu

##########################################
# From the VIEW v_alma_patron in Banner. #
##########################################

# This list should correspond to the names from the database SELECT statement.
oracle_column_names = ( "PIDM", "UR_ID", "USER_GROUP", "STATUS", "NET_ID",
    "FIRST_NAME", "LAST_NAME", "MIDDLE_INITIAL", "EMAIL_ADDRESS", "PHONE_NUMBER",
    "ADDRESS_LINE_1", "ADDRESS_LINE_2", "ADDRESS_LINE_3", "ADDRESS_CITY", "ADDRESS_STATE",
    "ADDRESS_ZIP", "ADDRESS_NATION" ) 

# Other things we want to add to the XML. Note that "pseudo_columns" is 
# a well known name, so don't use something else.
pseudo_columns = ('expiry_date', 'purge_date')

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
file_data.frame_name = 'users'

# The name of each "thing" that is wrapped by the outermost tag.
file_data.row_name = 'user'

file_data.encoding = 'utf-8'
file_data.nodotzero = False

"""
Documentation from emails 28 May through 2 June 2020.

For address and phone
 
PE = Permanent
DP = Departmental
CM = Campus
LA = Local
 
Preference order for FacStaff address and phone is DP, PE
Preference order for Student address and phone is CM, LA, PE

Street address type needs to be one of these options:
 
·         home
·         work
·         school
·         alternative


Phone number options are:
 
·         Home
·         Mobile
·         Office
·         Office fax
"""

address_codes = uu.SloppyDict({
    "PE":"Home",
    "DP":"Work",
    "CM":"School",
    "LA":"Home"
    })

phone_codes = uu.SloppyDict({
    "PE":"Home",
    "DP":"Office",
    "CM":"Office",
    "LA":"Office"
    })



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

do_not_impute = ('PIDM', 'UR_ID', 'USER_GROUP', 
    'NET_ID',
    'ADDRESS_STATE', 'ADDRESS_NATION', 
    'ADDRESS_LINE_2', 'ADDRESS_LINE_3')

tag_data.global_impute = "missing_data"
tag_data.ADDRESS_ZIP.impute = '00000-0000'
tag_data.EMAIL_ADDRESS.impute = 'nobody@nowhere.org'
tag_data.FIRST_NAME.impute = 'NoFirstName'
tag_data.MIDDLE_INITIAL.impute = 'None'
tag_data.LAST_NAME.impute = 'NoLastName'
tag_data.PHONE_NUMBER.impute = '8885550000'
tag_data.ADDRESS_LINE_1.impute = 'No Street Address'
tag_data.ADDRESS_CITY.impute = 'No City'

tag_data.ADDRESS_CITY.tag.contact_info.tag.addresses.tag.address.attribute.preferred = 'true'
tag_data.ADDRESS_CITY.tag.contact_info.tag.addresses.tag.address.attribute.segment_type = 'External'
tag_data.ADDRESS_CITY.tag.contact_info.tag.addresses.tag.address.tag.city.value
tag_data.ADDRESS_LINE_1.tag.contact_info.tag.addresses.tag.address.tag.line1.value

tag_data.ADDRESS_LINE_2.tag.contact_info.tag.addresses.tag.address.tag.line2.value
tag_data.ADDRESS_LINE_3.tag.contact_info.tag.addresses.tag.address.tag.line3.value
tag_data.ADDRESS_NATION.tag.contact_info.tag.addresses.tag.address.tag.country.value
tag_data.ADDRESS_STATE.tag.contact_info.tag.addresses.tag.address.tag.state_province.value
tag_data.ADDRESS_ZIP.tag.contact_info.tag.addresses.tag.address.tag.postal_code.value

tag_data.ADDRESS_ZIP.tag.contact_info.tag.addresses.tag.address.tag.address_types.tag.address_type.attribute.desc = 'Work'
tag_data.ADDRESS_ZIP.tag.contact_info.tag.addresses.tag.address.tag.address_types.tag.address_type.value = 'work'

# tag_data.ADDRESS_TYPE.tag.contact_info.tag.addresses.tag.address.tag.address_types.tag.address_type.attribute.desc = lambda x: address_codes[x]

tag_data.EMAIL_ADDRESS.tag.contact_info.tag.emails.tag.email.tag.email_address.value
tag_data.EMAIL_ADDRESS.tag.contact_info.tag.emails.tag.email.attribute.preferred = 'true'
tag_data.EMAIL_ADDRESS.tag.contact_info.tag.emails.tag.email.attribute.segment_type = 'External'
tag_data.EMAIL_ADDRESS.tag.contact_info.tag.emails.tag.email.tag.email_types.tag.email_type.attribute.desc = 'School'
tag_data.EMAIL_ADDRESS.tag.contact_info.tag.emails.tag.email.tag.email_types.tag.email_type.value = 'school'


tag_data.FIRST_NAME.tag.first_name.value

tag_data.LAST_NAME.tag.last_name.value

tag_data.MIDDLE_INITIAL.tag.middle_name.value

# Note that you don't need to have rules at all for items that will
# not become a part of the XML.
# tag_data.NET_ID.tag = None

tag_data.PHONE_NUMBER.tag.contact_info.tag.phones.tag.phone.tag.phone_number.value
tag_data.PHONE_NUMBER.tag.contact_info.tag.phones.tag.phone.attribute.preferred = 'true'
tag_data.PHONE_NUMBER.tag.contact_info.tag.phones.tag.phone.attribute.segment_type = 'External'
tag_data.PHONE_NUMBER.tag.contact_info.tag.phones.tag.phone.tag.phone_types.tag.phone_type.value = 'office'
tag_data.PHONE_NUMBER.tag.contact_info.tag.phones.tag.phone.tag.phone_types.attribute.desc = 'Office'


# tag_data.PIDM.tag = None
# tag_data.STATUS.tag = None
tag_data.UR_ID.tag.primary_id.value
tag_data.USER_GROUP.tag.user_group.value

tag_data.expiry_date.tag.expiry_date.value = lambda : str(datetime.date.today() + datetime.timedelta(days=180))[:10]
tag_data.purge_date.tag.purge_date.value = lambda : str(datetime.date.today() + datetime.timedelta(days=365))[:10]

