# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from   typing import *

""" 
The following constants are the constants, values, and types used in 
Canøe, most prominently in the recipe compiler.
"""

# System imports
import csv
import enum
import os
import shutil
import socket
import sys

# Canøe imports

import urutils as uu

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2019, University of Richmond'
__credits__ = None
__version__ = '0.9'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Development'

__license__ = 'MIT'
import license

###
# Roughly how this file is laid out:
#
#  Part 1: Documentary data for integrations. 
#  Part 2: Globally used "types" and constants. 
#  Part 3: Compiler look-up tables.
#
# The names of everything in the grammar file are in all
# capital letters because that is the tradition in grammars
# for other compilers.
###

############################################################
### PART 1: Documentary Data
############################################################

PARKINGLOT = os.environ.get('PARKINGLOT', '/sw/canoe/parkinglot')

# NetIDs of everyone who works in AdminSys.
ADMINSYS = ['azinski', 'thawkin3', 'vgriffit', 
            'dbroome', 'lparker', 'rcargill', 
            'ralexan2', 'dwarrick',
            'owner', 'canoe' ]

# Definitions in DATA_CLASSES come from 
# https://is.richmond.edu/policies/technology/general/Data_Security_Policy%20Feb%202014.pdf
# The strategy has been approved by Troy Boroughs, VP of IT, in an email
# of 7 September 2018 at 14:02.

DATA_CLASSES = {
    "confidential":["ssn", "pci", "banking", "finaid", "medical", "passwords", "access", "taxes"],
    "restricted":["education", "employment", "salary", "urid", "dob"],
    "official":["pii"],
    "public":[]
    }
DATA_CLASS_TAGS = {}
for k, v in DATA_CLASSES.items():
    for datum in v:
        DATA_CLASS_TAGS[datum] = k

############################################################
### PART 2: Globally used types and constants.
############################################################

class ERROR_ACTION(enum.IntEnum):
    pass

class ERROR_ACTION(enum.IntEnum):
    """
    This is a control class, returned by all plugins, and defined
    in any section of any IJKL recipe/program. The several functions
    are notational saccharine to tidy the logic in plugins.
    """

    @staticmethod
    def default_name() -> str:
        return 'proceed'


    @staticmethod
    def default() -> ERROR_ACTION:
        """
        The compiler's code references this function, and the fuction
        provides the default value to populate every section of the 
        IJKL program.
        """
        return ERROR_ACTION.__members__[ERROR_ACTION.default_name()]


    @staticmethod
    def returnable(v:ERROR_ACTION) -> bool:
        """
        returns -- True iff the argument can be returned out of the
            function where it is detected.
        """
        return v not in [
            ERROR_ACTION.skip, 
            ERROR_ACTION.test_empty,
            ERROR_ACTION.crash
            ]
        

    @staticmethod
    def by_name(name:str) -> ERROR_ACTION:
        return ERROR_ACTION.__members__[name.lower()]


    @staticmethod
    def coerce(v:ERROR_ACTION) -> ERROR_ACTION:
        """
        returns -- the argument, coerced to an appropriate value.
        """
        return v if ERROR_ACTION.returnable(v) else ERROR_ACTION.proceed
            

    crash = -2
    stop = -1
    proceed = 0
    notify = 1
    skip = 2
    cleanup = 3
    retry = 4
    test_empty = 5

    # End of ERROR_ACTION class


# We do a lot of testing for the empty and its opposite is 
# handy also.
PHI = frozenset()
UNIVERSAL = uu.Universal()

############################################################
### PART 3: Tables of grammar that drive the compiler.
###
###  These are alphabetized, with the exception of
###  dependencies.
############################################################

# These are defined by Box.
BOX_KEYS_TRANSFORMS = dict.fromkeys(["client-id", "client-secret", "enterprise-id", 
            "jwt-key-id", "rsa-private-key-file", "rsa-private-key-passphrase", 
            "app-user-id"], str.__str__)
BOX_KEYS = set(BOX_KEYS_TRANSFORMS.keys())


BUNZIP2_KEYS = {"input", "output"}

CHROMEFILTER_KEYS = {'input', 'index', 'keepindex', 'sep'}

# Being tidy.
CLEANUP_OPS = [ uu.SloppyDict(
    {
        "host":"localhost", 
        "ops": [
            "delete $mydir/*asc", 
            "delete $mydir/*diag" 
        ]
    }
)]

CR_IMAGES_KEYS = {'host', 'db', 'dest_folder', 'url', 'timeout'}
CR_IMAGES_VALUES = {"", "", "~", "", 1}
CR_IMAGES_DEFAULTS = dict(zip(CR_IMAGES_KEYS, CR_IMAGES_VALUES))

CR_MASTERCARD_KEYS = {'file', 'db', 'table', 'exceptions'}
CR_MASTERCARD_VALUES = {'Transaction*{YYYY}*[0-9]', 
    '', 
    'cr_mastercard', 
    'mastercard-exceptions.{YYYYMMDD}.txt'}
CR_MASTERCARD_DEFAULTS = dict(zip(CR_MASTERCARD_KEYS, CR_MASTERCARD_VALUES))

CURL_KEYS = ( 'host', 'type' )
CURL_TYPES = ( 'BOX', 'SHAREFILE', 'OPENAPI3', 'ETHOS', 'BASIC', 'SFTP' )

DASHBOARD_KEYS = [ "db", "order", "recent", "explain", "limit", "width", "output" ]
DASHBOARD_VALUES = [ os.path.expandvars("$CANOE_HOME/canoestats.db"), "alpha",
    False, False, 1000, 95, "tempfile"]
DASHBOARD_DEFAULTS = dict(zip(DASHBOARD_KEYS, DASHBOARD_VALUES))

# The are only for an Oracle database, at least the SID part.
DB_OBJECT_KEYS = ['user', 'host', 'port', 'password', 'SID']
DB_OBJECT_KEY_TRANSFORMS = dict(zip(
    DB_OBJECT_KEYS, 
    [str.__str__, str.__str__, int.__call__, str.__str__, str.__str__]
    ))

# These will change once we move away from SQLoader.
DBLOAD_KEYS_REQ = frozenset({'csvfile', 'db', 'tables'})
DBLOAD_KEYS = frozenset(list(DBLOAD_KEYS_REQ) + ['format', 'splits', 'remap', 'badfile'])

# Every recipe must have a roster. This is the default. Cleanup will
# be appended if not specified, but an explicit roster need not 
# have cleanup as the final step.
DEFAULT_ROSTER_ORDER = ['remote_ops', 'source', 'xforms', 
    'destination', 'cleanup', 'pgpinspect']

DISCARD_THESE_KEYS = {'next_job', 'roster'}

DOCUMENTARY_SECTIONS = (
    'zzz', 'devlead', 'owner', 
    'date_offset', 'debug', 'rerun_ok', 
    'comment', 'frequency', 'schedule', 
    'allowed_environments', 'affirm' 
    )
DOCUMENTARY_VALUES = ( 
    None, ['no lead'], ['no owner'], 
    0, False, True, 
    ['Missing comment.'], 'unknown', None, 
    None, False )
DOCUMENTARY_DEFAULTS = dict(zip(DOCUMENTARY_SECTIONS, DOCUMENTARY_VALUES))


EMPTY_KEYS = ( 'lines', 'bytes', 'whitespace' )
EMPTY_VALUES = ( 1, 10, True )
EMPTY_DEFAULTS = dict(zip(EMPTY_KEYS, EMPTY_VALUES))

# Encrypt pics works from a list of IDs, locates images with the same names,
# and encrypts them.
ENCRYPTPICS_KEYS = ('input', 'images', 'column', 'publickey', 'nameprefix', 'ext')
ENCRYPTPICS_VALUES = (
    'tempfile',
    None,
    None,
    None,
    '',
    'gpg')
ENCRYPTPICS_DEFAULTS = dict(zip(ENCRYPTPICS_KEYS, ENCRYPTPICS_VALUES))

# Places to which we deliver and from which we read.
ENDPOINTS = {'box', 's3', 'sharefile', 'host', 'azure', 'curl'}
ENDPOINTS_TYPES = dict.fromkeys(ENDPOINTS, str)

# There is a little bit of a hack here on the delete=None default value.
# The default value is really True or False depending on the endpoint
# and which direction the data are moving.
ENDPOINT_KEYS = ('file', 'directory', 'overwrite', 
    'zip', 'unique', 'password', 'empty',
    'directory_alias', 'debug', 'local_dir', 'delete',
    'required', 'wait' )
ENDPOINT_VALUES = ( None, None, None, 
    None, False, None, None,
    '', None, '', None,
    None, None )
ENDPOINT_DEFAULTS = uu.SloppyDict(zip(ENDPOINT_KEYS, ENDPOINT_VALUES))

ENVIRONMENTS = {'prod','test','dev'}

FLAGS = ["rerun", "encrypted"]

# Every file has a name, a directory where it arrives, and a directive
# about whether or not it should be overwritten.
FILE_SPEC = ['file', 'directory', 'klobber']
FILE_SPEC_TYPES = dict(zip(FILE_SPEC, [str, str, bool]))

FRAMEDIFF_KEYS = set(['first', 'output1', 'output2', 'second', 'sep' ])

FREQUENCY_KEYS = ['often','hourly','daily','weekdays','weekly','monthly','manually','unknown']
FREQUENCY_VALUES = ['&', 'H', '7', '5', 'W', 'M', '!', '?'] 
FREQUENCY_TRANSLATIONS = dict(zip(FREQUENCY_KEYS, FREQUENCY_VALUES))

FUSIONPICS_KEYS = [ 'xref', 'convert_exe', 'convert_ops' ]
FUSIONPICS_VALUES = [ 'urid.and.pidm.csv', 
    shutil.which('convert'), 
    '-resize 200x200^ -despeckle -gravity center -crop 200x200+0+0' ]
FUSIONPICS_DEFAULTS = dict(zip(FUSIONPICS_KEYS, FUSIONPICS_VALUES))


GPG_EXT = 'gpg'
GPG_RECIPIENTS = ('C5A7C17D21B90C99',)
GPG_SIGNING_KEYS = ('21B90C99', )
GPG_MAX_YEARS = 5
GPG_MSG_OLDKEY = f"The key for {{}} is older than {GPG_MAX_YEARS} years."
GPG_MIN_LEN = 2048
GPG_MSG_SHORTKEY = f"The key for {{}} is less than {GPG_MIN_LEN} bits."
GPG_GRACE_PERIOD = 0.5
GPG_MSG_GETTINGOLD = f"The key for {{}} will expire in less than {GPG_GRACE_PERIOD*12} months."

# Note that GPG_OPTIONS is a list so that we can add it to
# another list of options that pertain to the integration being
# compiled.
GPG_OPTIONS = [
    '--encrypt --sign --armor --yes -u {} --batch --quiet --force-mdc --recipient {} --trust-model always '.format(*GPG_SIGNING_KEYS, *GPG_RECIPIENTS)
    ]

GRAP_KEYS = ('zipfile', 'filter', 'box_stage', 'box_backup', 'destination_dir')
GRAP_VALUES = ('*.zip', '*', None, None, None)
GRAP_DEFAULTS = dict(zip(GRAP_KEYS, GRAP_VALUES))

# Facts about CPUs to which we connect.
HOST_KEYS = ['connectionattempts', 'connecttimeout', 'controlpath', 'password',
            'hostname', 'identityfile', 'port', 'user', 'serveraliveinterval']
HOST_KEYS_TYPES = dict(zip(HOST_KEYS, 
    [int, int, str, str, str, List[str], int, str, int])) 


IGNORED_SECTIONS = ['schedule','business_contact', 'name',
            'origin', 'compiler_info', 'zzz', 'roster', 'allowed_environments',
            'rerun', 'compiled_time', 'supersedes', 
            'this_dir', 'debug', 'rerun_ok']

ITERABLE_SECTIONS = frozenset({'remote_ops', 'destination', 'source', 'xforms', 'cleanup'})

# The keywords may not be used for things like recipe names, or any user
# defined data.
KEYWORD_TRANSFORMS = {
    'allowed_environments':[uu.listify],
    'box':[str.__str__],
    'cleanup':[uu.listify],
    'comment':[uu.listify],
    'date_offset':[int.__call__],
    'db':[str.__str__],
    'dbload':[uu.listify],
    'destination':[uu.listify],
    'devlead':[uu.listify],
    'directory':[str.__str__],
    'file':[str.__str__],
    'frequency':[str.__str__],
    'host':[str.__str__],
    'ops':[uu.listify],
    # 'owner':[uu.listify],
    'randomfile':[uu.listify],
    'remote_ops':[uu.listify],
    'roster':[uu.listify],
    'schedule':[uu.listify],
    'source':[uu.listify],
    'target':[str.__str__],
    'techlead':[uu.listify],
    'tempfile':[],
    'xforms':[uu.listify],
    'XML':[uu.listify],
    'zzz':[]
}
KEYWORDS = set(KEYWORD_TRANSFORMS.keys())

LITERAL = 'literal'

# These locations are all equivalent to "here" when they appear.
LOCAL_HOSTS = ['localhost', 'localhost.localdomain', 
        socket.gethostname(), socket.getfqdn() ]

# These steps require a directory, and we generate it 
# programmatically when it is missing.
NEED_DIR = ['source', 'destination']

# This empty class is used as an exception to control looping when
# the loops are nested.
class OuterLoop(Exception): 
    pass

OVERWRITE_TRANSLATIONS = {
    # Will create versions.
    True : True,
    None : True,
    "true" : True,

    # Will /not/ create a new version of an existing file.
    False : False,
    "false" : False,

    # Removes all versions of target, and then moves the file.
    "replace" : None
    }

# We support several formats.
PANDAS_KEYS = {'format', 'filename'}
PANDAS_KEYS_TYPES = dict.fromkeys(PANDAS_KEYS, str)

PLUGINS = ( 'framediff', 'slateupload', 'chromefilter', 
            'cr_images', 'dotzero', 'randomfile', 'grap',
            'cr_mastercard', 'xmlscrub', "fusionpics", "dashboard",
            'bunzip2', 'XML', 'pgpinspect', 'encryptpics', 
            'studentpics', 'keymaint', 'testconnect' )
ACTIONS = (*('remote_ops', 'source', 'xforms', 'destination', 'cleanup'), *PLUGINS)


REQUIRED_KEYS = ('count',)
REQUIRED_VALUES = (range(0, sys.maxsize),)
REQUIRED_DEFAULTS = uu.SloppyDict(zip(REQUIRED_KEYS, REQUIRED_VALUES))

# The user does not always provide these, but we generate them from the
# compiled data.
RESERVED = ('this_dir', 'name', 'compiled', 'origin', 'roster', 'devlead', 
    'allowed_environments', 'schedule', 'comment', 'password', 'owner',
    'notifications', 'metadata', 'framediff', 'devlead')

RESERVED_TYPES = dict(zip(
    RESERVED, 
    [ str, str, Tuple[int, str], str, list, list, list, str, list ]
    ))

S3_EXE  = shutil.which('aws') or 'aws'
S3_KEYS = {'aws_access_key_id', 'aws_secret_access_key'}

# DESTINATION explanation.
#                   aws   cp|mv fqn    //bucket/key/         bucket
S3_DEST_CMD_1    = '{} s3 {{}} {{}} s3://{{}}/{{}}/ --profile {{}}'.format(S3_EXE)
#                   aws   cp|mv here      //bucket/key                          file           bucket
S3_DEST_CMD_MANY = '{} s3 {{}}  {{}}  s3://{{}}/{{}}/ --recursive --exclude "*" --include "{{}}" --profile {{}}'.format(S3_EXE)

# SOURCE explanation.
#                    aws   cp|mv    //bucket/key/file  here             bucket
S3_SOURCE_CMD_1    = '{} s3 {{}} s3://{{}}/{{}}/{{}}   {{}} --profile {{}}'.format(S3_EXE)
#                    aws   cp|mv    //bucket/key  here                            file            bucket
S3_SOURCE_CMD_MANY = '{} s3 {{}} s3://{{}}/{{}}   {{}} --recursive --exclude "*" --include "{{}}" --profile {{}}'.format(S3_EXE)

# handshake-importer-uploads/importer-production-richmond 

SHAREFILE_KEYS = { 'host', 'client_id', 'client_secret', 'username', 'password', 'root_folder' }

SLATEUPLOAD_KEYS = ('username', 'password', 'format_id', 'file', 'url')
SLATEUPLOAD_VALUES = (
    'endpointaccount', 
    'DM2B2C4s7Xu9', 
    None,
    None,
    'https://connect.richmond.edu/manage/service/import?cmd=load&format={}')
SLATEUPLOAD_DEFAULTS = dict(zip(SLATEUPLOAD_KEYS, SLATEUPLOAD_VALUES))

STUDENTPICS_KEYS = ('input', 'column', 'images')

# TODO: Allow for the newly defined TCSP operations.
TCSP_KEYS = ['error', 'filename', 'filename2', 'folder', 'mode', 'klobber']
TCSP_KEYS_TYPES = dict(zip(TCSP_KEYS, [int, str, str, str, int, bool]))

TCSP_MODES = {'put':1, 'get':2, 'move':3}
TCSP_TYPES = {'box', 'sharefile'}

# These occur at the top level of the recipe, defining blocks.
# As plugins are written, they perhaps should be added to this
# list to ensure checking. The values are the type that the argument
# must take.
TOP_LEVEL_KEYS = uu.sloppy({
    'allowed_environments':(list, str), 
    'cleanup':(list, dict, (str, object)), 
    'comment':(str, None),
    'date_offset':(int, None), 
    'dbload':(list, dict), 
    'destination':(list, dict), 
    'devlead':(list, str),
    'framediff':(list, dict),
    'frequency':(str),
    'metadata':(list, str), 
    'notifications':(dict, (str, list)), 
    'owner':(str, None),
    'password':(str, None),
    'remote_ops':(list, dict), 
    'roster':(list, str), 
    'schedule':(list, str), 
    'source':(list, dict), 
    'techlead':(list, str),
    'xforms':(list, dict),
    'XML':(list, dict)
    })

# These are the default values for the above.
TOP_LEVEL_VALUES = [None, 'missing',
    'missing', 0, None, None, None, None,
    None, None, DEFAULT_ROSTER_ORDER, None, 'orphan', '@adhoc',
    None, None ]

# And one dict-zip to bind them.
TOP_LEVEL_DEFAULTS = dict(zip(TOP_LEVEL_KEYS.keys(), TOP_LEVEL_VALUES))

WAIT_KEYS = ('time', 'until', 'use')
WAIT_VALUES = (0, -1, 0)
WAIT_DEFAULTS = uu.SloppyDict(zip(WAIT_KEYS, WAIT_VALUES))


# For transformations.
XFORM_KEYS = ['input', 'ops', 'output']
XFORM_IO_KEYS = ['name', 'type']
XFORM_FILE_TYPES = ['csv', 'feather', 'msgpack', 'pickle', 'txt', 'xml']

XFORM_CSV_KEYS =   ['header', 'sep', 'quote', 'esc', 
    'qforce', 'nodotzero', 'footer', 'fixed_header', 'rows' ]
XFORM_CSV_VALUES = [ True,    ",",   2,       "\\",   
    csv.QUOTE_MINIMAL, True, None, None, -1 ]
XFORM_CSV_DEFAULTS = dict(zip(XFORM_CSV_KEYS, XFORM_CSV_VALUES))
XFORM_CSV_QUOTES = [ "", "'", '"', "`" ]

XFORM_XML_KEYS = [ 'frame', 'remap', 'sep', 'frame_name', 'row_name',
    'encoding', 'output', 'debug', 'nodotzero' ]
XFORM_XML_VALUES = [ '', {}, '/', 'compiler_default_frame', 'row',
    'utf-8', '', False, True ]
XFORM_XML_TYPES = dict(zip(XFORM_XML_KEYS, 
    [ str, dict, str, str, str, str, str, bool, bool ] ))
XFORM_XML_DEFAULTS = dict(zip(XFORM_XML_KEYS, XFORM_XML_VALUES))


XFORM_OPS = uu.SloppyDict({
    "csv":"csv",
    "dbload":"/sw/oracle/product/client12c/bin/sqlldr",
    "delete":shutil.which('rm') + " -f ",
    "dos2unix":"/usr/bin/dos2unix",
    "gpg":shutil.which('gpg'),
    "gzip":shutil.which("gzip") + " --best ",
    "gunzip":shutil.which("gunzip"),
    "iconv":shutil.which("iconv"),
    "mail":shutil.which('mail'),
    "move":shutil.which("mv") + ' -f',
    "rename":shutil.which("mmv"),
    "save":"save",
    "scp":shutil.which("scp"),
    "sed":shutil.which("sed"),
    "sweep":"{} -f $mydir/*".format(shutil.which("rm")),
    "tar":shutil.which("tar"),
    "touch":shutil.which("touch"),
    "unix2dos":shutil.which("unix2dos"),
    "xml":"xml",
    "zip":"/sw/oracle/product/client12c/bin/zip",
    "zzz":"zzz"
    })   

XML_KEYS = ('input', 'output', 'grammar')
XML_VALUES = ('tempfile', 'tempfile.xml', None)

XML_DEFAULTS = dict(zip(XML_KEYS, XML_VALUES))
XML_GRAMMAR_LOCATION = os.environ.get('plugins')

XML2CSV_KEYS = {'output', 'xpath', 'columns'}
XMLSCRUB_KEYS = {'input', 'output'}

ZIP_OPTS = {
    'box'   : (XFORM_OPS['gzip'], 'gz'),
    True    : (XFORM_OPS['zip'], 'zip'),
    False   : (None, None),
    'gzip'  : (XFORM_OPS['gzip'], 'gz')
    }
