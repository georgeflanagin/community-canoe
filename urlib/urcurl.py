# -*- coding: utf-8 -*-
"""
A generalized interface to use of curl and libcurl.
"""

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, 2020 University of Richmond'
__credits__ = 'Douglas Broome, original version for Box'
__version__ = '0.1'
__maintainer__ = 'George Flanagin, Douglas Broome'
__email__ = '{gflanagin|dbroome}@richmond.edu'
__status__ = 'Production'

__license__ = 'MIT'

# Builtin packages.
import enum
import glob
from   http import HTTPStatus
import os
import shutil
import sys
from   typing import *

# UR imports
import fname
import urutils as uu
from   urdecorators import show_exceptions_and_frames as trap

CODES = uu.SloppyDict({
    "OK":       [HTTPStatus.OK],
    "OK_CREATE":[HTTPStatus.OK,HTTPStatus.CREATED],
    "CREATE":   [HTTPStatus.CREATED],
    "OK_DELETE":[HTTPStatus.NO_CONTENT, HTTPStatus.OK],
    "DELETE":   [HTTPStatus.NO_CONTENT]
    })


class CurlInterface(enum.Enum):
    """
    Types of interfaces for the URcurler.
    """

    BOX = 'BOX'
    SHAREFILE = 'SHAREFILE'
    OPENAPI3 = 'OPENAPI3'
    ETHOS = 'ETHOS'
    BASIC = 'BASIC'
    SFTP = 'SFTP'


CurlMessage = uu.MessageTable({
    0: "All fine. Proceed as usual.",
    1: "The URL you passed to libcurl used a protocol that this libcurl does not support. The support might be a compile-time option that you didn't use, it can be a misspelled protocol string or just a protocol libcurl has no code for.",
    2: "Very early initialization code failed. This is likely to be an internal error or problem, or a resource problem where something fundamental couldn't get done at init time.",
    3: "The URL was not properly formatted.",
    4: "A requested feature, protocol or option was not found built-in in this libcurl due to a build-time decision. This means that a feature or option was not enabled or explicitly disabled when libcurl was built and in order to get it to function you have to get a rebuilt libcurl.",
    5: "Couldn't resolve proxy. The given proxy host could not be resolved.",
    6: "Couldn't resolve host. The given remote host was not resolved.",
    7: "Failed to connect to host or proxy.",
    8: "The server sent data libcurl couldn't parse. This error code was known as as CURLE_FTP_WEIRD_SERVER_REPLY before 7.51.0.",
    9: "We were denied access to the resource given in the URL. For FTP, this occurs while trying to change to the remote directory.",
    10: "While waiting for the server to connect back when an active FTP session is used, an error code was sent over the control connection or similar.",
    11: "After having sent the FTP password to the server, libcurl expects a proper reply. This error code indicates that an unexpected code was returned.",
    12: "During an active FTP session while waiting for the server to connect, the CURLOPT_ACCEPTTIMEOUT_MS (or the internal default) timeout expired.",
    13: "libcurl failed to get a sensible result back from the server as a response to either a PASV or a EPSV command. The server is flawed.",
    14: "FTP servers return a 227-line as a response to a PASV command. If libcurl fails to parse that line, this return code is passed back.",
    15: "An internal failure to lookup the host used for the new connection.",
    16: "A problem was detected in the HTTP2 framing layer. This is somewhat generic and can be one out of several problems, see the error buffer for details.",
    17: "Received an error when trying to set the transfer mode to binary or ASCII.",
    18: "A file transfer was shorter or larger than expected. This happens when the server first reports an expected transfer size, and then delivers data that doesn't match the previously given size.",
    19: "This was either a weird reply to a 'RETR' command or a zero byte transfer complete.",
    21: 'When sending custom "QUOTE" commands to the remote server, one of the commands returned an error code that was 400 or higher (for FTP) or otherwise indicated unsuccessful completion of the command.',
    22: "This is returned if CURLOPT_FAILONERROR is set TRUE and the HTTP server returns an error code that is >= 400.",
    23: "An error occurred when writing received data to a local file.",
    25: "Failed starting the upload. The error buffer usually contains the server's explanation for this.",
    26: "There was a problem reading a local file or an error returned by the read callback.",
    27: "A memory allocation request failed. This is serious badness and things are severely screwed up if this ever occurs.",
    28: "The specified time-out period was reached according to the conditions.",
    34: "This is an odd error that mainly occurs due to internal confusion.",
    35: "A problem occurred somewhere in the SSL/TLS handshake. You really want the error buffer and read the message there as it pinpoints the problem slightly more. Could be certificates (file formats, paths, permissions), passwords, and others.",
    36: "The download could not be resumed because the specified offset was out of the file boundary.",
    37: "A file given with FILE:// couldn't be opened. Most likely because the file path doesn't identify an existing file. Did you check file permissions?",
    51: "The server's SSL/TLS certificate or SSH fingerprint failed verification.",
    58: "The client certificate had a problem so it could not be used.",
    59: "Unsupported SSL cipher.",
    60: "Certificate could not be authenticated against known CA certificates.",
    61: "Unknown data transfer encoding.",
    63: "Maximum file size exceeded.",
    67: "Login failure. At least one credential is invalid.",
    78: "The resource referenced in the URL does not exist.",
    79: "Unspecified SSH session error.",
    
    255: "Unknown total catastrophic failure. The curl-universe has ended."
    })


class CurlInterfaceKeys(enum.Enum):
    """
    These are the types of information we need with each service. The SFTP
    is thrown in as a sanity check / experiment. No information yet on ETHOS.
    The tuples will be used to validate that we have all the information 
    required to attempt a connection.
    """

    BOX = ( "client-id", "client-secret", "enterprise-id", 
            "jwt-key-id", "rsa-private-key-file", "rsa-private-key-passphrase", 
            "token-url", "app-user-id", "verify-ssl-certs",
            "host", "instance_vars")
    SHAREFILE = ( 'grant_type', 'client_id', 'client_secret', 
        'username', 'password', 'root_folder', 'host', 'instance_vars' )
    OPENAPI3 = ( 'xapitoken', 'host', 'instance_vars')
    ETHOS = ( 'host', 'instance_vars') 
    BASIC = ( 'user', 'password', 'host', 'instance_vars')
    SFTP  = ( 'user', 'key', 'host' )


class CurlSchemes(enum.Enum):
    """
    This class is provided for completeness and consistency
    with other parts of the code. There is nothing terribly advanced
    going on here; it is nice to have everything work the same way.
    """

    BOX       = 'https'
    SHAREFILE = 'https'
    OPENAPI3  = 'https'
    ETHOS     = 'https'
    BASIC     = 'https'
    SFTP      = 'sftp'


class URcurler:
    """
    Our motto: We can curl anything that can be curled.

    General notes:

        None of these functions changes the name of anything. If you wish
        to rename files, you need to do it before they are sent or after
        they are collected. The concept of valid names is outside the
        scope of this module because there is no way to validate names.

        Some https services contain many seldom used functions, such as 
        getting a particular version of an object. The URcurler does not
        support these. If you need these operations, you can derive from
        this class, and add those functions.

        There is no facility to get a "raw blob." In Linux, everything 
        has an entry in the file system, so we save the data to files,
        always, always, always.
    """
    __keys__ = {'mode':          'a type of CurlInterface',
                'scheme':        'https, for the most part.', 
                'checklist':     'a list of the params we require.',
                'connected':     'whether we are authenticated and connected', 
                'message':       'last (error) message',
                'host':          'basic location of the service',
                'port':          'the port to use',
                'verbose':       'whether to narrate the activity.',
                'instance_vars': 'parameters that uniquely identify the repo.'}

    __others__ = dict.fromkeys(set(( x for y in CurlInterfaceKeys for x in y.value )))
    __values__ = (None, 'https', None, False, "", "", 
        "localhost", 443, False, uu.SloppyDict())
    __defaults__ = { **__others__, **dict(zip(__keys__, __values__)) }
    exe = shutil.which('curl')


    ###
    # Step 1, build the object and idenfity the choice of mechanism.
    ###
    def __init__(self, mode:Union[CurlInterface,str]):
        """
        mode -- the type of thing we will be curling. 
        """
        # I cannot imagine how this is going to happen, but 
        # we might as well keep a lookout for it. If we don't, the
        # code will blow up in some unusual way that will be
        # hard to debug.
        if not URcurler.exe:
            raise Exception('FATAL: Cannot find curl or libcurl.') 

        # Note that the objects have all the slots, but only 
        # a subset is relevant for any particular use.
        for k, v in URcurler.__defaults__.items():
            setattr(self, k, v)

        self.mode = ( mode if isinstance(mode, CurlInterface) else 
                      getattr(CurlInterface, mode) )
        if self.mode is None:
            raise Exception(f"Unknown curling mode {mode}")
        uu.tombstone(f"curler created for {self.mode=}")

        self.scheme    = getattr(CurlSchemes, mode).value
        # The checklist contains the names of the slots that
        # make a difference.
        self.checklist = getattr(CurlInterfaceKeys, mode).value
        

    def __str__(self) -> str:
        return "\n".join(
            [f"{k}:{getattr(self, k)}" 
            for k in sorted(URcurler.__defaults__) ]
            )
        

    @property
    def _OK_to_attachIO(self) -> bool:
        """
        Verify info is enough to proceed.
        """
        return True if all(getattr(self, k) is not None for k in self.checklist) else False
        

    @property
    def error_message(self) -> str:
        """
        return the most recent message, and clear it. The purpose is
        to preserve relevance and context of any messages.
        """

        s = self.message
        self.message = ''
        return s


    def __bool__(self) -> bool:
        """
        returns True if we have a valid connection.
        """
        return self.connected


    ###
    # Step 2, pass in parameters that idenfity the repo we are connecting to.
    ###
    def add_credential(self, k:str, v:object) -> bool:
        self.add_credentials(**{k:v})
        return self._OK_to_attachIO


    def add_credentials(self, **kwargs) -> bool:
        """
        kwargs -- parameters required for the current mode of connection.

        returns -- True  iff the parameters are relevant and complete. 
                   False iff the info is not yet complete.
                   None  iff the caller is trying to set an irrelevant parameter.
        """
        for k, v in kwargs.items():
            if k in self.checklist:
                setattr(self, k, v)
            else:
                self.message = f"{k} not in checklist for {self.mode}"
                return None
        
        return self._OK_to_attachIO    
    

    def _resolve(self, local_items:Union[str, list]) -> list:
        """
        The functions that do the heavy lifting are designed to work
        with a list of local files. We allow the caller to build the list
        or let URcurler build it.
        """
        if isinstance(local_items, (list, tuple)): 
            return local_items
        elif isinstance(local_items, dict): 
            return list(local_items.keys())
        else: 
            return glob.glob(uu.expandall(local_items))


    def _remote_resolve(self, local_items:Union[str, int]) -> list:
        """
        This function is like _resolve, except that we need to 
        extract relevant parts of the name(s). 
        """
        if isinstance(local_items, int): 
            return [local_items]
        elif isinstance(local_items, list): 
            return local_items
        else:
            return [os.path.basename(local_items)]
            

    ###
    # Step 3, connect.
    ###
    def open(self) -> bool:
        """alias for attachIO()"""

        return self.attachIO()


    def attachIO(self) -> bool:
        """
        Authenticate with and connect to the curlable repo.

        returns -- True if we are connected.
        """
        if not self._OK_to_attachIO: 
            self.message = f"checklist for {self.mode} is incomplete"
            uu.tombstone(f"{self.checklist=}")
            return False

        return getattr(self, f"_attachIO_{self.mode.value}")()


    ##################################################################
    # The functional, public operations.
    ##################################################################

    def browse(self, folder:Union[str, int]) -> uu.SloppyDict:
        """
        folder -- the name or ID of a folder in the repo.

        returns -- a dict containing the information about the contents.
            If the dict is empty, there was no information. If the return
            is None, then there was an error or the folder did not exist.
        """
        return uu.SloppyDict()
     

    def delete(self, item_names:Union[str, int, list]) -> Tuple[bool, int, int]:
        """
        item_names -- the name, ID, or a list of names and/or IDs of 
            items to be deleted.

        returns -- a tuple containing:
            bool <-> True if there are no errors.
            int  <-> number of successes.
            int  <-> number of failures.
        """
        return True, 1, 0


    def get(self, item_name:Union[str, int], local_dir:str) -> Tuple[bool, int, int]:
        """
        item_name -- The name or ID of something to retrieve. Note that
            the name may be a 'wild card' that has meaning within the type
            of curl-able repo.
        local_dir -- The name of the local directory where the items will be left.

        returns -- a tuple containing:
            bool <-> True if there are no errors.
            int  <-> number of successes.
            int  <-> number of failures.
        """
        return True, 1, 0


    def put(self, local_items:Union[str, list], 
        destination_folder:str="") -> Tuple[bool, int, int]:
        """
        local_items -- either a (wild card) name, or a list of names.
        destination_folder -- some place on the destination where 
            these go.

        returns -- a tuple containing:
            bool <-> True if there are no errors.
            int  <-> number of successes.
            int  <-> number of failures.
        """
        AOK = True
        successes = 0
        failures = 0
        for f in self._resolve(local_items):
            if not fname.Fname(f): 
                print(f"Cannot locate {f}")
                failures += 1
                AOK = False

            elif self._put_one(f, destination_folder):
                successes += 1

            else:
                failures += 1
                AOK = False
                
        return AOK, successes, failures


    ##################################################################
    # The non-public, iteratative operations for delete, get, and put.
    ##################################################################

    def _delete_one(self, item_name:str) -> bool:
        """
        item_name -- the name of something to be deleted.

        returns --  True if the item was removed.
                    False if the removal fails.
                    None if the item does not exist.       
        """
        return True


    def _get_one(self, item_name:Union[str, int], local_dir:str) -> bool:
        """
        item_name -- The name or ID of a specific item.
        local_dir -- Where it goes.

        returns -- True if it worked, and False otherwise.
        """
        return True


    def _put_one(self, local_item:str, destination_folder:str="") -> bool:
        """
        local_item -- A specific name of something to be sent.
        destination_folder -- the location in the repo where we are sending it.

        returns -- True if it worked, and False otherwise.        
        """
        return getattr(self, f"_put_{self.mode.value}")(local_item, destination_folder)


    ##################################################################
    # The attachIO family.
    ##################################################################

    def _attachIO_BASIC(self) -> bool:
        """
        There is nothing to do because each request is authenticated.
        """
        self.connected = True
        return self.connected


    def _attachIO_BOX(self) -> bool:
        self.connected = True
        return self.connected


    def _attachIO_ETHOS(self) -> bool:
        self.connected = True
        return self.connected


    def _attachIO_SFTP(self) -> bool:
        """
        Nothing to do here; each request is authenticated.
        """
        self.connected = True
        return self.connected


    def _attachIO_SHAREFILE(self) -> bool:
        self.connected = True
        return self.connected


    def _attachIO_OPENAPI3(self) -> bool:
        """
        """
        cmd = f"{URcurler.exe} -H 'X-API-TOKEN:{self.xapitoken}' {self.host}"
        return uu.dorunrun(cmd, quiet=True)



    ##################################################################
    # The delete family.
    ##################################################################

    def _delete_BASIC(self) -> bool:
        return True


    def _delete_BOX(self) -> bool:
        return True


    def _delete_ETHOS(self) -> bool:
        return True


    def _delete_SFTP(self) -> bool:
        return True


    def _delete_SHAREFILE(self) -> bool:
        return True


    def _delete_OPENAPI3(self) -> bool:
        """
        """
        cmd = f"{URcurler.exe} -H 'X-API-TOKEN:{self.xapitoken}"
        return uu.dorunrun(cmd, verbose=self.verbose)



    ##################################################################
    # The get family.
    ##################################################################

    def _get_BASIC(self) -> bool:
        return True


    def _get_BOX(self) -> bool:
        return True


    def _get_ETHOS(self) -> bool:
        return True


    def _get_SFTP(self) -> bool:
        return True


    def _get_SHAREFILE(self) -> bool:
        return True


    def _get_OPENAPI3(self) -> bool:
        """
        """
        cmd = f"{URcurler.exe} -H 'X-API-TOKEN:{self.xapitoken}"
        return uu.dorunrun(cmd, verbose=self.verbose)


    ##################################################################
    # The put family.
    ##################################################################

    def _put_BASIC(self, local_file:str, destination_folder:str) -> bool:
        cmd = " ".join((
            f"""{URcurler.exe} -X POST -H "Content-Type:text/plain" """,
            f"""-u {self.user}:{self.password} """,
            f"""--data-binary @{local_file} {self.scheme}://{self.host}/{destination_folder} """
            ))
        uu.tombstone(f"\n\n{cmd}\n\n")
        result = uu.dorunrun(cmd, quiet=True, return_exit_code=True)
        uu.tombstone(CurlMessage[result])
        return result == 0


    def _put_BOX(self) -> bool:
        return True


    def _put_ETHOS(self) -> bool:
        return True


    def _put_SFTP(self, local_file:str, destination_folder:str) -> bool:
        cmd = " ".join((
            f"""{URcurler.exe} -u {self.user}: --key ~/.ssh/{self.key}""",
            f"""-k -T {local_file}""",
            f"""{self.scheme}://{self.host}:{self.port}/{destination_folder}"""
            ))
        
        uu.tombstone(cmd)
        result = uu.dorunrun(cmd, quiet=True, return_exit_code=True)
        uu.tombstone(CurlMessage[result])
        return result == 0
        


    def _put_SHAREFILE(self) -> bool:
        return True


    def _put_OPENAPI3(self, local_file:str, destination_folder:str) -> bool:
        """
        """
        cmd = " ".join((
            f"{URcurler.exe} -X POST -H 'X-API-TOKEN:{self.xapitoken}'",
            f"-F 'file=@{local_file}' {self.host}/{destination_folder}"
            ))
        return uu.dorunrun(cmd)


if __name__ == '__main__': 
    local_file = ""
    destination_folder = ""

    try:
        local_file, destination_folder = sys.argv[1:]
    except Exception as e:
        print("Usage: urcurl.py local_file destination_folder")
        sys.exit(os.EX_USAGE)

    creds = uu.SloppyDict({ 
        'user':'d02a63e9-0c6b-48d8-85d5-1e61b027bbd6',
        'password':'chocolate',
        'host':'richmond-testmig.blackboard.com',
        })
    
    curler = URcurler(CurlInterface.BASIC)
    print("curler created.")
    curler.add_credentials(**creds)
    print("added credentials.")
    if curler.attachIO():
        print("IO attached.")
    else:
        sys.exit(os.EX_NOINPUT)
    
    result = curler.put(local_file, destination_folder)
    print(result)
    
