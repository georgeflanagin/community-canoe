#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
A HOP is a Half-Open Pipe. It is used to connect to other machines
(and localhost) to send files, collect files, and issue remote
commands. The half-open aspect of the name, and the use of the
word 'collect' rather than 'receive' means that all activity
originates at one end only of the connection. It does not, however,
create a shell on the other end. 
"""

import fnmatch as fnm
from   glob import glob
import os
import paramiko
from   paramiko import SSHException
import shlex
import shutil
import socket
import subprocess
import sys
import typing
from   typing import *

import tombstone as tomb
from   urdecorators import show_exceptions_and_frames as trap
import urtunnel as urt
import urutils as uu

from   canoeobject import CanoeObject
from   fname import Fname

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.5'
__maintainer__ = 'George Flanagin, Douglas Broome, Kerri Chapman'
__email__ = 'gflanagin@richmond.edu, dbroome@richmond.edu, kchapman@richmond.edu'
__status__ = 'Prototype'


__license__ = 'MIT'
import license

# setup logging
if __name__ == "__main__":
    import logging
    logging.basicConfig()
    logging.getLogger('paramiko.transport').setLevel(logging.DEBUG)
    # paramiko.util.log_to_file('hop.log')


def maintombstone(x:Any) -> None:
    """
    As this is the key routine of the entire library, this function calls
    tombstone() iff we are running as a script; i.e., if we are executing
    the test program at the end of the source.

    For the real errors (rather than the flowtrace) we call tombstone() 
    directly.
    """
    if __name__ == "__main__": tomb.tombstone(x)
    else: pass


# These names are always equivalent to "here"
always_localhost = ['localhost', 
                'localhost.localdomain', 
                socket.gethostname(),
                socket.getfqdn()] 

###
# These are commands that can be executed through the native
# sftp interface without use of subprocess. The idea is that
# the linux commands (intuitive) are keys for the sftp server
# commands (less intuitive). 
###
sftp_commands = {
    "chmod":"chmod", 
    "chown":"chown", 
    "stat":"stat",
    "mkdir":"mkdir", 
    "pwd":"getcwd", 
    "mv":"rename", 
    "rm":"remove",
    "ls":"listdir"
    }


class HOP:
    pass

class HOP:
    """ The HOP (i.e., "Half Open Pipe") represents a connection open to
    another machine.

    The connection to the remote host persists for the life of the object, unless
    it is terminated at the remote end. HOPs use the lower level SSHv2 libraries.
    """

    @classmethod
    def get_instance(HOP_cls, host:str='localhost', password:str=None) -> HOP:
        """ 
        Pseudo constructor that is suitable for almost all uses outside of
        pure testing. This method conceals most of the complexity within __init__()
        and is probably the constructor most often used.

        host - Not necessarily the name of a /host/, but the name of something known
            to the ssh agent via the process owner's ssh config file.

        returns - an instance of HOP.

        raises HOPException on failure.

        Example:

            try:
                h = HOP.get_instance('myserver')
            except e:                 ^^^^^^^^
                                          \_____ name of something in ssh config.  

        """
        return HOP_cls(host, password)


    @trap
    def __init__(self, hostinfo:uu.SloppyDict) -> None:
        """ 
        Create a connection to a host. 

        hostinfo -- A dict containing everything needed to connect.

                     {'connectionattempts': '3',
                      'connecttimeout': '2',
                      'controlpath': '/tmp/ssh-canoe@starr.richmond.edu:22',
                      'hostname': 'starr.richmond.edu',
                      'identityfile': ['/home/canoe/.ssh/id_rsa'],
                      'password':None
                      'port': '22',
                      'serveraliveinterval': '59',
                      'user': 'canoe'}

        """
        CanoeObject.__init__(self)

        hostinfo = uu.SloppyDict(hostinfo)

        self._user = None
        self._result = None
        self._exit_code = 0
        self._error_message = None
        self._hop = None
        self._password = hostinfo.get('password')
        self._transport = None
        self._sftp = None
        self._hostname = hostinfo.hostname

        for k, v in hostinfo.items():
            setattr(self, "_"+str(k), v)

        ###
        # Note that we are using the info from ssh to determine localhost.
        ###
        tomb.tombstone('Getting connection data for {}'.format(hostinfo.hostname))
        self.local_hop = hostinfo.hostname in always_localhost 

        # If we are local, different rules apply and we are finished.
        if self.local_hop: 
            uu.tombstone("Local hop")
            return

        try:
            self._hop = urt.drill_baby_drill(hostinfo, self._password)
            if self._hop is None: return
            self._transport = self._hop.get_transport()
            self._sftp = paramiko.SFTPClient.from_transport(self._hop.get_transport())
    
        except Exception as e:
            uu.tombstone(f"Exception {e}")
            self._hop = None
            return            

        uu.tombstone(f"Connected to {hostinfo.hostname}")
        


    def __str__(self) -> str:
        """ 
        Print what this object is connected to. 
        """
        this_user = uu.me() if self._user is None else self._user
        this_host = 'localhost' if self.local_hop else self._hostname
        this_result = 0 if self._result is None else self._result
        if isinstance(this_result, bytes): this_result=this_result.decode('utf-8')
        return f"{this_user}@{this_host}: last result: {this_result}"


    def __bool__(self) -> bool:
        """ 
        Return whether self._hop is attached to anything.
        """

        # We presume that we are always self connected.
        if self.local_hop: return True
        if self._hop is None: return False

        try:
            _ = self._hop.get_transport()
            _.send_ignore()
            return True

        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            return False


    def exit_code(self) -> int:
        return self._exit_code


    @trap
    def get_file(self, 
            remote_filename: str, 
            local_directory: str=".", 
            overwrite: bool=True) -> bool:
        """ 
        Copy a file from a remote host to "here."

        remote_filename -- candidate file for copying.   /a/b/c/x.dat
        local_directory -- the remote file should land here.  /m/n
        overwrite -- if set to true, we can skip the check for pre-existence.

        returns: -- True if successful
                    False if there is nothing to do.
                    raises Exception on errors.
        """
        tomb.tombstone(uu.fcn_signature("hop.get_file", remote_filename, local_directory, overwrite))
        remote_filename = uu.date_filter(remote_filename)

        if self.local_hop:
            shutil.copy2(remote_filename, local_directory) 
            return True
            

        try:
            remote_dir, remote_file = os.path.split(remote_filename)
            if not remote_dir: remote_dir = '.'
            tomb.tombstone("checking remote dir <{}>".format(remote_dir))
            try:
                remote_files = [ 
                    _ for _ in 
                    self._sftp.listdir(remote_dir) 
                    if fnm.fnmatch(_, remote_file) 
                    ]
                tomb.tombstone("found these matching files: " + str(remote_files))
                if not len(remote_files): 
                    return False

            except FileNotFoundError as e:
                tomb.tombstone("Nothing found matching {}.".format(remote_filename))
                return False

            except OSError as e:
                tomb.tombstone("unable to ls on " + remote_dir)
                raise

            else:
                tomb.tombstone("{}".format("\n".join(remote_files)))

            i = 0
            for _ in remote_files:
                tomb.tombstone(uu.fcn_signature('_sftp.get',
                    uu.path_join(remote_dir, _), uu.path_join(local_directory, _)
                    ))
                self._sftp.get(
                    uu.path_join(remote_dir, _), 
                    uu.path_join(local_directory, _)
                    )
                i += 1
            else:
                tomb.tombstone("Retrieved {} files.".format(i))

        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            raise
        
        return True


    def ls_sftp(self, pathname:str) -> List[str]:
        """
        The SFTP client does not support wildcard names. This is
        probably because the definition of a wildcard metacharacter 
        cannot be precisely known on a remote machine.

        pathname -- something like '/files/export/*csv'

        returns -- a list of matching file names.
        """
        d, f = os.path.split(pathname)
        all_the_files = self._sftp.listdir(d)
        if not f: return all_the_files

        for f_part in f.split('*'):
            all_the_files = [ _ for _ in all_the_files if f_part in _ ]

        return sorted([ uu.path_join(d, _) for _ in all_the_files ])


    @trap
    def remote_exec(self, cmd:str, ignore_non_zero:bool=False) -> HOP:
        """ 
        Execute exactly one command.

        NOTE: this function populates the _result and _error_message
        members of the object. A typical use is something like one of these:

            filelist = my_hop.remote_exec('ls').results()
            if my_hop.remote_exec('ls'): filelist = my_hop.results()

        returns: -- self, so that you can chain other functions.
        """

        self._result = []

        cmd = uu.date_filter(cmd).strip()

        if self.local_hop:
            try:
                tomb.tombstone("ignore_non_zero is {}".format(ignore_non_zero))
                tomb.tombstone("attempting subprocess.check_output({})".format(cmd))
                self._result = subprocess.check_output(cmd, shell=True)

            except subprocess.CalledProcessError as e:
                # subprocess raises and exception on non-zero exit status. There is
                # nothing to be done, so just report it, and continue on. 
                if ignore_non_zero: 
                    tomb.tombstone('ignoring non-zero return code from last operation.')
                else:
                    tomb.tombstone(uu.type_and_text(e))

            except Exception as e:
                tomb.tombstone(uu.type_and_text(e))
                if ignore_non_zero: pass
                else: raise

        else:
            cmd = shlex.split(cmd)
            direct_function = sftp_commands.get(cmd[0], None)
            if direct_function is None:
                tomb.tombstone('subprocessing {}'.format(cmd))
                stdin, stdout, stderr = self._hop.exec_command(" ".join(cmd))
                
            else:
                target = cmd[1]
                all_the_files = self.ls_sftp(target)
                print(all_the_files)
                for f in all_the_files:
                    result = getattr(self._sftp, direct_function)(f)

                
                """
                direct_function = next(iter([ _ for _ in sftp_commands 
                                        if cmd.startswith(_) ]), None)
                try:
                    if direct_function:
                        tomb.tombstone('sftp direct function {} {}' + direct_function)
                        new_cmd = " ".join(shlex.split(cmd)[1:])
                        fcn = getattr(self._sftp, sftp_commands[direct_function])
                        fcn(new_cmd)

                    else:
                        tomb.tombstone('subprocessing ' + cmd)
                        stdin, stdout, stderr = self._hop.exec_command(cmd)

                except Exception as e:
                    raise
                """

                # self._result = [line.strip() for line in stdout]
                # self._error_message = [line.strip() for line in stderr]

        return self


    @trap
    def send_one_file(self, 
            local_filename: str, 
            remote_filename: str=None, 
            overwrite: bool=True,
            remote_directory:str=None) -> bool:
        """ 
        Sends a file to the remote host.

        local_filename -- name of something in the local file system.
        remote_filename -- name the transferred object should have at the destination.
            remote_filename defaults to the "file part" of local_filename.
        overwrite -- When set to False, this causes the HOP to look for a file of
            the given/calculated name at the remote end before attempting the transfer.
        remote_directory -- where the file should go at the destination
            
        NOTE: send_one_file() checks for existence of local_filename.

        returns: -- True on success, False on all errors except FileNotFound.
        """
        tomb.tombstone(uu.fcn_signature(
                'HOP.send_one_file', local_filename, remote_filename, overwrite, remote_directory))

        # We must figure out the destination filename.
        local_filename_as_fname = Fname(local_filename)

        if not remote_filename: remote_filename = local_filename_as_fname.fname
        if remote_directory: 
            remote_filename = uu.path_join(remote_directory, remote_filename)

        # If we are local, then we use the os library.
        if self.local_hop:
            """ find out if the /mnt is mounted """
            if ( remote_directory.startswith('/mnt') and 
                not self.mount_exists(remote_directory) ):
                    text = remote_directory + " is not mounted. Contact your friendly SA"
                    tomb.tombstone(text)
                    raise Exception(text)

            tomb.tombstone(" ".join(["local copy:", local_filename, "to", remote_filename]))
            return uu.fcopy_safe(local_filename, remote_filename) == 0

        try:
            channel = self._sftp.get_channel()
            channel.settimeout(5.0)
        except Exception as e:
            tomb.tombstone('Cannot set timeout of get channel')

        # Note that if this blows up there are several possible causes, and most of 
        # them involve an incorrectly written set of instructions. For example, you
        # cannot copy a bunch of files to a single file.
        try:
            tomb.tombstone("local name: {}".format(os.path.abspath(local_filename)))
            tomb.tombstone("dest name:  {}".format(remote_filename))
            result = self._sftp.put(
                os.path.abspath(str(local_filename)), 
                remote_filename)
            tomb.tombstone("result is {}".format(result))
            
        except AttributeError as e:
            if self._sftp is None: 
                tomb.tombstone('Host {} is not available.'.format(self._hostname)) 
                return False
                
        except FileNotFoundError as e:
            tomb.tombstone("Either the source file does not exist or the destination file cannot be written.")
            return None

        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            return False

        return True

    @trap
    def mount_exists(self, mount_name:str) -> bool:
        try:
            subprocess.check_call(shlex.split("/bin/ls " + mount_name), timeout=0.5)
        except subprocess.TimeoutExpired as e:
            tomb.tombstone(mount_name + " is not mounted")
            return False

        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            return False

        else:
            return True


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Syntax: python hop.py host filename [password]")
        exit(0)


    host = sys.argv[1]
    f = Fname(sys.argv[2])
    if not f:
        tomb.tombstone("No file named",str(f)) 
        exit(1)

    password = None if len(sys.argv) < 4 else sys.argv[3]

    # This one is /full/dir/name/filename
    local_file_name = str(f)
    # This one is just "filename". We don't know the directory structure at
    # the other end.
    remote_file_name = f.fname

    tomb.tombstone("Attempting to transfer " + local_file_name + " to " + str(host) +
            " where it will be known as " + remote_file_name)

    try:
        h = HOP.get_instance(host, password)
        tomb.tombstone(" ".join(["HOP connected to",host]))

        h.send_one_file(local_file_name)
        tomb.tombstone(["File send results:",h.results(True)])

        command = "ls -l " + remote_file_name
        h.remote_exec(command)
        tomb.tombstone([command,"results:",h.results(True)])

        h.get_file(remote_file_name, remote_file_name + ".new")
        tomb.tombstone(["got",remote_file_name,"as",remote_file_name+".new"])

        h.get_file(remote_file_name, remote_file_name + ".new", True)
        tomb.tombstone(["got",remote_file_name,"as",remote_file_name+".new with klobber"])

    except NameError as e:
        tomb.tombstone()

    except Exception as e:
        tomb.tombstone(uu.type_and_text(e))

else:
    # print(str(os.path.abspath(__file__)) + " compiled.")
    print("*", end="")

