#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from typing import *

""" 
This module detects and sets the environment variables for proper
operation of Canøe.
"""

# Credits
__author__ = 'George Flanagin, Douglas Broome'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.4'
__maintainer__ = 'George Flanagin, Douglas Broome'
__email__ = 'gflanagin@richmond.edu, dbroome@richmond.edu'
__status__ = 'Working Prototype'
__required_version__ = (3, 4)


__license__ = 'MIT'
import license

# Standard imports

import os
import subprocess
import sys
import tempfile

# Installed imports

# Canøe imports

import tombstone as tomb
import urutils as uu

############# BEGIN #################

compilation_verbosity = len(sys.argv) > 1 and '-v' in sys.argv
os.environ['comp_verbose'] = str(int(compilation_verbosity))
    
startup_step = 1
# We should be graceful in all things. Let's make sure CANOE_HOME
# is defined.
print(" ")
tomb.tombstone("Looking for $CANOE_HOME in the env")
if 'CANOE_HOME' not in os.environ:
    tomb.tombstone("CANOE_HOME must be given a value.")
    tomb.tombstone("Usually ... the value should be the top level directory of")
    tomb.tombstone(" the 'git clone' operation that contains the runnable code.")
    sys.exit(os.EX_CONFIG)

canoe_environ = uu.sloppy({
    'CANOE_CONFIG':'/sw/canoe/compilerconfig',
    'CANOE_LOG':'/sw/canoe/var/log/canoe',
    'CANOE_DATA':'/sw/canoe/var/data',
    'CANOE_PLUGINS':'/sw/canoe/canoe19_home/src/plugins'
    })

# If these directories are not available, we cannot continue.
for _ in canoe_environ.keys():
    startup_step += 1
    try:
        v = os.environ[_]
        uu.make_dir_or_die(v)
    except:
        os.environ[_] = canoe_environ[_]
        uu.make_dir_or_die(os.environ[_])

# We are not going to try to create the queues -- they must be
# created from the canoe console. However, we do need values
# to be present in the environment of all canoe processes.
for _ in canoe_queues.keys():
    startup_step += 1
    try:
        v = os.environ[_]
    except:
        os.environ[_] = canoe_queues[_]

startup_step += 1
uu.make_dir_or_die(os.environ['CANOE_DATA'])

#Set PWD to temp directory
os.environ['PWD'] = tempfile.gettempdir()
    
# Are we running inside virtualenv? It cannot be conclusively
# determined, but let's give it a go. This ordering is based on
# the idea that Canoe's code is written to run on the plain old
# bare metal, and with virtualenv. Therefore, if v_env is set to
# an integer greater than 1, then we have a problem.

startup_step += 1
tomb.tombstone("Looking for virtual environments.")
v_env_names = [
    'no virtual environment',
    'virtualenv',
    'pyvenv',
    'unknown']

# Assume the best...
v_env = 0

# The smoke and mirrors are done by prepending various things to the
# modules being loaded. sys.prefix is the "real" one, and the virtual
# environments introduce impostors.
if hasattr(sys, 'real_prefix'):
    v_env = 1
elif hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix:
    v_env = 2
elif 'VIRTUAL_ENV' in os.environ:
    v_env = 3
else:
    pass

# REAL programmers may want to continue. Let's provide that option.
tomb.tombstone( v_env_names[v_env] + ' detected.')
if v_env > 1 and len(sys.argv) > 1 and sys.argv[1] == '--force':
    pass
elif v_env == 0:
    tomb.tombstone("All praise Kibo, the Great, the Wise, the Wonderful.")
    pass
else:
    tomb.tombstone('The code may not work. You can unclick ' + 
                'the safety belts, but only by using --force.')
    sys.exit(os.EX_TEMPFAIL)

startup_step += 1
tomb.tombstone("Setting value of $ORACLE_HOME")
# If Oracle is installed, we will hope that it is installed but once on this machine.
if 'ORACLE_HOME' not in os.environ:
    tomb.tombstone("ORACLE_HOME is not set. Creating it now from whole cloth.")
    cmd = 'find / -wholename "*/oracle/product/*" 2>&1 | grep -v "Permission denied" | head -1'
    p = subprocess.Popen([cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    os.environ['ORACLE_HOME'] = str(out) if out is not None and len(out)>0 else None
    if os.environ['ORACLE_HOME'] is not None:
        tomb.tombstone("ORACLE_HOME has been set to " + os.environ['ORACLE_HOME'])
    else:
        tomb.tombstone("Oracle does not appear to present. Install it NOW.")
        sys.exit(os.EX_SOFTWARE)

startup_step += 1
tomb.tombstone("Setting value of $LD_LIBRARY_PATH")
# Oracle is present, set the LD_LIBRARY_PATH
if 'LD_LIBRARY_PATH' not in os.environ:
    tomb.tombstone("LD_LIBRARY_PATH is not set. Creating it based on available information.")
    cmd = ("find " + os.environ['ORACLE_HOME'] + 
            ' - wholename "*/lib" 2>&1 | grep -v "Permission denied" | head -1')
    p = subprocess.Popen([cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    os.environ['LD_LIBRARY_PATH'] = str(out) if out is not None and len(out)>0 else None
    if os.environ['LD_LIBRARY_PATH'] is not None:
        tomb.tombstone("LD_LIBRARY_PATH has been set to " + os.environ['LD_LIBRARY_PATH'])
    else:
        tomb.tombstone("Oracle may not be properly installed. Call your friendly DBA.")
        sys.exit(os.EX_SOFTWARE)

# Set up TNS_ADMIN
if 'TNS_ADMIN' not in os.environ:
    tomb.tombstone("Setting TNS_ADMIN")
    os.environ['TNS_ADMIN'] = os.environ['ORACLE_HOME'] + "/network/admin"


# Now that we have found the location of the site packages, let's append that
# useful bit of info to the PYTHONPATH
# We need the locations for this project, no matter what.
os.environ['PYTHONPATH'] = (os.pathsep).join([
    os.environ['CANOE_HOME'] + "/src", 
    os.environ['CANOE_HOME'] + "/src/urlib", 
    os.environ['CANOE_HOME'] + "/src/plugins",
    os.environ['CANOE_HOME'] + "/src/canoelibs" 
    ])

# And now, the denouement of so many of us, we cope with the virtual environment.
if v_env == 1 and 'VIRTUAL_ENV' in os.environ:
    tomb.tombstone("Attempting to add the VIRTUAL_ENV to PYTHONPATH")
    cmd = "find " + os.environ['VIRTUAL_ENV'] + ' -name site-packages | head -1'
    p = subprocess.Popen([cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if out is not None and len(out) > 0:
        locations.append(str(out))


if __name__ == '__main__':
    print('-# Environment #-')
    for k, v in sorted(os.environ.items()):
        print("{} = {}".format(k,v))
else:
    pass
