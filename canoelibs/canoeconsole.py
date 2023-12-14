# -*- coding: utf-8 -*-
import typing
from   typing import *

""" 
CanoeConsole is a class derived from cmd that supports ad hoc
operation of the Canoe functions, starts and stops the background
process ("daemon"), and supports scriptable operation.
"""

import calendar
import cmd
import collections
import datetime
import difflib
import glob
import pprint
from   pprint import PrettyPrinter
import os
import psutil
from   urutils import setproctitle, getproctitle
import shlex
import signal
import socket
import stat
import subprocess
import sys
import time


from   canoebrowser import ScheduleBrowser
import canoedb as cdb
import canoelib as cl
import fifo
from   fifo import FIFO
from   fname import Fname
import hop
import jparse as jp
import loader
from   sfy import rfy
import tombstone as tomb
import urutils as uu

if uu.in_production():
    from urdecorators import show_exceptions_and_frames as trap
else:
    from urdecorators import null_decorator as trap

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = 2.71828 
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Prototype'


__license__ = 'MIT'
import license

BLUE = '\033[94m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
PURPLE="\033[35m"

colors = {
    'blue':BLUE, 'green':GREEN, 'yellow':YELLOW, 
    'red':RED, 'purple':PURPLE
    }

ENDC = '\033[0m'
BOLD = '\033[1m'
REVERSE = "\033[7m"
REVERT = "\033[0m"
BLINK = "\033[5m"
UNDERLINE = '\033[4m'

indent="     "
banner = [
    "",
    ' ',
    '='*80,
    indent + ' ',
    indent + '                            /',
    indent + '                /\\_'+uu.blind(RED+'Canøe__/_20'+ENDC) + '_/\\',
    indent + '   ~~~    ~^~~~~\\_________/_______/~~^~~^~~~~',
    indent + '               ~~~~  ~~~ /~    .       ~~~   ~~',
    indent + '....................~~^~/.....................><{{{{°><...........',
    indent + '..'+BLUE+'<°}}}}>'+REVERT+'...                                        ..............',
    indent + '............  '+ GREEN + 'All your integration are belong to us.' + REVERT + '  ............',
    indent + '.............                                        .............',
    indent + '...................'+PURPLE+'><{{{°>'+REVERT+'..............................><(((°x...',
    indent + '',
    indent + " Don't know what to do? Afraid to ask for help? Well, type 'help'",
    indent + " or 'pray.' There .... now how hard is that?",
    indent + " ",
    indent + " TO GET AN OVERVIEW, type 'general' and read the FAQ that will",
    indent + "    be displayed.",
    indent + ' ',
    '='*80,
    ""
]

# These statuses are an expansion on the available statuses defined
# in the Linux top utility.
statuses = { 
    "sleeping":"S", 
    "dead":"D", 
    "waking":"W", 
    "zombie":"Z",
    "running":"R", 
    "disk-sleep":"O", 
    "stopped":"T", 
    "tracing-stop":"/" 
    }

canoe_processes = {
    "canoe:console":"This program. Allows interaction with the daemons.",
    "canoearkd":"Interfaces with CyberArk.",
    "canoed":"The execution daemon. Bound to a single OAQ. Forks for each task.",
    "canoelockd":"Processes to keep network shares active.",
    "canoelogd":"Writes the logfiles and the database tables that contain logging info.",
    "canoenagd":"Constructs packets for Nagios IX.",
    "canoeschedd":"Process to run the tasks that require a schedule. Bound to a single OAQ.",
    "canoeups":"The file delivery daemon (with retry).",
    "dinghyd":"Parent process forks children, each one monitoring a separate event."
    }

# These processes use a named and shared OAQ. 
canoe_processes_q = ['canoed', 'canoeschedd', 'dinghyd']


@trap
def isrunning(process_name:str='canoed') -> str:
    """ 
    Linux only function to determine if a process is running.

    process_name -- A name, or fragment thereof, that interests you.
    returns: -- a list of qualifying processes.
    """

    try:
        result = subprocess.check_output(['/sbin/pidof', "-s", process_name])
        # print(result)
    except subprocess.CalledProcessError:
        return 'No process named ' + uu.blind(process_name) + ' found.'
    return result.decode('utf-8').strip()


@trap
def isrunning_message() -> str:
    pid = isrunning()
    msg = " "
    try:
        _ = int(pid)
        msg = msg + "The canoe daemon is running"
    except:
        msg = msg + " **** The canoe daemon is NOT running. **** "

    return msg


@trap
def xlate(s:str) -> str:
    try:
        x = cxx.CanoeCrypter()
        s = x.decript(s)
    except Exception as e:
        tomb.tombstone("Cannot continue; unable to open Canoe's database.")
        exit(1)
    else:
        return s


class CanoeConsole(cmd.Cmd):
    """ A simple, text based console with arg parsing and help. """

    intro = rfy(banner)
    prompt = "\n" + '[ /\_Canøe_/__/\ ]: '
    use_rawinput = True
    doc_header = PURPLE + "A list of Canøe commands, each of which has help." + REVERT
    doc_header += '\nIf you need to generally know what is going on generally, try "helpme"'


    @trap
    def __init__(self):
        cmd.Cmd.__init__(self)
        calendar.setfirstweekday(calendar.SUNDAY)
        self._color = GREEN
        self.sn = uu.new_serial_number()
        self.start_time = time.time()
        self.version = uu.version(False)
        self.recipe_names = None
        self.recipes = {}
        self.schedules = {}
        self.command_names = None
        self.columns = uu.columns()
        self.g = uu.SloppyDict()
        self.db = None
        self.active_pipe = None

        self.g['verbose'] = '-v' in sys.argv
        _ = subprocess.call('clear',shell=True)


    def __bool__(self) -> bool:
        return True


    def __str__(self) -> str:
        return str(self.__dict__)


    def _get_credentials(self) -> dict:
        """
        This strategy is a kind of thunking layer that we provide for the
        purpose of rolling your own solution. One possibility is to store
        the credentials encrypted, in a database table. Other solutions
        are the Oracle Wallet, or perhaps a non-Oracle solution like 
        CyberArk.
        """
        return self.db.get_credentials()


    @trap
    def precmd(self, s:str="") -> str:
        """
        Process all user input in a standard way. Let's pretend that it
        is bash command line stuff.
        """
        return s


    # Three special functions: preloop(), postloop(), and emptyline()

    @trap
    def preloop(self) -> None:
        """ 
        The startup code is in this function. In addtion to setting
        the display [back] to its default colors of green-on-black, this
        function does some other important things:

        1. SIGHUP is intercepted, and mapped to the emptyline function. At
            some point we might change this, but for now....

        2. We print the version of python rather bluntly on the screen
            because on most machines "python" is either a sym link or
            something in the /usr/bin/alternatives directory.

        3. Using the socket.getfqdn interface, we attempt to decide
            whether we are in production or not.

        4. We pre-parse all the things that appear to be recipes.

        """

        setproctitle('canoe:console')

        # And get our output presentable.
        p = PrettyPrinter(indent=4, width=100, compact=True)

        signal.signal(signal.SIGHUP, self.emptyline)
        x = sys.version_info
        time.sleep(0.25)

        host = socket.getfqdn().replace('-','.')
        user = uu.me()

        files = []
        CONFIGDIR = os.environ.get('CONFIGDIR', '/sw/canoe/compilerconfig')
        for r, ds, fs in os.walk(CONFIGDIR, followlinks=True):
            files.extend(
                [ os.path.join(r, _) for _ in fs if _.endswith('.json') ]
                )

        num_processed = 0
        for i, _ in enumerate(files):
            result = self.add_config(_)
            if result is not None: num_processed += 1
            for k in list(result.keys()): self.g[k] = result[k]

        self.g = uu.deepsloppy(self.g)
        uu.tombstone(f'{num_processed} of {i+1} config files loaded.')

        # Fix a few globals 
        if not hasattr(self.g.sys_params, 'niceness'):
            self.g.sys_params['niceness'] = 10

        time.sleep(0.25)

        # gkf Changed the method of identification on 23 October 2015
        # gkf Changed the method of idenfitication to reflect new network
        #     naming conventions. 18 September 2019
        this_env = ''
        for k in self.g.env:
            if host in self.g.env[k]:
                this_env = k
                tomb.tombstone("This environment is {}".format(uu.blind(k)))
                break
        else:
            this_env = 'unknown'

        time.sleep(0.25)
        env_string = uu.blind(this_env) if this_env == 'prod' else this_env
        CanoeConsole.prompt = "\n" + '[ /\_Canøe/' + env_string + '_/__/\ ]: '

        
        count, self.recipes = loader.loader(
            os.environ.get( 'COMPILEDRECIPES', '/sw/canoe/compiledrecipes')
            )
        self.recipe_names = list(self.recipes.keys())
        self.schedules = { k: v.schedule for k, v in self.recipes.items() }
        for k, v in self.schedules.items():
            for i, line_item in enumerate(v):
                for j, element in enumerate(line_item):
                    if not element: v[i][j] = uu.Universal()
        self.command_names = [ _[3:] for _ in dir(self) if _.startswith('do_') ]

        time.sleep(0.25)

        try:
            self.db = cdb.default()
            self.sn = uu.new_serial_number()
            os.environ['sn'] = self.sn
            tomb.tombstone("console:begin-job")

        except Exception as e:
            tomb.tombstone(uu.type_and_text(e))
            exit(1)

        # See if there are recent errors, and print them. This is the most
        # likely reason someone is using this program.
        recent_window = self.g.sys_params['recent_in_minutes']

        time.sleep(0.25)
        print("Type '{}' to find out what is currently running.".format(
            uu.blind('top')))

    @trap
    def add_config(self, filename:str) -> int:
        """
        Open a file, assume it is JSON, and build an object. Combine
        the keys from the object with the existing one.

        filename -- a FQN
        returns: -- 1 if file was added
                    0 if file was not a data object
                    None if there was a syntax error.
        """
        o = None
        try:
            json_reader = jp.JSONReader()
            o = json_reader.attach_IO(filename, True).convert()
            return o

        except Exception as e:
            uu.tombstone('{} contains a syntax error.'.format(filename))
            uu.tombstone(uu.type_and_text(e))
            return None

    @trap
    def postloop(self) -> None:
        """ The shutdown code (for the console only) is in this function. """

        self.db.end_job(self.sn, "console:"+uu.me(), "postloop")


    @trap
    def default(self, args:str='') -> None:
        if args: args=uu.listify(args)
        if args[0] == 'EOF':
            self.do_exit(args)
        else:
            print(f'unknown command {args}')


    @trap
    def emptyline(self) -> None:
        """ 
        Don't repeat the previous command, just do nothing. In the
        cmd base class, the default behavior is "redo." 
        """
        return []


    @trap
    def _log(self, target:str, message:str='') -> None:
        try:
            tomb.tombstone(message)

        except Exception as e:
            print(uu.type_and_text(e))
            tomb.tombstone(uu.dump_exception(e))
            return
        

    ############################################################################
    # Our commands, in alpha order.
    ############################################################################

    @trap
    def do_addcred(self, args:str=None) -> None:
        """
        Add a credential to the database. To do so, you must know the following:

        The name of the global object to which it belongs (usually a recipe).
        The name of the key (usually something like "password")
        The value of the key in plain text. Quotes and such will be assumed to be 
            part of the value.
        Up to 200 chars of description to help you remember what you did. This field
            is not encrypted, so don't put your password hints here. 

        Type 

            addcred recipe-name [unencrypted]

        and it will prompt you for the rest. If you typo, just press Control-C and
        try it all again. 

        Use 'unencrypted' if you want to just store something in the table 
        (example: file-dinghy)
    
        """  
        if not args:
            self.do_help('addcred')
            return 
        plain_text = 'unencrypted' in args  

        k_family = args[0]
        try:
            k, _ = uu.parse_user_input(input('Name of key: '))
            v, _ = uu.parse_user_input(input('Value associated with key: '))
            desc, _ = uu.parse_user_input(input('Anything you want to say about it? '))

        except KeyboardInterrupt as e:
            print("Whoa! You asked pressed control-C.")
            return

        except EOFError as e:
            print("No input. Aborting.")
            return

        desc = " " if not len(desc) else " ".join(desc)
        if len(k) * len(v) == 0: return self.do_help('addcred')
        k = k[0]
        v = v[0]
        
        try:
            self.db.add_credential(self.sn, k_family, k, v, desc, plain_text)
        except Exception as e:
            tomb.tombstone(str(e))

        return


    @trap
    def do_color(self, arg:str="") -> None:
        """ 
        Syntax: color { red | yellow | green | blue } 

        This is a fragile command, and has no use other than testing the
        operation of the audit log with a harmless record of activity.
        """
        global colors
        if not arg: arg = 'green'
        choice = colors.get(arg[0], GREEN)

        print(self._color)
        self._log('color:' + arg)
        return


    @trap
    def do_exit(self, args: str=None) -> None:
        """ 
        Exits the console program, and leaves any background processes
        running. 
        """
        tomb.tombstone("Console terminated normally.")
        sys.exit(os.EX_OK)


    @trap
    def do_forecast(self, line:str="") -> None:
        """
        Show the prev/next items to do. The format is
            forecast {t1} [t2] [jobname]
 
        where t1 and t2 are relative minutes to now. They can 
        be in either order. If t2 is omitted, it is assumed to be now.

        Thus:
            "forecast -20" shows the preceding 20 minutes.
            "forecast 50 -20" show the preceding 20 minutes and the next 50.

        If the name of a job is included, it is assumed that you are 
            asking when in the interval described the job ran, or will be
            run.

        Thus:

            "forecast 1440 cigna-health" will give the time when the 
                cigna-health job will be run in the next 24 hours.

            "forecast -2880 bb_courses" will tell when in the past two
                days the bb_courses job was run.
        """
        tokens = line.strip().split()
        now = int(time.time())
        if not tokens: self.do_help('forecast'); return

        token_types = [ None, None, None ]
        token_values = [ None, None, None ]
        for i, token in enumerate(tokens):
            if i >= len(token_types): 
                print("extra token ignored.")
                break
            try:
                _ = int(token)
                token_types[i] = int
                token_values[i] = _ * 60 + now
            except:
                token_types[i] = str
                token_values[i] = token

        patterns = [
            [ int, None, None ],
            [ int, int, None ],
            [ int, str, None ],
            [ int, int, str ]
            ]
        
        if token_types == patterns[0]:
            token_values[1] = now
            pattern = 0
        elif token_types == patterns[1]:
            pattern = 1
        elif token_types == patterns[2]:
            token_values[2] = token_values[1]
            token_values[1] = now
            pattern = 2
        elif token_types == patterns[3]:
            pattern = 3
        else:
            self.do_help('forecast')
            return
        
        if token_values[0] > token_values[1]: 
            token_values[0], token_values[1] = token_values[1], token_values[0]
            
        all_schedules = self.schedules
        print("\n" + str(len(all_schedules)) + " schedules found.")
        results = collections.defaultdict(list)
        
        for t in range(token_values[0], token_values[1], 60):
            results[datetime.datetime.fromtimestamp(t)] = cl.whats_next(all_schedules, t)

        print("\n")
        if pattern in {2,3}:
            results = {k:v for k,v in results.items() if token_values[2] in str(v) }

        for _ in sorted(results):
            print(str(_)[:-3] + " -> " + ", ".join(results[_]))


    @trap
    def do_review(self, args:str="") -> None:
        """ 
        Display information about a job.

        Syntax: review {serial number | hash}

        1. The serial number, if supplied will be matched to "serial numbers that
            begin with ...." 
        2. The hash is the last four characters of the serial number. We will
            look backward through the log file from the present and retrieve the
            first job whose hash matches.
        """

        if not args:
            self.do_help('review')
            return

        use_sn = len(args) > 4 

        SQL = {
            True  : f"SELECT * FROM canoe_log WHERE serial_number LIKE '{args}%'",
            False : f"""SELECT * FROM canoe_log 
                WHERE serial_number LIKE '%{args}' """
            }
        order = " ORDER BY serial_number, sequence_number ASC"

        QUERY = SQL[use_sn] + order
        frame = self.db.execute_SQL(QUERY)
        if frame.empty:
            print('Nothing matching your search criteria')
            return 

        start = frame.iloc[0]['microtime']
        for i in range(frame.shape[0]):
            row = frame.iloc[i]
            elapsed = round(row['microtime'] - start, 3)
            print(f"{elapsed:7.3f} {row['message']}")


    @trap
    def do_license(self, args:str="") -> None:
        """
        Print the license.
        """
        print(license.__license_text__)


    @trap
    def do_listpipes(self, arg:str="") -> None:
        """
        Show all the pipes and sockets that are in use.

        Syntax:
            listpipes

        """
        d = os.environ.get('PIPEDIR', '/sw/canoe/pipes')
        print(f"Looking in {d} for pipes.\n")
        print("Pipe name      Created/Modified     Last access (sec/min)")
        print("-"*60)

        now = time.time()
        with os.scandir(d) as iter:
            for candidate in iter:
                if stat.S_ISFIFO(candidate.stat().st_mode): 
                    name = candidate.name
                    info = candidate.stat()
                    created = str(datetime.datetime.fromtimestamp(info.st_ctime))[:19]
                    last_read_s = round(now - info.st_atime, 1)
                    last_read_m = round((now - info.st_atime)/60, 2)
                    print("{:<14} {:<20} {:>10} / {:>10}".format(
                        candidate.name, created, last_read_s, last_read_m))
    
        return


    @trap
    def do_newpipe(self, arg: str='') -> None:
        """
        Create a new pipe

        Syntax:
            newpipe {pipe_name}

        Creates the named pipe. If the pipe already exists, the
        command gleefully declares success.

        """
        if not arg:
            self.do_help('newpipe')
            return

        try:
            os.mkfifo(os.path.join(os.environ.get('PIPEDIR', '/sw/canoe/pipes'), arg), 0x600)
            print(f"named pipe {arg} created.")

        except FileExistsError as e:
            print(f"Pipe {arg} already exists.")
            pass

        except Exception as e:
            tomb.tombstone(str(e))
            print(f"failed to create {arg}")
            


    @trap
    def do_deletepipe(self, arg:str='') -> None:
        """
        Syntax:
            deletepipe {pipename}

        Deletes the queue, and the underlying queue table.
        """
        if not arg:
            self.do_help('deletepipe')
            return
        
        try:
            os.unlink(os.path.join(os.environ.get('PIPEDIR', '/sw/canoe/pipes'), arg))
        except FileNotFoundError as e:
            print(f'no pipe named {arg}')
        except Exception as e:
            tomb.tombstone(str(e))
            print(f"failed to remove {arg}")
            

    @trap
    def do_quit(self, arg: str=None) -> None:
        """ Same as exit. """

        return self.do_exit()


    @trap
    def do_show(self, args:str=None) -> None:
        """ 
        Show something about Canøe's environment.

        Syntax: show { host | hosts | items | loaded | tunnels
            | schedule | urlib | version | memory | modules
            | name-of-something-else-in-the-config }

        gpg [full]         -- formatted dump of known possible recipients.
            The optional term 'full' will show the key fingerprints
            as well as the owners.
        host {hostname}    -- shows connection details for that host.
        hosts              -- shows the network hosts known to Canøe.
        items [-1] [prefix]  -- lists top level, global objects of all kinds, 
            including config items and recipes.
             The optional parameter "-1" lists them without decorations or 
             comments, much like "ls -1" on the command line.
             The optional parameter "prefix" allows you to list & search
             for matching items all in one operation.
        loaded             -- shows an alphabetized list of the modules
            currently in memory.
        memory             -- shows the memory usage.
        name-of-something  -- Hey, if you know the name, try showing it!
        new                -- describe new(er) features.
        tunnels            -- shows open tunnels 
        urlib              -- shows mtimes for the Canøe modules we call urlib
        version            -- shows the current version of **Canøe**.
        """

        if not args: return self.do_help('show')
        args = shlex.split(args)
        pp = PrettyPrinter(indent=4, width=self.columns, compact=True)

        action = args[0]
        self._log('show:'+action)


        if "commands".startswith(action):
            for _cmd in self.get_names():
                if _cmd[:3] == "do_": print(_cmd[3:])
            return

        if "version".startswith(action):
            return self.do_version()

        elif 'new'.startswith(action):
            print("""
    [1] A new command, top, will show you what is running and the
        status of the current logfiles.
    [2] A new command, bounce, will send a signal to all Canøe processes
        that causes them to 'reload their configurations.'
    [3] The daemons can now be /correctly/ started and stopped within
        this program. No more bash scripts.
    [4] Recipes will now manipulate files in their own directories
        instead of abandoning them in $PWD.
    [5] There is a process throttle in canoed to prevent launching too
        many concurrent processes for the size of this machine.
    [6] The console (this program) is now aware of the screen width. 
    [7] Most subcommands are recognized by the first few letters. There 
        is always some ambiguity, so if you don't get the desired result
        type out the subcommand name more completely.
            """)        

        elif 'credentials'.startswith(action):
            k_family = None
            k = None
            if len(args) == 1: 
                pp.pprint(self._get_credentials())
                return 

            elif len(args) == 2:
                k_family = args[-1]
            else:
                k = args[-1]
                k_family = args[-2]
            results = self.db.get_credentials_by_name(self.sn, k_family, k)
            if not results:
                print("Nothing found matching " + str(k_family) + " and " + str(k))
                return

            pp.pprint(results)
            return
            

        elif "forecast".startswith(action):

            try:
                look_ahead = 60 * int(args[1])
            except:
                look_ahead = 60

            stride = 1 if look_ahead > 0 else -1

            now_nearest_minute = ( time.time() // 60 ) * 60
            now_tuple = uu.crontuple_now()

            match = None if len(args) < 3 else str(args[2])

            # TODO: the efficiency of this nested loop could be improved!
            forecast_events = []
            for i in range(0, look_ahead, stride):
                for name in sorted(self.g.keys()):

                    # Skip any "test" recipes that might have a valid schedule.
                    # They ain't gonna be in the forecast. Period.
                    skip = False
                    for _ in self.g.sys_params['test_predicate']:
                        if _ in name: skip = True
                    if skip: continue
                    if match and not match in name: continue

                    # Let's future proof the code a bit. At the moment only
                    # recipes have schedules, but rather than try to locate
                    # compiled recipes by object type, let's check every object
                    # to see if the object simply *has* a schedule. 
                    try:
                        obj = self.g.get_by_name(name)[0]
                    except Exception as e:
                        continue
                    if not obj or not hasattr(obj, 'schedule'): continue
    
                    next_time = now_tuple + datetime.timedelta(minutes=i)
                    if uu.schedule_match(next_time.timetuple(), obj.schedule):
                        forecast_events.append(next_time.isoformat().replace('T','   ') + 
                            "   " + name)
            print(" ")
            print("DATE         TIME       RECIPE NAME")
            print("----------------------------------------------------------------")
            print("\n".join([_ for _ in forecast_events]))

        elif "gpg".startswith(action):
            e = cl.CanoeGPG(self.g)
            keys = e.list_possible_recipients()
            rev_dict = { _['uids'][0] : _['keyid'] for _ in keys }
            print(" ")
            if len(args) > 1 and args[1] == 'full':
                for k in sorted(rev_dict.keys()):
                    print(str(k)[:60].ljust(60) + 
                        " <=> {} {}".format(rev_dict[k][:8], 
                                rev_dict[k][8:])
                        )
            else:
                for owner in sorted([ _['uids'][0] for _ in keys ]):
                    print(owner)

        elif 'host'.startswith(action):
            if len(args) < 2: return self.do_help('show')
            print(" ")
            try:
                host_info = uu.get_ssh_host_info(args[1])
            except:
                host_info = None
            if host_info is None:
                print(args[1] + " is not a known host.")
                return

            for k, v in sorted(host_info.items()):
                print(str(k) + " <=> " + str(v))

            f = Fname(host_info['controlpath'])
            if not f:
                print("\nNo connection to" + uu.blind(args[1]) + "is currently open.")
            else:
                print("\nA Layer 3 connection to" + uu.blind(args[1]) + "is open.")


        elif 'items'.startswith(action):
            # 11 Februrary 2016 feature fix by gkf
            # 15 June 2019 .. changed to show recipes and data in the
            #   new Canoe-19 world. gkf
            # 
            # show items -1 [prefix] 
            #
            # Now works much like the shell command, ls
            #
            # ls -1 [filespec]
            #
            if '-1' in args: bare = True
            else: bare = False

            if len(args) < 2:
                match = None
            elif bare and len(args) < 3:
                match = None
            else:
                match = args[-1]
                if match.endswith('*'): match = match[:-1]
            first_letter = ' '
            ks = sorted(list(self.g.keys()) + self.recipe_names)
            is_recipe = uu.blind("R")

            recipe_names = self.recipe_names

            cols = uu.columns()
            for k in ks:
                if match and not k.startswith(match): continue
                if bare:
                    print(k)
                    continue
                c = k[0]
                if c != first_letter:
                    print(uu.blind(" " + c + " "))
                    first_letter = c
                if k in recipe_names:
                    r = self.recipes[k]
                    desc = " {} {}".format(
                        r.get('owner', 'no owner'),
                        " ".join(r.get('comment', ['missing comment']))[:cols-55]
                        )
                    pad = "  " if len(k) < 26 else "* "
                    print("    " + k[:25].ljust(25) + pad + is_recipe + str(desc))
                else:
                    print("    {}  (DATA)".format(k))

            if not match:
                print("\n" + uu.blind(' tunnels '))
                return self.do_show('tunnels')

        elif 'hosts'.startswith(action):
            hosts = uu.get_ssh_host_info()
            if hosts is not None:
                hostnames = sorted(iter(hosts.get_hostnames()))
            for _ in hostnames:
                if _ != "*": print(_)

        elif "memory".startswith(action):

            all_pids = []
            anywhere = True
            for name in ['canoed','canoeschedd','dinghyd']:
                all_pids.extend(uu.pids_of(name, anywhere))

            mem_info = {}
            for pid in all_pids:
                mem_info[str(pid)] = uu.parse_proc(pid)

            interesting_keys = ['Size   ', 'Lck    ', 'HWM    ', 
                                'RSS    ', 'Data   ', 'Stk    ', 'Exe    ', 'Swap   ' ]
            print('            '+" ".join(sorted(interesting_keys)))
            for pid in all_pids:
                print(str(pid).ljust(7, ' ') + ": " + 
                    " ".join([str(v).rjust(7, ' ') for k, v in 
                    sorted(mem_info[str(pid)].items())]))


        elif 'loaded'.startswith(action):
            print("\t".join(
                sorted(
                    [i for i in list(sys.modules.keys()) if "." not in i])))
#            for _ in sorted([i for i in list(sys.modules.keys()) if "." not in i]):
#            for _ in sorted([i for i in list(sys.modules.keys())],
#                            cmp=lambda x,y: cmp(x.lower(), y.lower())):
#                print (" " + str(_))


        elif 'schedule'.startswith(action):
            end = self.g.sys_params['recent_in_minutes']
            begin = -1 * end 
            if len(args) == 2:
                try:
                    begin=int(args[1])
                except:
                    print("Lower bound must be a number (of minutes)")
                    return
            elif len(args) == 3:
                try:
                    begin = int(args[1])
                    end = int(args[2])
                except:
                    print("The upper and lower boundaries must be numbers.")
                    return
            
            sched = ScheduleBrowser(begin, end)
            if sched.get_schedule(self.g): 
                print("\n\t\t" + isrunning_message() + "\n")
                print(str(sched))
                print("\n\t\t" + isrunning_message())

            return


        elif 'urlib'.startswith(action):
            urlib = os.environ['CANOE_HOME'] + "/src/urlib/"
            print(rfy(sorted([(f + "\t => " + 
                time.ctime(os.path.getmtime(urlib+f))).expandtabs(20) 
                for f in os.listdir(urlib) 
                    if os.path.isfile(os.path.join(urlib,f)) and f.endswith("py")])))


        elif 'tunnels'.startswith(action):
            names = [ _ for _ in os.listdir('/tmp') if _.startswith('ssh-') ]
            i = 0
            if not names:
                print("\n There are no currently active layer 3 connections.")
            else:
                print("\n Layer 3 connections:\n")
                
                here = self.g.sys_params['default_domain']
                internal = [ _ for _ in names if here in _ or '.' not in _ ]
                external = [ _ for _ in names if here not in _  and '.' in _ ]

                print("  Internal\n")
                for _ in internal:
                    i += 1
                    try:
                        x, y = _.split('-', 1)
                        user, z = y.split('@', 1)
                        host, port = z.split(':', 1) 
                    except:
                        continue           
                    print("\t" + str(i) + uu.blind(host) + "as " + user + " on port " + port) 

                print("\n  External\n")
                for _ in external:
                    i += 1
                    try:
                        x, y = _.split('-', 1)
                        user, z = y.split('@', 1)
                        host, port = z.split(':', 1) 
                    except:
                        continue           
                    print("\t" + str(i) + uu.blind(host) + "as " + user + " on port " + port) 

        elif args[0] in self.recipe_names:
            objs = {k:v for k, v in self.recipes.items() if k.startswith(args[0]) }
            for k, v in objs.items(): pp.pprint(v)

        elif args[0] in self.g:
            objs = {k:v for k, v in self.g.items() if k.startswith(args[0]) }
            for k, v in objs.items(): pp.pprint(v)

        else:
            print("Cannot find {} to show it.".format(args[0]))

        return


    @trap
    def _start_stop_args(self, who:str, args:List[str]) -> tuple:
        """
        Returns a tuple with service_name, queue_affinity.
        """
        if len(args) < 2: args.append(self.active_pipe.upper())
        else: args[1] = 'CANOE_QUEUE_{}'.format(args[1].upper())

        # Normalize the service name that we are trying to start.
        candidate_names = []
        for _ in canoe_processes:
            if _.startswith(args[0]): 
                candidate_names.append(_)

        if not candidate_names:
            tomb.tombstone('Unknown service {}'.format(args[0]))
            return None

        if len(candidate_names) > 1:
            tomb.tombstone('Ambiguous name {} matches {}'.format(args[0], candidate_names))
            return None

        return candidate_names[0], args[1], ":".join(args)

        
    @trap
    def do_time(self, arg: str=None) -> None:
        """ 
        Print the current time in three formats: seconds, UTC,
        localtime. Also prints out how long the console has been 
        running ... in case you forgot it was running at all.
        """

        x = time.gmtime()
        y = time.localtime()
        print("Console started " + str(round(time.time() - self.start_time, 2)) + " seconds ago.")
        print(str(round(time.time(), 3)) + " seconds.")
        print(time.strftime("%a, %d %b %Y %H:%M:%S UTC", x))
        print(time.strftime("%a, %d %b %Y %H:%M:%S Local time", y))

    
    @trap
    def do_top(self, args:str='') -> None:
        """
        Give a summary of Canøe's activities in a format similar to the
        Linux utility, top. This command, however, only runs once rather 
        than looping until you stop.
        """

        screen_header = [
"""[?] [ pid] [ppid] [MM-DD HH:MM:SS] [cpu S] [mem K] [name of the process ****************************]\n""",
"""0123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789""" 
            ]

        screen_format = """{0[0]: ^3} {0[1]:>6} {0[2]:>6} {0[3]: ^16} {0[4]: >7} {0[5]: >7} {0[6]}"""
            
        files_header = """[file size] [sec ago] [filename***********************************]"""
        files_format = """ {: >9}   {: >7.2f}   {}"""

        @trap
        def handler(signum:int, stack:object=None) -> None:
            """
            Signals are sent by the canoed process(es) when new tasks are created. This
            way we always see them even when they run in less time than the sleep window.
            """
            tomb.tombstone('Received signal {}.'.format(signum))
            if signum in [signal.SIGUSR1, signal.SIGUSR2]:
                self.do_top()

            else:
                tomb.tombstone("ignoring signal {}. Did you forget something?".format(signum))
                pass

        try:
            logs = {}
            screen_lines = [screen_header[0]]
            procs = uu.pids_of(uu.me(), True)

            print("{} procs.".format(len(procs)))

            # and the logfile data
            log_files = glob.glob(os.path.join(os.environ.get('CANOE_LOG'), '*20.log'))
            for _ in log_files: logs[_] = os.stat(_)

            for p in procs:
                try:
                    info = psutil.Process(p)
                    with info.oneshot():
                        info_line = (statuses[info.status()], p, info.ppid(), 
                            datetime.datetime.fromtimestamp(
                                info.create_time()).strftime("%m-%d %H:%M:%S"),
                            info.cpu_times().user, info.memory_full_info().uss >> 10, info.name())
                        if info.name() in ['bash', 'scl', 'vim', 'ssh']: continue
                        if info_line[0] == 'R':
                            screen_lines.append(
                                (REVERSE+screen_format+REVERT).format(info_line)
                                )
                        elif info_line[0] != 'S':
                            screen_lines.append(
                                (RED+screen_format+REVERT).format(info_line)
                                )
                        else:
                            screen_lines.append(screen_format.format(info_line))

                except psutil.NoSuchProcess as e:
                    # Process terminated before we collected data.    
                    pass
              
                except psutil.AccessDenied as e:
                    # We asked for info about a priv process.
                    pass


                except Exception as e:
                    """
                    If there is an error while collection process information, there
                    is exactly nothing we can do about it. So we carry on.
                    """
                    print(uu.type_and_text(e))

            screen_lines.append("\n"*2)
            screen_lines.append(files_header)

            for k, v in logs.items():
                screen_lines.append(files_format.format(v.st_size, time.time()-v.st_mtime, k)) 
                
            try:
                print("\n".join([ _[:self.columns] for _ in screen_lines]))

            except Exception as e:
                # continue
                print(str(e))
                pass

        except KeyboardInterrupt as e:
            print('You pressed control-C.')
            

    @trap
    def do_version(self, arg: str=None) -> None:
        """ 
        Prints the version .. that's all. 
        """

        print("You are running Canøe code from git commit " + uu.version())
        return


    @trap
    def do_whoami(self, arg: str=None) -> None:
        """
        Prints the thread id, and the PID of the console you are currently running.

        The purpose of this function is to make it clear which process is sending
        which message in this multi-threaded and multi-processing environment.
        """
        print("\nI cannot believe that you needed to ask. <sigh> ... \n")
        uu.whoami()
        

if __name__ == "__main__":
    pass
else:
    # print(str(os.path.abspath(__file__)) + " compiled.")
    print("*", end="")

