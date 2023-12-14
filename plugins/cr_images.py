# -*- coding: utf-8 -*-
""" 
Retrieve Chrome River images.
"""

import typing
from   typing import *

# System imports

import json
import os
import os.path
import shlex
import subprocess
import sys
import urllib
import urllib.parse

# Installed imports

# Canoe imports

import canoestats
from   canoestats import LED
import fname
from   grammar import *
import hop
import tombstone as tomb
import urbox as ux
import urdb
import urpacker
import urutils as uu

if uu.in_production():
    from urdecorators import show_exceptions_and_frames as trap
else:
    from urdecorators import null_decorator as trap

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2019, University of Richmond'
__credits__ = None
__version__ = '0.9'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'testable'

__license__ = 'MIT'

import license

chrome_url = 'https://{}/receipts/doit'
curl_prog = '/usr/bin/curl'
curl_opts = ' '.join([
    '--verbose',                            # help debug.
    '--location',                           # follow symbolic links.
    '--connect-timeout 5',
    '-X POST'
    ])
auth = {
    'un':'RichmondU',
    'pw':'zklV69TUDkPH'
    }

curl_args = {'method':'getReceipts'}
curl_args.update(auth)

curl_optional_args = {
    "getImage":True,
    "getMileageDetails":True,
    "getPDFReport":True,
    "getPDFReportWithNotes":True,
    "imageFirst":False
    }
curl_args.update(curl_optional_args)

@trap
def cr_images_main(opcodes:uu.SloppyDict) -> ERROR_ACTION:
    """
    Get the images from Chrome River, and put them in appropriate
    directories for later pickup.
    """
    global chrome_url
    stats = canoestats.default()
    mytype = 'xforms_custom'
    myname = uu.name_from_dirname(opcodes.local_dir)
    stats.update(myname, mytype, LED.ON)

    # Get a connection to the specified database.
    db = urdb.URdb(opcodes.db)
    chrome_url = chrome_url.format(opcodes.url)

    # These are specific tables and locations connected with this process,
    # and are therefore hard coded.
    indicator_field = 'onbase_feed_ind'
    banner_tables = ['cr_invoices', 'cr_pcard', 'cr_advances']
    destinations =  [uu.path_join(opcodes.dest_folder, _) 
        for _ in ['invoice', 'pcard', 'advances']]
    
    # Go through one type of "thing" to get at a time, noting where
    # it goes. The zip() operation associates each table with a 
    # corresponding folder. 
    for table, folder in dict(zip(banner_tables, destinations)).items():
        SQL = "SELECT DISTINCT report_id FROM {} where {} = 'N'".format(table, indicator_field)
        result = db.execute_SQL(SQL)
        report_ids = len(result)
        tomb.tombstone('Query returned {} rows from {}.'.format(report_ids, table))

        # If we don't have anything to get, move on to the next type of "thing."
        if not report_ids: continue

        # Saved to a disc file only for debugging purposes.
        # The next four lines can be removed at a later date.
        packer = urpacker.URpacker()
        t = uu.path_join(opcodes.local_dir, 'tempfile')
        packer.attachIO(t)
        packer.write(result)

        # Get the image[s] for each report_id. The single element data structure
        # returned from the database unfortunately looks like this:
        # [ {'report_id':'00107890'}, {'report_id','00107988'}, ..] 
        #
        # Let's extract the second item of each k/v pair into a separate list.
        # Might as well sort it while we are working on it.
        values = []
        for x in result:
            k, v = x.popitem()
            values.append(v)
        values = sorted(values)
        tomb.tombstone("Getting images for these report IDs {}".format(values))

        for report_id in values: 
            print("getting info for report ID {}".format(report_id))
            curl_args.update({'reportID':report_id})
            curl_data = '-d {}'.format(urllib.parse.urlencode(curl_args))
            query = ' '.join([curl_prog, curl_opts, curl_data, chrome_url])
           
            # curl me ...
            try:
                result = subprocess.run(shlex.split(query), 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE, 
                    timeout=opcodes.timeout)
            except subprocess.TimeoutExpired as e:
                print("Timed out....") 
                sys.exit(os.EX_DATAERR)

            # Make the name in chunks that will be useful a few lines from now.
            pdf_name = "{}.pdf".format(report_id)
            target_file = uu.path_join(opcodes.local_dir, pdf_name)
            with open(target_file, 'w+b') as f:
                f.write(result.stdout)

            got_it = 'Y' if uu.is_PDF(result.stdout) else 'E'
            SQL = "UPDATE {} SET {} = {} WHERE report_id = {}".format(
                table, indicator_field, uu.q1(got_it), uu.q1(report_id)
                )
            db.execute_SQL(SQL)

            if got_it == 'Y':
                try:
                    handle = hop.HOP(opcodes.host)
                    handle.send_one_file(target_file, pdf_name, True, folder)
                except Exception as e:
                    stats.update(myname, mytype, LED.YELLOW)
                    tomb.tombstone("Failed to send {}".format(pdf_name))
                    tomb.tombstone(uu.type_and_text(e))
            else:
                tomb.tombstone('Retrieved item for report ID {} was not a PDF'.format(pdf_name))

    stats.update(myname, mytype, LED.GREEN)
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

    print("\n")
    print("Compiler version {}".format(uu.compiler_info(opcodes)))
    print("Compiled on      {}".format(uu.compiled_time(opcodes)))
    print(80*'-')

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
        print("The program {} has no opcodes for the {} operation.".format(sys.argv[-1], vm_function))

    except Exception as e:
        print(uu.type_and_text(e))

    finally:
        print(80*'=')
    
