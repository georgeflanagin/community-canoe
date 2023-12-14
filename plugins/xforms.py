# -*- coding: utf-8 -*-
""" 
Canøe VM component to handle data transformations.
"""

import typing
from   typing import *

# System imports

import copy
import glob
import os
import os.path
import shlex
import subprocess
import sys
import tempfile

# Installed imports

import pandas

# Canoe imports

import canoestats
from   canoestats import LED
import fname
from   grammar import *
import hop
import pandas2xml
import pluginlib
import tombstone as tomb
import urbox as ux
import urpacker
import urutils as uu

if not uu.in_production():
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

debugging = True
@trap
def apply_file_labels(datafile:str, opcodes:uu.SloppyDict) -> bool:
    """
    Some files require a "label" on the front of the file, or at its end
    in addition to the CSV data. This is not the same as the CSV data
    having a header row.

    datafile -- the name of a file that already has CSV data in it.
    opcodes  -- the opcodes for the subroutine this function is executing.
        The opcodes have the relevant file info.
    """
    
    csv_info = opcodes.output.format
    if not hasattr(csv_info, 'footer'): 
        uu.tombstone(f"Safely skipping older object code {opcodes.local_dir}")
        return True

    has_header = csv_info.fixed_header is not None
    has_footer = csv_info.footer is not None

    fixed_header = ""
    footer = ""

    # Most CSV files do not require labels.
    if not has_header and not has_footer: 
        return True

    # If there is a header, read it. We need to append a newline
    # so that it doesn't run into the first data row.
    if csv_info.fixed_header:
        with open(csv_info.fixed_header) as f:
            fixed_header = uu.date_filter(f.read().strip()).format(csv_info.rows) + "\n"

    # Do the same for the footer. The data will end in a newline.
    if csv_info.footer:
        with open(csv_info.footer) as f:
            footer = uu.date_filter(f.read().strip()).format(csv_info.rows) 

    # And now, for a little sleight of hand. Let's exploit hard links in
    # the ext4 file system. Note that we have to use the dir= parameter
    # because we cannot create links across mount points.
    with tempfile.NamedTemporaryFile(mode='a', dir=opcodes.local_dir) as f_temp:

        # Bang down the fixed_header
        f_temp.write(fixed_header)
        
        # and continue with the file of CSV data.
        with open(datafile) as f:
            f_temp.write(f.read())

        # tack on the footer.
        f_temp.write(footer)

        # delete the original file.
        os.unlink(datafile)
    
        # create a hard link to the new file using the old name.
        # The OS will remove the tempfile when we leave the current
        # context, reducing the hard link count to one.
        os.link(f_temp.name, datafile)
    
    return True


@trap
def xforms_main(opcodes:list) -> ERROR_ACTION:
    """
    Execute the transformations to the data from previous steps.
    """
    global debugging
    stats = canoestats.default()
    mytype = 'xforms_std'
    tomb.tombstone(">>>>>>>>> XFORMS")

    # For each part of the xforms (and there is often only one)
    for i, subroutine in enumerate([uu.deepsloppy(_) for _ in opcodes]):
        debugging = subroutine.debug
        myname = uu.name_from_dirname(subroutine.local_dir)
        if not i: stats.update(myname, mytype, LED.ON)
        debugging and uu.tombstone(f"section {i+1} of {len(opcodes)} of {myname}.{mytype}")

        input_file = uu.expandall(uu.date_filter(subroutine.input.name))
        input_files = glob.glob(input_file)
        multiple_files = len(input_files) > 1

        # See if there is anything to do.
        debugging and uu.tombstone(f"testing empty for {input_files}")
        if pluginlib.test_empty(input_files, subroutine.on_error):
            stats.update(myname, mytype, LED.GREEN)
            return ERROR_ACTION.stop

        # There is at least one file to work on.
        tomb.tombstone(f'input_files {input_files}')

        output_file = uu.expandall(uu.date_filter(subroutine.output.name))

        if (subroutine.output.type in ['txt', 'msgpack'] and 
            not multiple_files and input_file != output_file): 

            try:
                os.rename(input_file, output_file)
                debugging and uu.tombstone(f"renaming {input_file} to {output_file}")
            except OSError as e:
                
                if e.errno == 39:
                    pass
                else:
                    stats.update(myname, mytype, LED.RED)
                    raise
                    

        elif subroutine.output.type == 'csv':
            debugging and uu.tombstone("found csv opcode.")

            csv_info = subroutine.output.format
            p = urpacker.URpacker()
            p.attachIO(input_file, s_mode='read')
            frame = p.read(format='pandas')

            # If frame is empty, and we were halfway expecting it...
            if ( frame.empty and 
                subroutine.on_error in 
                    [ ERROR_ACTION.test_empty, ERROR_ACTION.stop] ):
                stats.update(myname, mytype, LED.GREEN)
                return ERROR_ACTION.stop

            elif frame.empty:
                # It's empty, and we expected something to be there...
                stats.update(myname, mytype, LED.RED)
                tomb.tombstone('no pandas data in {}'.format(input_file))

            else:
                # We got data!
                column_names = None
                column_file = fname.Fname(f"{input_file}.columns")
                row_file = fname.Fname(f"{input_file}.rows")

                if column_file:
                    with open(str(column_file)) as cf:
                        column_names = cf.read().split('|')

                if row_file:
                    with open(str(row_file)) as rf:
                        try:
                            csv_info.rows = int(rf.read().strip())
                        except:
                            csv_info.rows = -1

                debugging and uu.tombstone("converting pandas DataFrame to csv")
                frame.to_csv(output_file, index=False,
                    header=csv_info.header,
                    quoting=csv_info.qforce,
                    columns=column_names,
                    sep=csv_info.sep, 
                    quotechar=csv_info.quote)
                debugging and uu.tombstone(f"{output_file} written.")
                stats.update(myname, mytype, LED.GREEN)

                if not apply_file_labels(output_file, subroutine):
                    stats.update(myname, mytype, LED.YELLOW)


        elif subroutine.output.type == 'xml':
            debugging and uu.tombstone("found xml opcode.")
            
            xml_info = subroutine.output.format
            tomb.tombstone("About to invoke pandas2xml plugin")
            xml_writer = pandas2xml.Pandas2XML(**xml_info)
            tomb.tombstone('Exporting data to XML file {}'.format(output_file))
            xml_writer.read_all()
            num = xml_writer.write()
            if debugging: 
                tomb.tombstone('wrote {} nodes.'.format(num))
                stats.update(myname, mytype, LED.GREEN)


        else:
            # TODO Other file types?
            pass

        # We don't always need to operate on the files.
        if not subroutine.ops: 
            debugging and uu.tombstone("No translation opcodes for this step.")
            continue

        operands = glob.glob(output_file)
        for f in operands:
            tomb.tombstone("operating on file {}".format(f))
            local_ops = copy.deepcopy(subroutine.ops)
            for op in local_ops:
                k, v = op.popitem()
                v = uu.date_filter(v)
                if k != XFORM_OPS['rename']:
                    cmd = shlex.split(" ".join([k, v, f]))
                else:
                    cmd = shlex.split(" ".join([k, v]))
                tomb.tombstone('executing {}'.format(uu.fcn_signature(*cmd)))
                results = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                tomb.tombstone("results of execution: {}".format(vars(results)))                

                if results.returncode == os.EX_OK: 
                    stats.update(myname, mytype, LED.GREEN)
                    continue

                if subroutine.on_error is ERROR_ACTION.proceed: 
                    stats.update(myname, mytype, LED.YELLOW)
                    continue
                elif subroutine.on_error is ERROR_ACTION.skip: 
                    stats.update(myname, mytype, LED.YELLOW)
                    raise uu.OuterLoop()
                elif subroutine.on_error is ERROR_ACTION.crash: 
                    stats.update(myname, mytype, LED.RED)
                    raise Exception().with_traceback(sys.exc_info()[2])

                return subroutine.on_error

    debugging and uu.tombstone(f"returning {ERROR_ACTION.proceed}")
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
    
    
