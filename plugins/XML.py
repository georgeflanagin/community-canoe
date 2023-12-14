# -*- coding: utf-8 -*-
""" 
Plugin template.
"""

import typing
from   typing import *

# System imports

import collections
import copy
import importlib as il
import json
import os
import os.path
import pprint
import sys
import xml.etree.ElementTree as ET

# Installed imports
import numpy
import pandas

# Canoe imports
import canoestats
from   canoestats import LED
import fname
from   grammar import *
import hop
import pluginlib
import tombstone as tomb
import urbox as ux
import urdb
from   urdecorators import show_exceptions_and_frames as trap
import urpacker
import urutils as uu


# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2020, University of Richmond'
__credits__ = None
__version__ = '0.9'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'testable'

__license__ = 'MIT'
import license

empty_frame = pandas.DataFrame()

################################################################################
# Finite State Machines are hard to debug. If you suspect something is wrong   #
# start by setting this variable to True, and using a small data set.          #
################################################################################
FSM_DEBUG = False 

if FSM_DEBUG:
    def debug_element(e:ET.Element, narrative:str="") -> None:
        tomb.tombstone("-"*10)
        if narrative: tomb.tombstone(narrative)
        tomb.tombstone(f"{str(e)}")
        tomb.tombstone(ET.dump(e))
        tomb.tombstone("="*10)
else:
    def debug_element(e:Any, s:str="") -> None:
        pass

################################################################################

debugging = True


def tag_fsm(datum:str,  
            rules:Union[uu.SloppyTree, None],
            e:ET.Element=None) -> ET.Element:
    """
    Finite State Machine for the tag grammar.
    
    datum -- An element that will appear in the .text part of the XML tag.
    rules -- An expression in the tag grammar.
    e     -- An existing XML Element, or a name that will be used to
            construct a top level element. 
    """
    global debugging

    ###
    # See if we are dropping this one. If there are no rules,
    # we really should be calling this function. However, the
    # inclusion of the test enables some testing operations.
    ###
    if rules is None: return None

    # See if we are starting with an element, a name, or nothing.
    if e is None: 
        e = ET.Element('NEW')
    elif isinstance(e, str):
        e = ET.Element(e)
    else:
        pass

    debug_element(e, 'e, at beginning of tag_fsm')

    ###
    # the tag name, and everything that goes in the tag.
    # we are guaranteed the keys are in insertion order,
    # so the views of keys and values are synchronized.
    ###
    iter_keys = iter(rules.keys())
    iter_values = iter(rules.values())
    sub_element = None

    while True:
        try:
            # Separate the tag_name and the rules we use. Each
            # k-v pair in the rules is a the name of an XML tag (k),
            # and the rules (v) that are used to construct any SubElements,
            # assign attributes, and or values to it.
            tag_name  = next(iter_keys)
            new_rules = next(iter_values)

        except StopIteration: # No more rules.
            break

        ###
        # Find out if there is already an element to which we 
        # can attach an additional [Sub]Element.
        ###
        pre_existing_element = e.find(tag_name)
        if pre_existing_element is not None:
            sub_element = pre_existing_element
        else:
            sub_element = ET.SubElement(e, tag_name)

        ###
        # The value is the .text part of the tag. If the value is
        # here, then this should be the end of the rule processing, 
        # because there is only one value, and only one place to put it
        # in the XML hierarchy.
        ###
        has_value = new_rules is not None and 'value' in new_rules
        if has_value:
            
            # If the value rule is empty, then we want to use the
            # datum that from the tabular data we read, and that
            # was an argument to this function.
            if not new_rules.value:
                sub_element.text = datum

            # If there is a lambda function as the value, then
            # we execute it, and use the result as the Element's
            elif isinstance(new_rules.value, collections.Callable):
                sub_element.text = new_rules.value()

            # The value is not empty, and not a function,
            # so we assign it.
            else:
                sub_element.text = new_rules.value

            # Because XML is text-only (everything is a string)
            # let's stringify.
            sub_element.text = str(sub_element.text)

        # Apply any attributes for this tag. Keep in mind that /any/
        # tag may have multiple attributes.
        if 'attribute' in new_rules:
            for k,v in new_rules.attribute.items():
                sub_element.set(k, v)

        # Print out what we have.
        debug_element(sub_element, 'sub_element, after manufacture')

        # An inner tag and the value should not both be present at the
        # same level. Raise an exception if the rules have this 
        # semantic error.
        if 'tag' in new_rules:
            if has_value:
                raise Exception(f'ERROR: tag attached to leaf {tag_name} in grammar.')
            if FSM_DEBUG: 
                tomb.tombstone(uu.fcn_signature(
                        'recursive call to tag_fsm', datum, new_rules.tag, sub_element
                        ))
            sub_element = tag_fsm(datum, new_rules.tag, sub_element)
            debug_element(sub_element, 'sub_element, after recursive call')        

    # Return the incoming tag, now with SubElements attached to it.
    debug_element(e, 'as we return from tag_fsm')
    return e
    
        
class Table2XML:

    default_header = '<?xml version="1.0" encoding="{}"?>'
    __slots__ = ( 'input', 'output', 'grammar', 'debug', 
        'tag_data', 'file_data', 'root', 'tree', 
        'frame_node', 'header', 'do_not_impute' )
    __defaults__ = ( None, None, None, None, 
        None, None, None, None, 
        None, default_header, [] )

    def __init__(self, **kwargs) -> None:
        """
        Set up the operations.

        kwargs -- data, and formatting instructions for the XML particulars. The
            useful values are fileheading (a tag to wrap the whole file) and
            the rowlabel (instead of the default literal "row").

            input -- your data to XML-ize OR the name of a file with the data.

            output -- the name of a file to contain the XML results.

            debug -- boolean that, when True, prints debug statements.

            grammar -- a mapping of names in the DataFrame to desired names
                of the XML elements. The default mapping maps the column 
                names onto themselves. The remap can be a list, in which
                case these are just the new names in the order in which
                the columns appear in the DataFrame. If the argument is a
                str, it is assumed to be the name of a packed file containing
                the definition of the mapping.

        """
        values = dict(zip(Table2XML.__slots__, Table2XML.__defaults__)) 

        ###
        # Set up the defaults.
        ###
        for k in Table2XML.__slots__:
            setattr(self, k, values[k])

        ###
        # And override them as required.
        ###
        args = [ setattr(self, k, v) 
            for k, v in kwargs.items() 
            if k in Table2XML.__slots__
            ]

        ###
        # Find out if we received data or the name of a packed file 
        # containing the data. If it is a filename, unpack it.
        ###
        if isinstance(self.input, str):
            if self.debug: tomb.tombstone("reading {}".format(self.input))
            p = urpacker.URpacker()
            if not p.attachIO(self.input, s_mode='read'):
                raise Exception("Could not read from {}".format(self.input))
            self.input = p.read(format='pandas')

        self.grammar = grammar = il.import_module(self.grammar)
        self.tag_data = grammar.tag_data
        self.file_data = grammar.file_data
        self.do_not_impute = grammar.do_not_impute

        if self.header == Table2XML.default_header:
            self.header = self.header.format('utf-8')

        ###
        # Get the XML tree happening. 
        ###
        self.root = ET.Element(self.file_data.frame_name)
        self.tree = None
        self.frame_node = self.root
        self.root.append(ET.Comment('Data generated by Canoe on {}'.format(uu.now_as_string())))

        self.pre_process()


    def __str__(self) -> str:
        """
        for ease in debugging.
        """
        s = ""
        for slot in Table2XML.__slots__:
            s += f"{slot} is\n"
            s += f"{getattr(self, slot)}\n"

        return s


    @trap
    def attachIO(self, filename:str) -> bool:
        """
        Prepare to write.
        """
        if self.output is not None:
            self.output.flush()
            self.output.close()

        self.output = None

        try:
            self.output = open(filename, mode='a+', encoding=self.encoding)
            if self.debug: tomb.tombstone(f'{self.output} opened for output.')
            return True

        except Exception as e:
            tomb.tombstone(str(e))
            sys.exit(os.EX_NOINPUT)
            return False


    @trap
    def pre_process(self) -> None:
        """
        Do the several steps before we begin looking at the XML generation.

        1. We need to add pseudo-columns (if any exist) to the DataFrame.
        2. We need to impute missing values.
        """
        
        for c in [ _ for _ in self.input.columns if _ not in self.do_not_impute]:
            if hasattr(self.tag_data[c], 'impute'):
                uu.tombstone(f"Imputing value for {c} = {self.tag_data[c].impute}")
                self.input[c] = self.input[c].replace(
                    r'^\s*$'
                    , self.tag_data[c].impute
                    , regex=True
                    )

            else:
                uu.tombstone(f"Imputing global value {c} = {self.tag_data.global_impute}")
                self.input[c] = self.input[c].replace(
                    r'^\s*$'
                    , self.tag_data.global_impute
                    , regex=True
                    )

        if hasattr(self.grammar, 'pseudo_columns'):
            for p_c in self.grammar.pseudo_columns:
                self.input[p_c] = ""

        uu.tombstone(f"{self.grammar.pseudo_columns} added to DataFrame.")

        uu.tombstone('Imputation complete')


    @trap
    def read_all(self) -> int:
        """
        Attempt to write all the data to the attached file.

        returns -- the number of records written.
        """
        return self.read_some(self.input.shape[0])
    

    @trap
    def read_some(self, start:int, stop:int=0) -> int:
        """
        num -- number of rows to write. 

        returns -- the number of rows written (might be less than
            the number requested).
        """
        if start > stop: 
            start, stop = stop, start

        i = 0
        try:
            for i in range(start, stop):
                self.read_one(i)

        except Exception as e:
            tomb.tombstone(str(e))
            return i

        else:
            return stop-start

    
    @trap
    def read_one(self, i:int) -> int:
        """
        read row i from the DataFrame, and add nodes to the XML doco.
        """
        if self.debug: tomb.tombstone(f'reading row {i} from the table.')
        try:
            row = self.input.iloc[i]
            if self.debug: tomb.tombstone(f'row {i} is {row}')
            # Create a node to contain the row.
            row_node = ET.SubElement(self.frame_node, self.file_data.row_name)

            # and add each column to it.
            for column, value in row.items():
                if not self.tag_data.get(column):  
                    if self.debug: tomb.tombstone(f"no rules for {column}, ignoring it.")
                    continue
                if self.debug: tomb.tombstone(f"processing {column} = {value}")

                # Call the FSM with the value, the rules, and this row's node.
                
                if self.debug: tomb.tombstone(uu.fcn_signature(
                    'outer call to tag_fsm', value, self.tag_data[column].tag, row_node
                    ))
                row_node = tag_fsm(value, self.tag_data[column].tag, row_node)
                if self.debug: tomb.tombstone(f"appended to {row_node}")
            
            return 1

        except KeyError as e:
            raise
            sys.exit(os.EX_DATAERR)


    @trap
    def write(self) -> int:
        self.tree = ET.ElementTree(self.root)
        self.tree.write(self.output)


@trap
def XML_main(subroutine:uu.SloppyDict) -> ERROR_ACTION:

    stats = canoestats.default()
    mytype = 'xforms_custom'
    global debugging

    for i, opcodes in enumerate(subroutine):
        debugging = opcodes.debug

        myname = uu.name_from_dirname(opcodes.local_dir)
        if not i: stats.update(myname, mytype, LED.ON)
        if not opcodes.input.startswith(os.sep):
            opcodes.input = os.path.join(opcodes.local_dir, opcodes.input)
        if not opcodes.output.startswith(os.sep):
            opcodes.output = os.path.join(opcodes.local_dir, opcodes.output)

        d = uu.SloppyDict(dict.fromkeys(['input', 'output', 'grammar']))
        d.input = opcodes.input
        d.output = opcodes.output
        d.grammar = opcodes.grammar
        d.debug = opcodes.debug

        writer = Table2XML(**d)
        uu.tombstone('writer initialized.')
        writer.read_all()
        uu.tombstone('all DataFrame rows read.')
        writer.write()
        uu.tombstone('XML written.')
    
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
    else:
        print(f"{len(opcodes)} read.")

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
        tomb.tombstone(f'Calling {vm_callable}')
        error_action = globals()[vm_callable](opcodes[vm_function])
        tomb.tombstone("ERROR_ACTION is {}".format(ERROR_ACTION(error_action).name))
        sys.exit(os.EX_OK if error_action is ERROR_ACTION.proceed else os.EX_DATAERR)

    except KeyError as e:
        print("The program {} has no opcodes for the {} operation.".format(sys.argv[-1], vm_function))

    except Exception as e:
        print(uu.type_and_text(e))

    finally:
        print(80*'=')

