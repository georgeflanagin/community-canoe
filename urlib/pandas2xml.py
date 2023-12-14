# -*- coding: utf-8 -*-
""" 
Convert pandas.DataFrame to XML using a mapping between
column names in the DataFrame and (possibly) nested tags
in the XML.
"""
import typing
from   typing import *

import os
import os.path
import sys
import xml.etree.ElementTree as ET

import pandas

import tombstone as tomb
import urpacker 
import urutils as uu

from urdecorators import show_exceptions_and_frames as trap

__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2019, University of Richmond'
__credits__ = None
__version__ = '0.9'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'testable'

__license__ = 'MIT'
import license

empty_frame = pandas.DataFrame()

class Pandas2XML:

    default_header = '<?xml version="1.0" encoding="{}"?>'
    default_frame_name = "pandas_data_frame"
    default_row_name = "pandas_row"
    default_encoding = 'utf-8'    

    __slots__ = (
        'frame', 'header', 'frame_name', 'row_name',
        'encoding', 'remap', 'output', 'tree', 
        'root', 'frame_node', 'sep', 'debug', 'nodotzero'
        )
    
    __defaults__ = (
        pandas.DataFrame(), default_header, default_frame_name, default_row_name,
        default_encoding, {}, None, None, 
        None, None, '/', False, True
        )

    def __init__(self, **kwargs) -> None:
        """
        Set up the operations.

        kwargs -- data, and formatting instructions for the XML particulars. The
            useful values are fileheading (a tag to wrap the whole file) and
            the rowlabel (instead of the default literal "row").

            frame -- your data to XML-ize OR the name of a file with the data.

            remap -- a mapping of names in the DataFrame to desired names
                of the XML elements. The default mapping maps the column 
                names onto themselves. The remap can be a list, in which
                case these are just the new names in the order in which
                the columns appear in the DataFrame. If the argument is a
                str, it is assumed to be the name of a packed file containing
                the definition of the mapping.

            sep -- a character for the values in the remap that indicates
                the tag name has "parts". For example, if the sep is the
                default value of '/', then a value of "A/B" means that 
                the XML to be created is <A ><B >v</B></A>.

            frame_name -- the wrapper tag around the whole thing.

            row_name -- the wrapper tag for each row in the DataFrame.

            encoding -- defaults to 'utf-8'

            output -- the name of a file to contain the XML results.

            debug -- boolean that, when True, prints debug statements.

            nodotzero -- supress the trailing '.0' for numbers that are
                converted to strings; i.e., represent them as integers.
        """
        values = dict(zip(Pandas2XML.__slots__, Pandas2XML.__defaults__)) 

        ###
        # Set up the defaults.
        ###
        for k in Pandas2XML.__slots__:
            setattr(self, k, values[k])

        ###
        # And override them as required.
        ###
        args = [ setattr(self, k, v) 
            for k, v in kwargs.items() 
            if k in Pandas2XML.__slots__
            ]

        ###
        # Find out if we received data or the name of a packed file 
        # containing the data. If it is a filename, unpack it.
        ###
        if isinstance(self.frame, str):
            if self.debug: tomb.tombstone("Attaching IO to {}".format(self.frame))
            p = urpacker.URpacker()
            if not p.attachIO(self.frame, s_mode='read'):
                raise Exception("Could not read from {}".format(self.frame))

            self.frame = p.read(format='pandas')
    
        ###
        # Find out what kind of map we got between the columns of data
        # and the XML file.
        ###
        if not self.remap: 
            # Do a null remap onto the current names.
            self.remap = uu.SloppyDict(dict(zip(list(self.frame.columns), list(self.frame.columns))))

        elif isinstance(self.remap, list): 
            # If it is a list of strings, assume they are in order.
            self.remap = uu.SloppyDict(dict(zip(list(self.frame.columns), self.remap)))

        elif isinstance(self.remap, dict):
            # We got a full dict; make it Sloppy.
            self.remap = uu.SloppyDict(self.remap)

        elif isinstance(self.remap, str):
            # This is a URpacker file with the map in it.
            if self.debug: tomb.tombstone("reading map from {}".format(self.remap))
            p = urpacker.URpacker()
            if not p.attachIO(self.remap):
                raise Exception("XML mapping file {} not found.".format(self.remap))

            self.remap = p.read(format='python')
            if self.debug: tomb.tombstone("\n{}\n".format(self.remap))

        else:
            # We are dead.
            raise Exception('remapping object is not usable: {}'.format(self.remap))
    
        ###
        # remove columns from the DataFrame whose mapped value is "drop",
        # and change empty columns to the value of the keys (in lower case).
        ###
        drop_columns = [ _ for _ in self.frame.columns if self.remap.get(_, 'drop') == 'drop' ]
        self.frame.drop(drop_columns, axis=1, inplace=True)
        
        blank_columns = [ _ for _ in self.remap.keys() if not self.remap[_] ]        
        for c in blank_columns:
            self.remap[c] = c.lower()

        ###
        # Assign the encoding to the header if we are using the
        # default header (that has a {} format for it).
        ###
        if self.header == Pandas2XML.default_header:
            self.header = self.header.format(self.encoding)

        ###
        # Get the XML tree happening. 
        ###
        self.root = ET.Element(self.frame_name)
        self.tree = ET.ElementTree(self.root)
        self.frame_node = self.root
        self.root.append(ET.Comment('Data generated by Canoe on {}'.format(uu.now_as_string())))


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
            return True

        except Exception as e:
            tomb.tombstone(str(e))
            sys.exit(os.EX_NOINPUT)
            return False


    @trap
    def read_all(self) -> int:
        """
        Attempt to write all the data to the attached file.

        returns -- the number of records written.
        """
        return self.read_some(self.frame.shape[0])
    

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
        try:
            row = self.frame.iloc[i]
            if self.debug: tomb.tombstone(uu.fcn_signature('SubElement', self.frame_node, self.row_name))

            ###
            # Add a row to the frame
            ###
            row_node = ET.SubElement(self.frame_node, self.row_name)

            ###
            # Note that if the tag name has no parts, splitting it will
            # transform it into a list with only one element that is 
            # otherwise unchanged from the original.
            #
            # A little narrative to explain by example how this works.
            #
            # Suppose the column is "D/E/F" and the value is 7, with the
            # separator as the default, '/'.
            ###
            for column, value in row.items():

                ###
                # The next line will create the remapped name ['D', 'E', 'F']
                ###
                try:
                    remapped_name = self.remap[column].split(self.sep)
                except KeyError as e:
                    # We apparently have something in the DataFrame that has
                    # no corresponding tag in the XML.
                    continue

                else:
                    if self.debug: tomb.tombstone('remapped_name is {}'.format(remapped_name))
                
                ###
                # We don't want to change the row_node, so we need an alias for
                # it, in this case we will call it sub_node.
                ###
                sub_node = row_node

                ###
                # We are going to put in a node that is <F>7</F>. But let's see
                # if the <D> and the <E> already exist. Note that if the name is
                # not compound, this loop does nothing.
                ###
                for e in remapped_name[:-1]:
                    if self.debug: tomb.tombstone(uu.fcn_signature('new_or_find', sub_node, e))
                    sub_node = self.new_or_find(sub_node, e)
                
                ###
                # Now sub_node is either <D><E> or it is still row_node, depending
                # on whether the name was compound or simple. So we create the <F>
                # tag and assign the value (which always gets string-ized.)
                ###
                if self.debug: 
                    tomb.tombstone('*** Adding element <{}> with value "{}"'.format(remapped_name[-1], value))
                    tomb.tombstone(uu.fcn_signature('make_node_with_attribs', sub_node, remapped_name[-1]))
                sub_node = self.make_node_with_attribs(sub_node, remapped_name[-1])
                # sub_node.text = str(value) 
                sub_node.text = uu.remove_zeros(str(value).strip(), self.nodotzero) 
                
            
            return 1

        except KeyError as e:
            raise
            sys.exit(os.EX_DATAERR)


    ###
    # These functions provide the translation.
    ###

    @trap
    def new_or_find(self, parent_node:ET.Element, name:str) -> ET.Element:
        """
        locate an existing node with the correct name (ignoring the 
        presence of any attributes), or make one of the requested name
        including the attributes.
        """

        ###
        # The name embedded may be something like "F x(y) a". In this case
        # just the F part is the tag name, and the rest is considered to be
        # attributes. See comment shred_to_attribs() on how this 
        # syntax is interpreted.
        ###
        tag_value = name.split()[0]
        if self.debug: tomb.tombstone(uu.fcn_signature("==>> " + parent_node.tag, 'find', tag_value))
        e = parent_node.find(tag_value)
        if self.debug: tomb.tombstone("FOUND NODE IS: {}".format(e))
        if e is None:

            ###
            # Note that we are passing "name" because we want to hand off the 
            # attribute syntax.
            ###
            if self.debug: tomb.tombstone(uu.fcn_signature('make_node_with_attribs', parent_node, name))
            e = self.make_node_with_attribs(parent_node, name)
            if self.debug: tomb.tombstone("Made new node: {}".format(e))
        else:
            if self.debug: tomb.tombstone("Found a matching node, and returning it: {}".format(e))

        return e


    @trap
    def make_node_with_attribs(self, parent:ET.Element, node_name:str) -> ET.Element:
        """
        Using our method/kludge from the mapping, filter the attribute name and
            apply it to a newly created node.
        """
        info = node_name.strip().split()
        if self.debug: tomb.tombstone(uu.fcn_signature('shred_to_attribs', info[1:]))
        attribs = self.shred_to_attribs(info[1:])
        if self.debug: tomb.tombstone(uu.fcn_signature('SubElement', parent, info[0], attribs))
        return ET.SubElement(parent, info[0], attrib=attribs)


    @trap
    def shred_to_attribs(self, shreds:List[str]) -> dict:
        """
        parse any attribs from the column definition, and return
        a dict suitable for passing to Element() or SubElement()
        factories.
        """
        if not shreds: return {}

        attr_dict = {}
        for attr in shreds:
    
            ###
            # We transform "x(y)" into x="y", and "x" into x=True
            ###
            attr_parts = [ _.strip(')') for _ in attr.split('(') ]
            if len(attr_parts) == 1:
                attr_dict[attr_parts[0]] = "true"
            else:
                attr_dict[attr_parts[0]] = str(attr_parts[1])

        if self.debug: tomb.tombstone("attr_dict = {}".format(attr_dict))
        return attr_dict
        

    @trap
    def write(self) -> int:
        print("writing to {}".format(self.output))        
        self.tree.write(self.output)


if __name__ == '__main__':

    # The map tests attributes and nested tags.

    mymap = {"a":"A", "b":"B tt", "c":'CandD/sub', "d":"CandD/sub x(yy)"}
    mydict = [
            {'a': 1, 'b': 2, 'c': 3, 'd': 4},
            {'a': 100, 'b': 200, 'c': 300, 'd': 400},
            {'a': 1000, 'b': 2000, 'c': 3000, 'd': 4000 },
            {'c': 30000, 'd':40 }
            ]

    myframe = pandas.DataFrame(mydict)
    opts = {'remap':'almamap.json', 
            'frame':'tempfile', 
            'output':'test.xml',
            'debug':True,
            'sep':'/',
            'frame_name':'userRecords',
            'row_name':'userRecord'}

    XMLEngine = Pandas2XML(**opts)
    XMLEngine.read_all()
    XMLEngine.write()

