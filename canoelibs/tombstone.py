# -*- coding: utf-8 -*-

import typing
from   typing import *

import datetime
import os
import sqlite3
import sys
import time

import canoedb
import fname
import urutils as uu

class Accumulator(object):
    """
    This only works in a multi-processing environment because
    we care about the monotonic increasing property of the 
    numbers, not their values or whether the set of values is
    duplicated in a forked process.

    Syntax:

        i = Accumulator()
    """
    ax = 0

    @classmethod
    def reset(cls):
        Accumulator.ax = 0


    def __init__(self):
        pass

    def __call__(self):
        Accumulator.ax += 1
        return Accumulator.ax

    def __int__(self):
        return ax


AX=Accumulator()
db=canoedb.default()
SQL="""insert into canoe_log
(serial_number, sequence_number, microtime, recipe, message)
values (?, ?, ?, ?, ?)"""

def tombstone(o:object) -> str:
    """
    Print out a message, data, whatever you pass in, along with
    a timestamp and the PID of the process making the call. 
    Along with printing it out, it returns it.
    """
    global AX
    global db
    global SQL

    i = AX()
    i_str = "{:0>4}".format(i) 
    now = uu.now_as_string()
    pid = os.getpid()
    a = f"{i_str} {now} :: {pid} :: "
    aa = f"{a} {str(o)}"
    
    sys.stderr.write(aa + "\n")

    try:
        db().execute(SQL, (
            os.environ.get('sn', 0), i, time.time(), os.environ.get('recipe', 'unknown'), aa))
        db.commit()
    except sqlite3.DatabaseError as e:
        uu.tombstone(f"Database Error {e}")
        uu.tombstone(f"Unable to record info about {o}")
        
    # Return the info for use by CanoeDB.tombstone()
    return aa
    

def tombstone_comments(opcodes:object) -> str:
    if hasattr(opcodes, '#'):
        return tombstone(getattr(opcodes, '#'))
    elif hasattr(opcodes, 'comment'):
        return tombstone(opcodes.comments)
    else:
        return None
