# -*- coding: utf-8 -*-
import typing
from   typing import *

import os
import sys

import enum
import sqlite3

import fname
import urutils as uu
from   urdecorators import show_exceptions_and_frames as trap

class LED(enum.IntEnum):
    """
    Lee Parker's display devices. We have not yet determined how to use the
    values, so this is the only place we will change them. In the calling code
    we just reference "RED" and "GREEN" without regard to what they mean. 
    """

    OFF    = 3
    WAITING = 2
    GREEN  = 1

    ON     = 0

    YELLOW = -1
    RED    = -2
    WAIT_EXPIRED = -3
    REQUIRED_FAIL = -4
    EMPTY_FAIL = -5

#                             321012345
dash_devices = dict(zip(LED, ' @..*#wre'))
dash_devices[None] = ' '

class CanoeStats: pass
class CanoeStats:

    inc_stmt = """update stats set last_update = CURRENT_TIMESTAMP,
            {0} = {0} + 1 where {0} > -1 and name = ?"""
    
    new_integration_stmt = "insert or ignore into stats (name, frequency) values (?, ?)"
    new_execution_stmt = """update stats set serial_number = ?,
        db_reads = NULL,
        db_writes = NULL,
        ops_local = NULL,
        ops_remote = NULL,
        files_in = NULL,
        files_out = NULL,
        xforms_std = NULL,
        xforms_custom = NULL,
        last_update = CURRENT_TIMESTAMP
    where name = ?"""
    update_stmt = "update stats set last_update = CURRENT_TIMESTAMP, {} = ? where name = ?"
    update_frequency_stmt = "update stats set frequency = ? where name = ?"

    fields = ('db_reads', 'db_writes', 'ops_local', 'ops_remote', 
        'files_in', 'files_out', 'xforms_std', 'xforms_custom')
    update_statements = tuple(
        "update stats set {} = ?, last_update = CURRENT_TIMESTAMP where name = ?".format(field)
        for field in fields )
    updates = uu.SloppyDict(dict(zip(fields, update_statements)))


    def __init__(self, dbname:str):
        """
        Open the database. 
        """        

        f = fname.Fname(dbname)
        if not f:
            raise Exception("Cannot connect to a database at {}".format(f))
        self.conn = sqlite3.connect(dbname)
        self.cursor = self.conn.cursor()


    def update(self, name:str, field:str, value:LED) -> bool:
        """
        Choose the update statement and execute it.

        name -- name of the integration (primary key)
        field -- attribute of the integration to update.
        value -- one of the enumerated values.
        """
        try:
            stmt = CanoeStats.updates[field]
            self.cursor.execute(stmt, (value, name))
            self.conn.commit()
            return True

        except Exception as e:
            uu.tombstone(str(e))
            return False


    def new_integration(self, name:str, frequency:str) -> bool:
        """
        Add a row for a new integration, or update it with a new timestamp
            and a lot of new values if it already exists.
        """

        try:
            self.cursor.execute(CanoeStats.new_integration_stmt, (name, frequency))
            self.cursor.execute(CanoeStats.update_frequency_stmt, (frequency, name))
            self.conn.commit()

        except sqlite3.IntegrityError as e:
            uu.tombstone('{} already exists, updating it instead.'.format(name))
            return self.new_execution(name, 0)

        except Exception as e:
            uu.tombstone(uu.type_and_text(e))
            return False
        return True


    def inc(self, name:str, field:str) -> bool:
        """
        Bump the value by one.
        """
        try:        
            self.cursor.execute(
                CanoeStats.inc_stmt.format(field), (name,)
            )
            self.conn.commit()

        except Exception as e:
            raise
            return False
        return True


    def new_execution(self, name:str, serial_number:int) -> bool:
        """
        We call this when we execute a new job.
        """

        try:
            self.cursor.execute(CanoeStats.new_execution_stmt, (serial_number, name))
            self.conn.commit()

        except Exception as e:
            uu.tombstone(uu.type_and_text(e))
            return False
        return True


    def get_jobs(self, order:str='alpha') -> list:
        """
        Do a flying select, already alphabetized. 
        """

        try:
            return ( self.cursor.execute("SELECT * from all_integrations").fetchall() 
                if order == 'alpha' else
                self.cursor.execute("SELECT * from recent_integrations").fetchall() )
        except Exception as e:
            uu.tombstone(uu.type_and_text(e))
            return []


def default() -> CanoeStats:
    """
    Open the default stats database.
    """
    return CanoeStats(
        os.path.join(os.environ.get('CANOE_HOME'), 'canoestats.db')
        )


def get_dashboard() -> str:
    """
    Pull it all back as a text string.
    """
    data = default().get_jobs()
    s = ""

    for line in data:
        for field in line:
            s += LED_BULBS.get(field, str(field)) + ' '
        s += '\n'
    s += '\n'
    return s
    

class Blinker:
    def __init__(self, myname:str, mytype:str):
        self.db = default()
        self.myname = myname
        self.mytype = mytype
        self.blink(LED.ON)

    def blink(self, color:LED) -> None:
        self.db.update(self.myname, self.mytype, color)



if __name__ == '__main__':
    db = CanoeStats(os.path.join(os.environ.get('CANOE_HOME', '.'), 'canoestats.db'))
    print("new_integration returned {}".format(db.new_integration('xxtestxx', 'hourly')))
    print("new_execution returned {}".format(db.new_execution('xxtestxx', 5103)))
    print("update returned {}".format(db.update('xxtestxx', 'files_in', 2)))
    print(db.get_jobs())


