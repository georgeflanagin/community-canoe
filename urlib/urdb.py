# -*- coding: utf-8 -*-
# Module supporting operations on Oracle databases from Python
# programs.

# Added typing for Python 3.5+
import typing
from typing import *

from   base64 import encodebytes, decodebytes
import binascii
import collections
import cx_Oracle
import doctest
import re
import os
import pandas
import random
import re
import signal
import socket
import sys
import time
import typing
from   typing import *

import urutils as uu
from   urdecorators import show_exceptions_and_frames

import pdb

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, 2016, 2017 University of Richmond'
__credits__ = None
__version__ = '2.0'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Production'
__license__ = 'MIT'


__license__ = 'MIT'
import license


class URdb:
    pass

class URdb:
    """ Class representation of UR's Oracle databases. """

    @classmethod
    def get_handle(URdb_cls, user:str,
            password:str,
            dbname:str,
            location:str=None,
            port:int=None,
            debug:bool=False) -> URdb:

        db_def = uu.SloppyDict(dict.fromkeys(['user', 'password', 'SID', 'port', 'host']))
        db_def.user = user
        db_def.password = password
        db_def.host = location
        db_def.port = port
        db_def.SID = dbname

        return URdb_cls(db_def)


    @show_exceptions_and_frames
    def __init__(self, db_def:object) -> None:
        """ Attempts a database open.

        user -- the Oracle user making the connection.
        password -- the user's password.
        dbname -- a string representation of a TNS name. This is either an alias (as
            defined as an entry in the tnsnames.ora file), or it is the name of a
            service exposed in that file.
        location -- a host name; can be None if we are using an alias.
        port -- not that anyone would ever do so, but let's guard against the
            possibility that someone decides to have Oracle listen on port 80,
            and httpd listen on 1521.
        debug -- makes it chatty if True.
    
        raises -- untyped exception on catastrophic failure
        """
        db_def = uu.deepsloppy(db_def)
        self.debug = True
        self._db = None
        self.selected_columns = []
        self.user     = db_def.user
        self.password = db_def.password
        self.row_count = 0
        self._db_name = db_def.SID
        port = db_def.port
        location = db_def.host
        self._db_location = "{}:{}".format(location, port)

        """
        # This hack is courtesy of research by Sasko Stefanovski, 4 September 2015
        # Altered into what you see here by George Flanagin, 5 October 2015
        # Reviewed for broken windows policy by George Flanagin, 17 October 2017
        # Changed for Canoe-19 by George Flanagin, 20 May 2019
        """
        dbname = self._db_name
        dsn_tns = []
        dsn_tns.append(cx_Oracle.makedsn(str(location), port, str(dbname)+":pooled"))
        dsn_tns.append(cx_Oracle.makedsn(str(location), port, str(dbname)))
        dsn_tns.append(cx_Oracle.makedsn(str(location), port, service_name = str(dbname)))

        for i in range(0, len(dsn_tns)):
            try:
                self._db = cx_Oracle.connect(self.user, self.password, dsn_tns[i])
            except cx_Oracle.DatabaseError as e:
                continue
            else:
                return

        try:
            self._db = cx_Oracle.connect(self.user, self.password, dbname)
        except Exception as e:
            uu.tombstone(uu.type_and_text(e))
            raise Exception("unable to open " + dbname) from e


    def __bool__(self) -> bool:
        """ 
        returns True if connected, False otherwise. 
        """
        return self._db is not None


    def __str__(self) -> str:
        """ 
        Provide the name of the database and its location. 
        """
        return '@'.join([self.user, self._db_name])


    @show_exceptions_and_frames
    def begin_transaction(self) -> None:
        """ 
        Send Oracle a start transaction message. 
        """
        self._db.begin()


    @show_exceptions_and_frames
    def column_exists(self, table:str, column:str) -> bool:
        """ 
        Determine if a column exists in the given table.

        table -- Oracle table name.
        column -- Oracle column name.

        NOTE: this function does not concern itself with whether the current
        user has write or read access to the column. This function may
        return 

        returns: -- True if the column exists, False otherwise.
        """
        if not self: return None

        sql = ( "select count(*) from all_tab_columns where column_name = {} " +
                "and table_name = {}".format(uu.q(column), uu.q(table)))
        return self.row_one(sql, 'count(*)') > 0


    @show_exceptions_and_frames
    def commit(self) -> bool:
        """ 
        Stop what we are doing and commit the open transaction. 

        returns -- True or False based on whether the Oracle commit
            succeeded. 
        """
        return self and self._db.commit()


    @show_exceptions_and_frames
    def db_error(self, sql:str, e:cx_Oracle.Error) -> None:
        """ 
        Take note of a database panic. This function is sometimes
        invoked when a connection goes stale, and we have to 
        do a seppuku. There is also the case where we don't have
        sufficient privileges to operate on the object, or the 
        object truly doesn't exist. We do our best to provide
        useful information about the cause of failure.

        sql - a text representation of the DDL or DML that was attempted.
        e   - the error object raised by Oracle
        die - even if this is a non-fatal error, we might want to end it.

        returns -- None. 
        """

        # These are the defined members of the cx_Oracle.error object.
        error_keys = ['code', 'offset', 'message', 'context', 'isrecoverable']

        # These are Oracle error numbers. Timeouts are fatal, the rest are not
        # recoverable for the database entity in question, but 
        connection_timeout = [ 2396, 1012 ]
        permissions = [ 1031 ]
        existence_issues = [ 24010, 942, 24001, 24006 ]

        # Strangely, the error is a mono-tuple.
        error, = e.args

        uu.tombstone("Oracle has returned an error while attempting: {}".format(sql))
        uu.tombstone("Complete information follows.")
        for k in error_keys:
            uu.tombstone("{} == {}".format(k, getattr(error,k,'no value')))

        if error.code in connection_timeout:
            uu.tombstone("This is death. R.I.P. Process cannot continue after fatal error.")
            uu.tombstone("database connection has been lost -- timeout")

        if not die and (error.code in permissions or error.code in existence_issues):
            raise e from None
        else:
            os.kill(os.getpid(), signal.SIGTERM)


    @show_exceptions_and_frames
    def describe_type(self, object_name:str) -> collections.OrderedDict:
        """
        Describe a /user/ type given its name.

        object_name -- case insensitive name of the thing you want described. It
            can be written as either 'schema.objname' or 'objname'.

        returns -- a dict, where the keys are the names of the attributes, and
            values that indicate the correct //PYTHON// type.
        """
        
        # if the object comes in prefixed by a schema type, then we need to truncate
        # that name because it is not in the TYPE_NAME column.
        object_name = object_name.upper().split('.')
        try:
            object_name = object_name[1]
        except:
            object_name = object_name[0]

        d = collections.OrderedDict()
        SQL = """SELECT attr_name, attr_type_name, precision, attr_no FROM user_type_attrs
                WHERE type_name = '{}' ORDER BY attr_no ASC""".format(object_name)

        for row in self.execute_SQL(SQL): 
            d[row['ATTR_NAME'].upper()] = ( 
                    uu.oracle_type_to_python(row['ATTR_TYPE_NAME']), 
                    row['PRECISION']
                    )

        return d


    @show_exceptions_and_frames
    def execute_SQL(self, sql:str, 
            no_wait_IO:bool = True, 
            die_on_error:bool = False) -> Union[List[List], int]:
        """ 
        Execute the [already escaped/prepped] SQL on an open database connection

        sql -- an appropriately triaged SQL shred.
        no_wait_IO -- flag to indicate whether to do it now (True), or open
            a transaction.
        die_on_error -- stop the process rather than raise an error.

        returns: -- One of these things:
            [1] a list of rows for SELECT statements
            [2] number of rows affected for other statements.
            [3] None on failure the function returns None.
        """
        if not self._db: return None

        result_set = None    # Whatever it is that execute() returns.
        rows = []            # arrays containing each row, and a list of rows.
        self.row_count = -1           # Count can only be zero or greater if it is valid.
        
        this_cursor = self._db.cursor()

        try:
            first_word, the_rest = sql.split(None, 1)
            first_word = first_word.upper()
        except Exception as e:
            uu.tombstone("**** BAD SQL ****: " + sql)
            raise

        the_rest = " ".join(sql.split()[1:])

        try:
            if not no_wait_IO: self.begin_transaction()

            # execute the SQL using a virgin cursor.
            if first_word == 'EXECUTE':
                # Use regex to find out if there are stored procedure 
                # args. If so, capture the proc name and parameters and 
                # feed them to callproc. If not just call the proc
                m = re.match(
                    '(?P<p_name>.*?) *\((?P<p_params>.*)\)', 
                    the_rest, flags=re.IGNORECASE)
                if m:
                    # There are stored procedure args. Split the params 
                    # from the regex capture. If an arg is numeric convert 
                    # it to the appropriate type, if not remove certain
                    # chars from the argument.
                    args = []
                    for _ in m.groupdict()['p_params'].split(','):
                        try:
                            int_val = int(_) if int(_) == float(_) else float(_)
                            args.append(int_val)
                        except:
                            args.append(_.strip(" \t\n\r\'\""))

                    this_cursor.callproc(m.groupdict()['p_name'], args)
                else:
                    this_cursor.callproc(the_rest)
                self.row_count = 1

            elif first_word == 'SELECT':
                try:
                    result_set = pandas.read_sql_query(sql, self._db)
                    result_set.fillna('', inplace=True) 
                    self.selected_columns = list(result_set.keys())
                    # rows = result_set.to_dict('r')
                    rows = uu.deepsloppy(result_set.to_dict('records'))
                    self.row_count = len(rows)

                except pandas.io.sql.DatabaseError as e:
                    uu.tombstone(str(e))
                    return None

                except Exception as e:
                    uu.tombstone(str(e))
                    raise

            else:
                try:
                    result_set = this_cursor.execute(str(sql))
                    self.row_count = this_cursor.rowcount
                except cx_Oracle.IntegrityError as e:
                    # Operation not possible, IOW. The count will be -1 when we
                    # return it, and we can leave it up to the caller to decide
                    # what to do.
                    uu.tombstone(str(e))
                except Exception as e:
                    raise
                
        except Exception as e:
            raise

        else:
            if no_wait_IO: self.commit()

        finally: # Always ... clean up and commit if needed.
            this_cursor.close()

        # The caller will know what to expect.
        return rows if first_word == 'SELECT' else self.row_count


    @show_exceptions_and_frames
    def get_lines(self) -> List[str]:
        """ 
        Call DBMS_OUTPUT.GET_LINE repeatedly until there are no more 
        lines in the buffer

        returns -- a (possibly empty) list of strings (lines)
        """
        
        lines = []
    
        try:
            cur = self._db.cursor()
            buff = cur.var(cx_Oracle.STRING)
            status = cur.var(cx_Oracle.NUMBER)

            #Call to dbms_output returns a tuple with (buff,status). If status == 0 then 
            #buff contains a line popped from the buffer. If status == 1 then we're done
            while cur.callproc("DBMS_OUTPUT.GET_LINE",[buff,status])[1] == 0:
                lines.append(buff.getvalue())
        finally:
            cur.close()

        return lines 


    @show_exceptions_and_frames
    def object_name_for_queue(self, queue_name:str) -> str:
        """
        Get the name of the object that is used in a given queue. Oracle
            does not have a direct function that does this.

        queue_name -- the case insensitive name of the queue that is
            of interest.

        returns -- the object name or None if the queue doesn't exist.
        """

        SQL = """ SELECT USER_QUEUE_TABLES.OBJECT_TYPE from USER_QUEUE_TABLES
            inner join USER_QUEUES on
                USER_QUEUE_TABLES.QUEUE_TABLE = USER_QUEUES.QUEUE_TABLE
                where USER_QUEUES.NAME = '{}'""".format(queue_name.upper())
        object_name = self.row_one(SQL, 'OBJECT_TYPE')

        return object_name if object_name else None        


    @show_exceptions_and_frames
    def last_row_ID(self, table:str, col:str='rowid') -> str:
        """ 
        Function not yet written. 
        """
        return ""


    @show_exceptions_and_frames
    def lock(self, tables:List[str], wait_seconds:int=1) -> bool:
        """ 
        Lock the table[s] given in the list in alphabetical order.

        tables -- list of table names.
        wait_seconds -- how long you want to wait for the lock, or 
            give up.

        returns -- true 
        """
        SQL = "lock table {} in share row exclusive mode wait {}"
        tables = sort(uu.listify(tables))
        for table in tables:
            try:
                self.execute_SQL(SQL.format(table, wait_seconds))
            except:
                return False

        return True


    @show_exceptions_and_frames
    def num_rows(self, table:str) -> int:
        """ 
        Return the row count for the given table.

        table -- an Oracle table name.

        returns: -- number of rows on success (might be zero), and None on
            recoverable failures.
        """

        if not self: return None
        sql = "select count(*) from " + table
        n = self.row_one(sql, 'COUNT(*)')
        if self.debug:
            uu.tombstone('There are {} rows in {}.'.format(n, table))
        return n


    @show_exceptions_and_frames
    def object_factory(self, object_name:str) -> cx_Oracle.OBJECT:
        """
        Safely create an empty object of the given name.

        object_name -- case insensitve name of the object you want
            to build. Note that the gettype() function is a direct
            part of the connection presented by cx-Oracle.

        returns -- an initialized, but empty object. Raises an exception
            on error (and prints it)
        """
        object_type = self._db.gettype(object_name.upper())
        return object_type.newobject()
        

    @show_exceptions_and_frames
    def _q_adm(self, proc_name:str, queue_name:str) -> bool:
        """
        This function is called by the public methods to "do something"
        to a queue in the current database. It is worth noting that
        starting a queue that is already running does not cause an error,
        although it does make a note of the fact, nor does stopping a 
        queue that is already stopped.

        proc_name -- Either 'start_queue' or 'stop_queue'
        queue_name -- target of your start/stop operation (case 
            insensitive)
        """
        try:
            this_cursor = self._db.cursor()
            this_cursor.callproc('dbms_aqadm.{}'.format(
                proc_name), (queue_name.upper(),)
                )

        except Exception as e:
            uu.tombstone(str(e))
            return False

        else:
            return True            


    @show_exceptions_and_frames
    def q_contents(self, queue_name:str, **criteria) -> tuple:
        """
        queue_name -- the queue that we are interested in.

        returns -- a list of queued messages and selected bookkeeping info 
            about them.
        """
        queue_name = queue_name.upper()
        table_name = self.table_from_queue(queue_name)
        if not self.q_count(queue_name): return [], {}
        
        SQL = """SELECT msgid, priority, state, expiration, enq_time,
                sender_name, sender_address, sender_protocol, user_data
                FROM {} WHERE q_name = '{}'""".format(table_name, queue_name.upper())
        contents = self.execute_SQL(SQL)
        desc = self.describe_type(self.object_name_for_queue(queue_name))

        return contents, desc


    @show_exceptions_and_frames
    def q_count(self, queue_name:str, **criteria) -> int:
        """
        queue_name -- the queue of interest.
        criteria -- matching criteria for the message.

        returns -- number of matching messages, or None if the queue
            is unavailable. Zero means no messages match.

        NOTE: the function doesn't in any way assume that you want
            the message count for "my messages."
        """
        # TODO: implement criteria.

        table_name = self.table_from_queue(queue_name.upper())
        if table_name is None: return None
        return self.num_rows(table_name)    


    @show_exceptions_and_frames
    def q_down(self, queue_name:str) -> bool:
        """
        Stops a queue. 

        returns -- True if the operation succeeds; False otherwise.
        """
        return self._q_adm('stop_queue', queue_name)


    @show_exceptions_and_frames
    def q_insert(self, queue_name:str, **kwargs) -> str:
        """
        Builds an object from the dict argument, and inserts it into
        the named queue.

        queue_name -- name of the queue (case insensitive)

        kwargs -- a Python dict containing the attribute/value pairs
            used to populate the object to be inserted. Note that the
            caller is responsible for knowing enough about the topology
            of the object to provide the correct keys.

            The keys will be converted to upper case because Oracle stores
            their names in upper case.

        returns -- message ID or None

        raises -- raises Exceptions in cases where the queue is unavailable,
            or the kwargs and the object definition does not match. 
        """

        msg_id = None
        object_type = self.object_name_for_queue(queue_name)
        if object_type is None: 
            raise Exception("Nothing found for a queue named {}".format(queue_name))

        o = self.object_factory(object_type)
        desc = self.describe_type(object_type)
        try:
            for k, v in kwargs.items():
                if k.upper() not in desc:
                    print("ignored {}".format(k))
                    continue
                setattr(o, k.upper(), desc[k][0]((v)))
            
            opts = self._db.enqoptions()
            # New line of code follows:
            opts.visibility = cx_Oracle.ENQ_IMMEDIATE

            properties = self._db.msgproperties()
            # New line of code follows:
            properties.delay = cx_Oracle.MSG_NO_DELAY

            msg_id = uu.hexxxify(self._db.enq(queue_name.upper(), opts, properties, o))

            # Move this line to the finally statement.
            # self.commit()

        finally:
            uu.tombstone("inserted {} into {}".format(msg_id, queue_name))
            self.commit()
            return msg_id


    def q_pop(self, queue_name:str, 
            wait:bool=True, 
            quiet:bool=False, 
            raw:bool=False) -> Union[dict, Tuple[object, str]]:
        """
        A destructive read of the queue. Get the next event from the
        named queue, and remove it so that others will not find the
        event still there.

        args as in q_peek()

        returns -- an event object converted to a Python type, or None
            if there is nothing to read.
        """

        return self.q_peek(queue_name, True, wait, quiet, raw)


    @show_exceptions_and_frames
    def q_peek(self, queue_name:str, 
            remove_it:bool=False, 
            wait:bool=False,
            quiet:bool=False,
            raw:bool=False) -> Union[dict, Tuple[object, str]]:
        """
        Get the next event from the named queue. Do not attempt to remove
        it unless told to, thereby allowing others to get the same event.

        raw -- return the message itself from the queue, rather than the
            python object constructed from the message.

        returns -- an event object converted to a Python type, or None
            if there is nothing to read.
        """
        
        object_type = self.object_name_for_queue(queue_name)
        if object_type is None: 
            uu.tombstone("Nothing found for a queue named {}".format(queue_name))
            return None

        o = self.object_factory(object_type)
        desc = self.pyobj_desc(queue_name)

        opts = self._db.deqoptions()
        opts.mode = cx_Oracle.DEQ_REMOVE if remove_it else cx_Oracle.DEQ_BROWSE
        opts.navigation = cx_Oracle.DEQ_NEXT_MSG 
        opts.wait = cx_Oracle.DEQ_NO_WAIT if not wait else cx_Oracle.DEQ_WAIT_FOREVER
        opts.visibility = cx_Oracle.DEQ_IMMEDIATE

        properties = self._db.msgproperties()

        message_id = self._db.deq(queue_name.upper(), opts, properties, o)
        if not message_id: return None
        message_id = uu.hexxxify(message_id)
        self.commit()

        # If we are going raw, we are finished.
        if raw: return o, message_id
        return self.pyobj(queue_name, o, message_id)


    def pyobj_desc(self, queue_name) -> dict:
        """
        queue_name -- the queue that interests us.

        returns -- a Python dict with keys that correspond to the 
            attribute names in the Object.
        """
        desc = self.describe_type(self.object_name_for_queue(queue_name))
        return desc


    def pyobj(self, queue_name:str, o:object, msgid:str="") -> uu.SloppyDict:
        """
        Create a Python object from the msg retrieved from a queue.
        """
        py_obj = uu.SloppyDict(
            dict.fromkeys(
                self.pyobj_desc(queue_name).keys()
                )
            )

        for k in py_obj.keys():
            try:
                py_obj[k] = getattr(o, k.upper())
            except:
                py_obj[k] = None
        else:
            py_obj['message_id'] = msgid if msgid else 'unknown'
    
        return py_obj


    @show_exceptions_and_frames
    def q_up(self, queue_name:str) -> bool:
        """
        Bring up a queue that is present in this database. It is worth noting
        that starting a queue that is already running does not cause an error, nor
        does stopping a queue that is already stopped.

        queue_name -- case insensitive name of the queue.
        
        returns -- True if the queue is now "up," and False otherwise.
        """
        return self._q_adm('start_queue', queue_name)


    @show_exceptions_and_frames
    def rollback(self) -> bool:
        """ 
        Rollback a current transaction. 

        returns -- True on success, False otherwise. Note that all exceptions
            are trapped (but logged), primarily because the most common
            problem is that no transaction is open --- and a rollback of nothing
            is not a big problem.
        """
        if not self: return None
        try:
            self._db.rollback()
        except Exception as e:
            uu.tombstone(uu.type_and_text(e))
            return False

        else:
            return True


    @show_exceptions_and_frames
    def row_one(self, sql:str, col:str=None) -> Union[List,object]:
        """ 
        Returns a flattened list of the first row in a query.

        sql -- an SQL shred for execution. See execute_SQL()
        col -- an optional column name, if one only wants one column.

        NOTE: there are a number of queries that are known to only return one
        meaningful row. Rather than clutter the code with returning a list of rows
        with only one row in the list, this function strips away the outer layer.
        Additionally, if one only wants one column, that value can be returned
        completely unwrapped.

        returns: -- a list of columns, or only the one value from the named
        column.
        """
        if not self: return None
        rows = self.execute_SQL(sql)
        if not rows: return None
        if hasattr(rows, '__iter__'):
            return rows[0] if col is None else rows[0][col]
        else:
            return rows


    @show_exceptions_and_frames
    def table_exists(self, table_name:str) -> bool:
        """ 
        Find out if a table exists.

        table -- an Oracle table name.

        NOTE: see note on column_exists() for similar behavior.

        returns: True if the table is visible, False otherwise.
        """
        if not self: return None
        sql = "select count(*) from all_tables where table_name = " + uu.q(table_name)
        return self.row_one(sql, 'count(*)') > 0


    @show_exceptions_and_frames
    def table_from_queue(self, queue_name:str) -> str:
        """
        Get the name of the underlying table in which data for a queue
        resides.

        queue_name -- name of the queue (case insensitive)

        returns -- name of the table, or None if queue is unavailable.
        """

        SQL = """ select queue_table from all_queues 
            where name = '{}'""".format(queue_name.upper())
        table_name = self.row_one(SQL, 'QUEUE_TABLE')
        if self.debug: uu.tombstone('Table {} underlies queue {}.'.format(
            table_name, queue_name))
        return table_name


