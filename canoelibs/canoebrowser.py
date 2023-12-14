# -*- coding: utf-8 -*-
""" Examine the database for results, and recipes for future schedule. """

# System imports

import datetime
import typing
from   typing import *

# Canoe imports
import canoedb
import canoeobject
from   recipe import *
import tombstone as tomb
import urutils as uu


# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Prototype'


__license__ = 'MIT'
import license

# non-class functions
class ScheduleBrowser:
    pass

# class
class ScheduleBrowser:
    """  
    This class allows examines the schedule, creates a list where
    one "row" in the list represents one job. The window is represented
    by a pair of integer minute offsets from now.

    Here are typical uses:

    Build a schedule with known offsets from "now" in *minutes*. The
    numbers can be positive (in the future) or negative (in the past)
    Here we see an example for plus/minus one hour.
    browser = ScheduleBrowser(-60, 60)

    The order doesn't matter, so you can be careless. :-) This broswer
    looks at a half hour ago to fifteen minutes from now.
    browser = ScheduleBrowser(15, -30)

    You can just give one parameter, and the other is assumed to be
    now.
    print (str(browser.get_schedule()))

    get_schedule() ... well ... gets the schedule from the database and
        from recipes in memory.
    __str__ is defined to produce a printable/displayable version of the
        schedule.

    """


    def __init__(self, x: int=0, y: int=0):
        """ To allow for parameters being in either order, we will just
        call them x and y, and assign them to the appropriate class members
        within this ctor. """

        self._lb = min(x, y)*60 + uu.now_as_seconds()
        self._ub = max(x, y)*60 + uu.now_as_seconds()
        self._results = []
        self.x = min(x, y)
        self.y = max(x, y) 

        self._OK = self._ub - self._lb > 0


    def __bool__(self) -> bool:
        """ method called by "if" test. """
        return self._OK


    def __str__(self) -> str:
        """ Return a printable version of the result set """

        s = []
        if len(self._results) > 0:
            for _ in self._results:
                s.append(str(_['MICROTIME']) + " => " +
                    str(_['RECIPE']).ljust(20)[:20] + " " +
                    str(_['TARGET']).ljust(20)[:20] + " " +
                    str(_['SERIAL_NUMBER']).rjust(10)) 
        return "\n".join(sorted(s))


    def show(self):
        if not self:
            print("Improperly initialized.")
        else:
            print(str(" ".join("From",str(self._lb),"to",self._ub)))


    def __eq__(self, other) -> bool:
        """
        Two ScheduleBrowser objects are equal if they have the
        same-ish lower and upper bounds. 
        """

        if isinstance(other, ScheduleBrowser):
            return self._results == other._results
        elif isinstance(other, str):
            return str(self) == other
        else:
            return NotImplemented


    def get_schedule(self, conf_data:object) -> bool:
        """ 
        Look in the database for the past, and in the CanoeConfig for
        the future .... 
        """

        # to avoid sorting, we get anything out of the database that might
        # qualify because anything in the database is in the past.

        db = canoedb.default()
        SQL = "SELECT * FROM schedule_info " + \
                " WHERE microtime > " + str(self._lb) + \
                " AND microtime < " + str(self._ub) 
        rows = db.execute_SQL(SQL)

        # Do a little transformation on the time. 
        self._results = []
        for _ in rows:
            _["MICROTIME"] = uu.iso_time(float(_["MICROTIME"]))
            self._results.append(_)

        # OK, that's good enough. Now let's look at the current catalog of
        # recipes that are up for execution.

        upcoming = {}
        t0 = uu.crontuple_now()

        # The idea here is to increment the minutes by 1 for each recipe until
        # the count is beyond our window.
        for name in sorted(conf_data.get_contents()):
            # Recipes /should/ have schedules, but it is possible for something
            # to be a recipe with a None schedule, meaning that it can only 
            # be executed ad-hoc.
            try:
                recipe = conf_data.get_recipe_by_name(name)
                if recipe is None: 
                    continue
                _ = recipe.schedule
            except:
                continue

            # We should have a recipe with a schedule if we are this far.
            for i in range(1, int(self.y)):
                t = t0 + datetime.timedelta(minutes=i)
                if uu.time_match(t, recipe.schedule):

                    upcoming['SERIAL_NUMBER'] = ''
                    upcoming['MICROTIME'] = t
                    upcoming['RECIPE'] = recipe.name
                    upcoming['TARGET'] = ''
                    upcoming['ERROR_NUMBER'] = ''
                    self._results.append(upcoming)
                    upcoming={}

        return self._results is not None


    def events(self) -> list:
        """ 
        returns the results of the last get_schedule() as a list of lists. 
        """

        return self._results


    def set_window(self, x:int=0, y:int=0) -> ScheduleBrowser:
        """ [Re]set the selection window. """

        self._lb = min(x, y)*60 + now_as_seconds()
        self._ub = max(x, y)*60 + now_as_seconds()
        self._OK = self._ub - self._lb > 0

        return self


if __name__ == '__main__':
    pass
else:
    # print(str(os.path.abspath(__file__)) + " compiled.")
    print("*", end="")
