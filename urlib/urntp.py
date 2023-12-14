# -*- coding: utf-8 -*-
"""
A way to compare time across machines.
"""

import math
import socket
import struct
import sys
import time


class urNTP:
    """
    Universal clock adapter.
    """
    __slots__ = ('ntp_host', 'port', 'sock', 'request', 'max_bytes')
    __values__ = (
        "ntp1.richmond.edu",
        123,
        socket.socket(socket.AF_INET, socket.SOCK_DGRAM),
        b'\x1b' + 47 * b'\0',
        1024
        )
    __defaults__ = dict(zip(__slots__, __values__))

    # RFC868 says this is the number of seconds from 1 January 1900
    # until the beginning of UNIX time.
    JAN_1_1970 = 2208988800     
 
    # The answer doens't change.
    FRAC_SCALE = math.pow(2, -32)

    # For the dictionary.
    labels = ('last_sync', 'originate', 'server_recv', 'server_xmit', 'local_time')


    def __init__(self, **kwargs) -> None:
        """
        Set up the object with the defaults, and 
        replace the defaults as directed in kwargs.
        """
        for k, v in urNTP.__defaults__.items():
            setattr(self, k, v)
        for k, v in kwargs.items():
            setattr(self, k, v)
            

    def _UNIX_time(self, integer_part: int, frac_part: int) -> float:
        """
        
        """
        return integer_part - urNTP.JAN_1_1970 + frac_part * urNTP.FRAC_SCALE


    def __call__(self) -> tuple:
        """
        Get the time by using the functor interface for the 
        class.
        """
        self.sock.sendto(self.request, (self.ntp_host, self.port))
        answer, address = self.sock.recvfrom(self.max_bytes)
        now = time.time()
        if not answer:
            raise Exception(f"Unable to contact {self.ntp_host}")

        timestamps = []
        t = struct.unpack('!12I', answer)
        for i in range(4, 12, 2):
            timestamps.append(self._UNIX_time(t[i], t[i + 1]))
        timestamps.append(now)

        return dict(zip(urNTP.labels, tuple((time.ctime(t), t) for t in timestamps)))


    def __str__(self) -> str:
        return f"{self()}"
