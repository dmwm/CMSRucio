#! /bin/env python
"""
Adding to multiprocessing the possibility to have
non demonized pools, allowing a child process to
spawn grandchildren.
"""

import multiprocessing
import multiprocessing.pool

class NoDaemonProcess(multiprocessing.Process):
    """
    non demonized process to enable child to spawn grandchildren
    """
    #pylint: disable=R0201
    def _get_daemon(self):
        return False

    #pylint: disable=R0201
    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)

#pylint: disable=W0223
class NDPool(multiprocessing.pool.Pool):
    """
    Customized Pool with non demonized process
    """
    Process = NoDaemonProcess

multiprocessing.NDPool = NDPool
