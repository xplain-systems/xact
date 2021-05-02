# -*- coding: utf-8 -*-
"""
Python multiprocessing inter process queue.

"""


import multiprocessing


# =============================================================================
class Queue:
    """
    Python multiprocessing inter process queue.

    """

    # -------------------------------------------------------------------------
    def __init__(self, cfg, cfg_edge, id_host):
        """
        Return an instance of a Queue object.

        """
        self._queue = multiprocessing.Queue()


    # -------------------------------------------------------------------------
    def blocking_read(self):
        """
        Return the next item from the FIFO queue, waiting if necessary.

        """
        return self._queue.get(block = True)


    # -------------------------------------------------------------------------
    def non_blocking_write(self, msg):
        """
        Write to the end of the FIFO queue, raising an exception if full.

        """
        return self._queue.put(msg, block = False)
