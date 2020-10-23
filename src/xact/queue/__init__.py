#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Package of classes that support the use of various queue implementations.

"""

import multiprocessing

import zmq


#==============================================================================
class LocalQueue:
    """
    Local queue.


    """

    #--------------------------------------------------------------------------
    def __init__(self):
        """
        """
        self._queue = multiprocessing.Queue()

    #--------------------------------------------------------------------------
    def blocking_read(self):
        """
        """
        return self._queue.get(block = True)

    #--------------------------------------------------------------------------
    def non_blocking_write(self, msg):
        """
        """
        return self._queue.put(msg, block = False)



#==============================================================================
class RemoteQueueServer:
    """
    Control server for an xact host.

    """

    #--------------------------------------------------------------------------
    def __init__(self, cfg, cfg_edge, id_host):
        """
        Return an instance of a RemoteQueueServer object.

        """
        address       = 'tcp://*:{port}'.format(
                                port = _port(cfg['host'][id_host], cfg_edge))
        is_src        = id_host == cfg_edge['id_host_src']
        socket_type   = zmq.PUB if is_src else zmq.SUB
        self._context = zmq.Context()
        self._socket  = self._context.socket(socket_type)
        self._socket.bind(address)

    #--------------------------------------------------------------------------
    def blocking_read(self):
        """
        Synchronize the client with the server.

        """
        return self._socket.recv_pyobj()

    #--------------------------------------------------------------------------
    def non_blocking_write(self, item):
        """
        Synchronize the client with the server.

        """
        self._socket.send_pyobj(item)


#==============================================================================
class RemoteQueueClient:
    """
    Client used to control an xact host.

    """

    #--------------------------------------------------------------------------
    def __init__(self, cfg, cfg_edge, id_host):
        """
        Return an instance of a RemoteQueueClient object.

        """
        cfg_host      = cfg['host'][id_host]
        address       = 'tcp://{hostname}:{port}'.format(
                                    hostname = cfg_host['hostname'],
                                    port     = _port_number(cfg_host, cfg_edge))
        is_src        = id_host == cfg_edge['id_host_src']
        socket_type   = zmq.PUB if is_src else zmq.SUB
        self._context = zmq.Context()
        self._socket  = self._context.socket(socket_type)
        self._socket.connect(address)

    #--------------------------------------------------------------------------
    def blocking_read(self):
        """
        Synchronize the client with the server.

        """
        return self._socket.recv_pyobj()

    #--------------------------------------------------------------------------
    def non_blocking_write(self, item):
        """
        Synchronize the client with the server.

        """
        self._socket.send_pyobj(item)


#------------------------------------------------------------------------------
def _port_number(cfg_host, cfg_edge):
    """
    Return the port number for the specified edge.

    """
    port_range    = cfg_host['port_range'].split('-')
    port_lo       = int(port_range[0])
    port_hi       = int(port_range[1])
    port          = port_lo + cfg_edge['idx_edge']
    if port > port_hi:
        raise RuntimeError('Insufficient port numbers available for graph')
    return port
