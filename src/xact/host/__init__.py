# -*- coding: utf-8 -*-
"""
Package of functions that support the operation of process hosts.

"""


import collections
import contextlib
import errno
import multiprocessing
import multiprocessing.managers
import os
import platform
import select
import signal
import sys
import time

import xact.cfg
import xact.host.util
import xact.log
import xact.proc
import xact.queue


FIFO_HOST_CONTROL = "xact_host_control"


xact.log.setup()


#------------------------------------------------------------------------------
@xact.log.logger.catch
def start(cfg):
    """
    Start all compute nodes for the local host

    """
    id_host = _setup_host(cfg)
    xact.log.logger.info('Host start')
    return _start_all_hosted_processes(cfg, id_host)


#------------------------------------------------------------------------------
@xact.log.logger.catch
def stop(cfg):
    """
    Stop all compute nodes for the local host.

    """
    id_host = _setup_host(cfg)
    xact.log.logger.info('Host stop')
    kill_prefix = cfg['system']['id_system']
    xact.host.util.kill_process_by_prefix(kill_prefix)


#------------------------------------------------------------------------------
@xact.log.logger.catch
def pause(cfg):
    """
    Pause all compute nodes for the local host.

    """
    id_host = _setup_host(cfg)
    xact.log.logger.info('Host pause')


#------------------------------------------------------------------------------
@xact.log.logger.catch
def step(cfg):
    """
    Single step all compute nodes for the local host.

    """
    id_host = _setup_host(cfg)
    xact.log.logger.info('Host step')


#------------------------------------------------------------------------------
def _setup_host(cfg):
    """
    Perform common setup actions and return id_host

    """
    cfg     = xact.cfg.denormalize(cfg)
    id_host = cfg['runtime']['id']['id_host']
    return id_host


#------------------------------------------------------------------------------
def _start_all_hosted_processes(cfg, id_host):
    """
    Start all processes on the local host.

    """
    # TODO: POSIX event handling.

    # Forking is unsafe on OSX and isn't supported
    # on windows.
    if platform.system() == 'Linux':
        multiprocessing.set_start_method('fork')
    else:
        multiprocessing.set_start_method('spawn')

    id_host_local = cfg['runtime']['id']['id_host']
    map_queues    = connect_queues(cfg, id_host_local)
    map_processes = dict()

    for (id_process, cfg_process) in cfg['process'].items():
        if cfg_process['host'] == id_host_local:
            map_processes[id_process] = _start_one_child_process(
                                                cfg        = cfg,
                                                id_process = id_process,
                                                map_queues = map_queues)
    for proc in map_processes.values():
        proc.join()


#------------------------------------------------------------------------------
def connect_queues(cfg, id_host_local):
    """
    Return a map of (id_node, path) to queues.

    Start servers and connect clients as required.

    """
    set_id_edge_ipc    = set()
    set_id_edge_server = set()
    set_id_edge_client = set()
    map_cfg_edge       = dict()

    for cfg_edge in cfg['edge']:

        id_edge = cfg_edge['id_edge']
        map_cfg_edge[id_edge] = cfg_edge

        if id_host_local not in cfg_edge['list_id_host']:
            continue
        if cfg_edge['ipc_type'] == 'inter_process':
            set_id_edge_ipc.add(id_edge)
        if cfg_edge['ipc_type'] == 'inter_host':
            if id_host_local == cfg_edge['id_host_owner']:
                set_id_edge_server.add(id_edge)
            else:
                set_id_edge_client.add(id_edge)

    map_queues = dict()
    for id_edge in set_id_edge_server:
        map_queues[id_edge] = xact.queue.RemoteQueueServer(
                                    cfg, map_cfg_edge[id_edge], id_host_local)

    for id_edge in set_id_edge_ipc:
        map_queues[id_edge] = xact.queue.LocalQueue()

    for id_edge in set_id_edge_client:
        map_queues[id_edge] = xact.queue.RemoteQueueClient(
                                    cfg, map_cfg_edge[id_edge], id_host_local)

    return map_queues


#------------------------------------------------------------------------------
def _start_one_child_process(cfg, id_process, map_queues):
    """
    Start a single specified child process.

    """
    # TODO: Support for different python interpreters / venvs.
    # TODO: Support for different runtimes (C/C++/C#).
    id_system = cfg['system']['id_system']
    id_host   = cfg['runtime']['id']['id_host']

    name_proc = _process_name(cfg, id_process)
    # xact.host.util.kill_process_by_name(name_proc)

    proc = multiprocessing.Process(
                        target = xact.proc.start,
                        args   = (cfg, id_process, id_host, map_queues),
                        name   = name_proc)
    proc.daemon = False
    try:
        proc.start()
    except BrokenPipeError as err:
        xact.log.logger.warning('Broken pipe when running: ' + id_process)
    return proc


#------------------------------------------------------------------------------
def _process_name(cfg, id_process):
    """
    Return the name of the specified process

    """
    return '{sys}.{host}.{proc}'.format(
                                    sys  = cfg['system']['id_system'],
                                    host = cfg['runtime']['id']['id_host'],
                                    proc = id_process)
