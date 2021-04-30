# -*- coding: utf-8 -*-
"""
Package of functions that support the operation of the system as a whole.

This package contains functions to start, stop,
pause/unpause and single-step an xact system.

"""


import copy
import os
import subprocess
import sys  # pylint: disable=W0406
import time
import uuid

import zmq

import xact.host
# import xact.sys.orchestration
import xact.util.serialization


# -----------------------------------------------------------------------------
def start(cfg):
    """
    Start the system.

    """
    cfg['runtime']['id']['id_run'] = uuid.uuid4().hex[0:8]
    cfg['runtime']['id']['ts_run'] = time.strftime('%Y%m%d%H%M%S',
                                                   time.gmtime())

    if cfg['runtime']['opt']['is_local']:
        return _run_locally(cfg)

    if cfg['runtime']['opt']['do_make_ready']:
        pass
        # xact.sys.orchestration.ensure_ready_to_run(cfg)

    for id_host in _list_id_host(cfg):
        _command(cfg, id_host, 'start-host')

    return 0


# -----------------------------------------------------------------------------
def stop(cfg):
    """
    Stop the system.

    """
    if cfg['runtime']['opt']['is_local']:
        return xact.host.stop(cfg)

    for id_host in _list_id_host(cfg):
        _command(cfg, id_host, 'stop-host')


# -----------------------------------------------------------------------------
def pause(cfg):
    """
    Pause the system.

    """
    if cfg['runtime']['opt']['is_local']:
        raise RuntimeError('Not implemented.')

    for id_host in _list_id_host(cfg):
        _command(cfg, id_host, 'pause-host')


# -----------------------------------------------------------------------------
def step(cfg):
    """
    Single step the system.

    """
    if cfg['runtime']['opt']['is_local']:
        raise RuntimeError('Not implemented.')

    for id_host in _list_id_host(cfg):
        _command(cfg, id_host, 'step-host')


# -----------------------------------------------------------------------------
def _list_id_host(cfg):
    """
    Return a sorted list of unique id_host.

    """
    iter_cfg_proc = cfg['process'].values()
    set_id_host   = set(cfg_proc['host'] for cfg_proc in iter_cfg_proc)
    list_id_host  = sorted(set_id_host)
    return list_id_host


# -----------------------------------------------------------------------------
def _command(cfg, id_host, command):
    """
    Give the specified host a command.

    """
    cfg_copy        = copy.deepcopy(cfg)
    cfg_copy['runtime']['id']['id_host'] = id_host

    cfg_encoded     = xact.util.serialization.serialize(cfg_copy)
    cfg_host        = cfg_copy['host'][id_host]
    dirpath_venv    = cfg_host['dirpath_venv']
    filepath_venv   = os.path.join(dirpath_venv, 'bin/activate')
    command_venv    = 'source {venv}'.format(venv = filepath_venv)
    command_name    = 'xact'
    command_group   = 'host'

    if id_host == 'localhost':
        subprocess.Popen(
                args   = [command_name, command_group, command, cfg_encoded],
                stdin  = subprocess.DEVNULL,
                stdout = subprocess.DEVNULL,
                stderr = subprocess.DEVNULL)

    else:
        remote_command = '{venv} && {cli} {grp} {cmd} {cfg}'.format(
                                                venv = command_venv,
                                                cli  = command_name,
                                                grp  = command_group,
                                                cmd  = command,
                                                cfg  = cfg_encoded)
        local_command = 'ssh {act}@{host} "{cmd}"'.format(
                                                act  = cfg_host['acct_run'],
                                                host = cfg_host['hostname'],
                                                cmd  = remote_command)
        subprocess.run(local_command, shell = True, check = True)


# -----------------------------------------------------------------------------
def _run_locally(cfg):
    """
    Start all compute nodes in the current process only.

    This function modifies the specified config
    dict to force all data flow graph nodes
    to execute in a single sequential process.

    This is intended to assist with debugging
    and diagnosing errors.

    """
    import xact.proc  # pylint: disable=C0415,W0621

    id_host_local    = 'localhost'
    id_process_local = 'mainprocess'

    for cfg_proc in cfg['process'].values():
        cfg_proc['host'] = id_host_local

    cfg['process'][id_process_local] = {'host': id_host_local}

    for cfg_node in cfg['node'].values():
        cfg_node['process'] = id_process_local

    cfg = xact.cfg.denormalize(cfg)

    for cfg_edge in cfg['edge']:
        cfg_edge['ipc_type']        = 'intra_process'
        cfg_edge['list_id_process'] = [id_process_local]

    cfg['runtime']['id']['id_host']    = id_host_local
    cfg['runtime']['id']['id_process'] = id_process_local

    map_queues = xact.host.connect_queues(cfg, id_host_local)
    return xact.proc.start(cfg, id_process_local, id_host_local, map_queues)
