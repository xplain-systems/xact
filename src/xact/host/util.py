# -*- coding: utf-8 -*-
"""
Module of utilities to support the operation of process hosts.

"""


import os
import signal

import loguru

try:
    import psutil
except ModuleNotFoundError:
    psutil = None


#------------------------------------------------------------------------------
def kill_process_by_prefix(prefix):
    """
    Kill process with the specified name (or iterable of names).

    """
    set_pid = _pid_from_prefix(prefix)
    loguru.logger.info('Send SIGTERM to {n} pids.'.format(n = len(set_pid)))
    kill_process_tree(iter_pid    = set_pid,
                      kill_signal = signal.SIGTERM)

    set_pid = _pid_from_prefix(prefix)
    if 0 == len(set_pid):
        return
    loguru.logger.info('Send SIGKILL to {n} pids.'.format(n = len(set_pid)))
    kill_process_tree(iter_pid    = set_pid,
                      kill_signal = signal.SIGKILL)


#------------------------------------------------------------------------------
def _pid_from_prefix(prefix):
    """
    Return a list of process ids that correspond to the specified names

    """
    if psutil is None:
        return {}
    set_pids  = set()
    for proc in psutil.process_iter(['name', 'pid']):
        if proc.info['name'].startswith(prefix):
            set_pids.add(proc.info['pid'])
    return set_pids


#------------------------------------------------------------------------------
def kill_process_tree(iter_pid, kill_signal):
    """
    Kill a process tree (including grandchildren).

    """
    if psutil is None:
        return
    for pid in iter_pid:

        assert pid != os.getpid(), 'A process should not attempt to kill itself'
        try:
            parent = psutil.Process(pid)
        except psutil.NoSuchProcess:
            continue

        children = parent.children(recursive=True)
        children.append(parent)
        for proc in children:
            proc.send_signal(kill_signal)

        psutil.wait_procs(children)