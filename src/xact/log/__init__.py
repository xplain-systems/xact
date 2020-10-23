# -*- coding: utf-8 -*-
"""
Module of utilities to support various logging operations.

"""


import loguru
import os
import sys


logger = loguru.logger


#------------------------------------------------------------------------------
def setup(cfg = None, id_host = 'localhost', id_process = 'main_process'):
    """
    Setup logging.

    """
    log_level   = 'WARNING'
    dirpath_log = None

    if cfg is not None and id_host in cfg['host']:
        cfg_host = cfg['host'][id_host]
        if 'log_level' in cfg_host:
            log_level   = cfg_host['log_level']
        if 'dirpath_log' in cfg_host:
            dirpath_log = cfg_host['dirpath_log']

    global logger
    logger.remove()
    logger.add(sys.stderr,
               level     = log_level,
               backtrace = False)

    if dirpath_log is not None:
        id_system    = cfg['system']['id_system']
        filename_log = '{sys}_{proc}.log'.format(sys  = id_system,
                                                 proc = id_process)
        filepath_log = os.path.join(dirpath_log, filename_log)
        logger.add(filepath_log,
                   rotation  = '100 MB',
                   level     = log_level,
                   backtrace = False)
