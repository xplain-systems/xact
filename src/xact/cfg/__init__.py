# -*- coding: utf-8 -*-
"""
Package of functions that manipulate configuration data.

"""

import collections
import enum
import glob
import json
import os

import loguru

import xact.cfg.data
import xact.cfg.edge
import xact.cfg.exception
import xact.cfg.load
import xact.cfg.override
import xact.cfg.validate
import xact.util
import xact.util.serialization

from xact.cfg.exception import CfgError


#------------------------------------------------------------------------------
@loguru.logger.catch(exclude = CfgError)
def prepare(path_cfg       = None,
            string_cfg     = None,
            do_make_ready  = False,
            is_distributed = True,
            delim_cfg_addr = '.',
            tup_overrides  = None):
    """
    Load configuration and override fields as required.

    """
    has_cfg_files  = path_cfg is not None
    has_cfg_string = string_cfg is not None
    has_cfg        = has_cfg_files or has_cfg_string
    if not has_cfg:
        raise CfgError('No configuration data has been provided.')

    cfg = dict()

    if has_cfg_files:
        cfg = merge_dicts(cfg, xact.cfg.load.from_path(path_cfg))

    if has_cfg_string:
        cfg = merge_dicts(cfg, xact.util.serialization.deserialize(string_cfg))

    cfg = xact.cfg.override.apply(cfg            = cfg,
                                  tup_overrides  = tup_overrides,
                                  delim_cfg_addr = delim_cfg_addr)

    id_cfg = xact.util.serialization.hexdigest(cfg)[0:8]

    cfg['runtime']          = dict()
    cfg['runtime']['opt']   = dict()
    cfg['runtime']['id']    = dict()
    cfg['runtime']['state'] = 'start'

    cfg['runtime']['opt']['do_make_ready']  = do_make_ready
    cfg['runtime']['opt']['is_distributed'] = is_distributed

    cfg['runtime']['id']['id_system']  = cfg['system']['id_system']
    cfg['runtime']['id']['id_cfg']     = id_cfg
    cfg['runtime']['id']['id_host']    = 'tbd'
    cfg['runtime']['id']['id_process'] = 'tbd'
    cfg['runtime']['id']['id_node']    = 'tbd'
    cfg['runtime']['id']['ts_run']     = '00000000000000'
    cfg['runtime']['id']['id_run']     = '00000000'


    # cfg = xact.util.format_all_strings(cfg)
    cfg = xact.cfg.validate.normalized(cfg)

    if cfg is None:
        raise xact.cfg.exception.CfgError('No config.')

    return cfg


#------------------------------------------------------------------------------
def denormalize(cfg):
    """
    Add redundant information to cfg to make it more convenient to use.

    The input configuration is designed
    to be DRY and succinct, at the cost
    of making some information implicit
    or otherwise requiring computation
    to infer. This function enriches
    the cfg data structure and makes
    such information explicit.

    """
    return xact.cfg.edge.denormalize(
                xact.cfg.data.denormalize(cfg))



# -----------------------------------------------------------------------------
def merge_dicts(first, second):
    """
    Merge two dictionaries. second takes priority.

    """
    return dict(_merge_dicts(first, second))


# -----------------------------------------------------------------------------
def _merge_dicts(first, second):
    """
    Merge two dictionaries (recursive function). second takes priority.

    """
    for key in set(first.keys()).union(second.keys()):
        _in_first  = key in first
        _in_second = key in second
        if _in_first and _in_second:
            _isdict_first  = isinstance(first[key], dict)
            _isdict_second = isinstance(second[key], dict)
            if _isdict_first and _isdict_second:
                yield (key, dict(_merge_dicts(first[key], second[key])))
            else:
                # second overwrites first if both are present.
                yield(key, second[key])
        elif _in_first:
            yield (key, first[key])
        else:
            yield (key, second[key])
