# -*- coding: utf-8 -*-
"""
Module for configuration override logic.

"""


import xact.cfg.util


#------------------------------------------------------------------------------
def apply(cfg, tup_overrides = None, delim_cfg_addr = '.'):
    """
    Apply all specified configuration field overrides.

    """
    if tup_overrides is None:
        return cfg

    for (address, value) in zip(tup_overrides[::2], tup_overrides[1::2]):
        try:
            cfg = xact.cfg.util.apply(data           = cfg,
                                      address        = address,
                                      value          = value,
                                      delim_cfg_addr = delim_cfg_addr)
        except KeyError:
            raise RuntimeError(
                    'Could not find "{path}" in cfg'.format(path = address))

    return cfg

