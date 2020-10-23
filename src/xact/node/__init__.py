# -*- coding: utf-8 -*-
"""
Package of functions that support the operation of individual compute nodes.

"""


import importlib
import sys

import xact.log
import xact.sys.exception
import xact.util


#==============================================================================
class Node():
    """
    Class representing a node in the data flow graph.

    """

    # -------------------------------------------------------------------------
    def __init__(self, cfg, id_node):
        """
        Return an instance of a Node object.

        """
        self.id_node       = id_node
        self.runtime       = cfg['runtime']
        self.config        = dict()
        self.inputs        = dict()
        self.state         = dict()
        self.outputs       = dict()
        self.input_queues  = dict()
        self.output_queues = dict()

        cfg_node = cfg['node'][id_node]
        try:
            self.config = cfg_node['config']
        except KeyError:
            pass

        (self.fcn_reset,
         self.fcn_step) = _load_functionality(cfg_node['functionality'])


    # -------------------------------------------------------------------------
    def reset(self):
        """
        Reset or zeroize node data structures.

        """
        if self.fcn_reset is not None:
            try:
                self.fcn_reset(self.runtime,
                               self.config,
                               self.inputs,
                               self.state,
                               self.outputs)
            except xact.sys.exception.ControlException:
                raise
            except Exception as err:
                xact.log.logger.exception(
                        'Reset function failed for id_node = "{id_node}"',
                                                        id_node = self.id_node)
                raise xact.sys.exception.NonRecoverableError()

    # -------------------------------------------------------------------------
    def step(self):
        """
        Step node logic.

        """
        for (path, queue) in self.input_queues.items():
            item = queue.blocking_read()
            _put_ref(self.inputs, path, item)

        if self.fcn_step is not None:
            try:
                self.fcn_step(self.inputs,
                              self.state,
                              self.outputs)
            except xact.sys.exception.ControlException:
                raise
            except Exception as err:
                xact.log.logger.exception(
                        'Step function failed for id_node = "{id_node}"',
                                                        id_node = self.id_node)
                raise xact.sys.exception.NonRecoverableError()

        for (path, queue) in self.output_queues.items():
            item = _get_ref(self.outputs, path)
            queue.non_blocking_write(item)


#------------------------------------------------------------------------------
def _get_ref(ref, path):
    """
    Get a reference to the value that path refers to.

    """
    for name in path:
        ref = ref[name]
    return ref


#------------------------------------------------------------------------------
def _put_ref(ref, path, item):
    """
    Make the specified node and path point to the specified memory.

    """
    for name in path[:-1]:
        ref = ref[name]
    ref[path[-1]] = item


# -------------------------------------------------------------------------
def _load_functionality(cfg_func):
    """
    Return a tuple containing the reset and step functions.

    """
    fcn_reset = None
    fcn_step  = None

    if 'py_module' in cfg_func:

        # Try to import the specified module.
        # Log any syntax errors.
        module = None
        with xact.log.logger.catch():
            module = importlib.import_module(cfg_func['py_module'])

        if module is None:
            raise xact.sys.exception.NonRecoverableError()

        if 'reset' in module.__dict__:
            fcn_reset = module.reset
        if 'step' in module.__dict__:
            fcn_step = module.step

    if 'py_src_reset' in cfg_func:
        fcn_reset = xact.util.function_from_source(cfg_func['py_src_reset'])
    if 'py_src_step' in cfg_func:
        fcn_step = xact.util.function_from_source(cfg_func['py_src_step'])

    return (fcn_reset, fcn_step)
