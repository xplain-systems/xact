# -*- coding: utf-8 -*-
"""
Package of functions that support the operation of individual compute nodes.

"""


import importlib
import sys

import xact.log
import xact.signal
import xact.util


# =============================================================================
class Node():  # pylint: disable=R0902
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
        self.inputs        = xact.util.RestrictedWriteDict()
        self.state         = dict()
        self.outputs       = xact.util.RestrictedWriteDict()
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
        _call_reset(id_node   = self.id_node,
                    fcn_reset = self.fcn_reset,
                    runtime   = self.runtime,
                    config    = self.config,
                    inputs    = self.inputs,
                    state     = self.state,
                    outputs   = self.outputs)

    # -------------------------------------------------------------------------
    def step(self):
        """
        Step node logic.

        """
        _dequeue_inputs(
                    input_queues = self.input_queues,
                    input_memory = self.inputs)

        signal = _call_step(
                    id_node  = self.id_node,
                    fcn_step = self.fcn_step,
                    inputs   = self.inputs,
                    state    = self.state,
                    outputs  = self.outputs)

        _enqueue_outputs(
                    output_queues = self.output_queues,
                    output_memory = self.outputs)

        return signal


# -------------------------------------------------------------------------
def _load_functionality(cfg_func):
    """
    Return a tuple containing the reset and step functions.

    """
    fcn_reset = None
    fcn_step  = None

    if 'py_module' in cfg_func:
        (fcn_reset, fcn_step) = _load_from_module(spec_module = cfg_func['py_module'])

    elif 'py_dill_reset' in cfg_func and 'py_dill_step' in cfg_func:
        (fcn_reset, fcn_step) = _load_from_dill(cfg_func)

    elif 'py_src_reset' in cfg_func and 'py_src_step' in cfg_func:
        (fcn_reset, fcn_step) = _load_from_source(cfg_func)

    return (fcn_reset, fcn_step)


# -----------------------------------------------------------------------------
def _load_from_module(spec_module):
    """
    Try to import the specified module.
    Log any syntax errors.

    """
    module = None
    with xact.log.logger.catch():
        module = importlib.import_module(spec_module)

    if module is None:
        raise xact.signal.NonRecoverableError(cause = 'Module not found.')

    is_coro = 'coro' in module.__dict__
    is_step = 'reset' in module.__dict__ and 'step' in module.__dict__

    assert is_coro or is_step

    if is_coro:
        fcn_reset = functools.partial(_coro_reset, module.coro)
        fcn_step  = _coro_step
    else:  # is_step
        fcn_reset = module.reset
        fcn_step  = module.step

    return (fcn_reset, fcn_step)


# -----------------------------------------------------------------------------
def _load_from_dill(cfg_func):
    """
    Load functionality from dill pickles.

    """
    fcn_reset = xact.util.function_from_dill(cfg_func['py_dill_reset'])
    fcn_step  = xact.util.function_from_dill(cfg_func['py_dill_step'])
    return (fcn_reset, fcn_step)


# -----------------------------------------------------------------------------
def _load_from_source(cfg_func):
    """
    Load functionality from source strings.

    """
    fcn_reset = xact.util.function_from_dill(cfg_func['py_dill_reset'])
    fcn_step  = xact.util.function_from_dill(cfg_func['py_dill_step'])
    return (fcn_reset, fcn_step)


# -----------------------------------------------------------------------------
def _call_reset(id_node, fcn_reset, runtime, config, inputs, state, outputs):
    """
    Call the reset function and return any signal.

    This function acts as an adapter for the
    various different ways that a reset function
    can return/throw a control signal.

    """
    if fcn_reset is None:
        return None

    try:
        return fcn_reset(runtime, config, inputs, state, outputs)

    except xact.signal.ControlSignal as signal:
        return signal

    except Exception as error:
        xact.log.logger.exception(
                    'Reset function failed for id_node = "{id}"', id = id_node)
        return xact.signal.NonRecoverableError(cause = error)

    # Control flow should never get here.
    raise RuntimeError(
        'Program logic error when trying to call the reset function.')


# -----------------------------------------------------------------------------
def _call_step(id_node, fcn_step, inputs, state, outputs):
    """
    Call the step function and return any signal.

    This function acts as an adapter for the
    various different ways that a step function
    can return/throw a control signal.

    """
    if fcn_step is None:
        return None

    try:
        return fcn_step(inputs, state, outputs)

    except xact.signal.ControlSignal as signal:
        return signal

    except Exception as error:
        xact.log.logger.exception(
                    'Step function failed for id_node = "{id}"', id = id_node)
        return xact.signal.NonRecoverableError(cause = error)

    # Control flow should never get here.
    raise RuntimeError(
        'Program logic error when trying to call the step function.')


# -----------------------------------------------------------------------------
def _dequeue_inputs(input_queues, input_memory):
    """
    Dequeue items from the input queues and store in input memory.

    Note: Shared memory edges are not touched by
    this process, as they are transmitted
    implicitly, by making parts of an input_memory
    and output_memory structure alias each
    other.

    """
    for (path, queue) in input_queues.items():
        item = queue.blocking_read()
        _put_ref(input_memory, path, item)


# -----------------------------------------------------------------------------
def _enqueue_outputs(output_queues, output_memory):
    """
    Enqueue items from output memory into output queues.

    Note: Shared memory edges are not touched by
    this process, as they are transmitted
    implicitly, by making parts of an input_memory
    and output_memory structure alias each
    other.

    """
    for (path, queue) in output_queues.items():
        item = _get_ref(output_memory, path)
        queue.non_blocking_write(item)


# -----------------------------------------------------------------------------
def _put_ref(ref, path, item):
    """
    Make the specified node and path point to the specified memory.

    """
    for name in path[:-1]:
        ref = ref[name]

    if isinstance(ref, xact.util.RestrictedWriteDict):
        ref._xact_framework_internal_setitem(path[-1], item)
    else:
        ref[path[-1]] = item


# -----------------------------------------------------------------------------
def _get_ref(ref, path):
    """
    Get a reference to the value that path refers to.

    """
    for name in path:
        ref = ref[name]
    return ref