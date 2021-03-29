# -*- coding: utf-8 -*-
"""
Test fixtures for xact.cfg.data package functional requirements tests.

"""


import inspect
import os

import pytest


# -----------------------------------------------------------------------------
@pytest.fixture
def simple_test_config():
    """
    Return a test configuration.

    """
    cfg = {
        'system': {
            'id_system': 'xact_smoke_test'
        },
        'host': {
            'localhost': {
                'hostname':       '127.0.0.1',
                'acct_run':       'xact',
                'acct_provision': 'xact'
            }
        },
        'process': {
            'main_process': {
                'host': 'localhost'
            }
        },
        'node': {
            'nodea': {
                'process':    'main_process',
                'state_type': 'python_dict',
                'functionality':  {
                    'requirement':   'some_requirement',
                    'py_src_reset':  inspect.getsource(nodea_reset),
                    'py_src_step':   inspect.getsource(nodea_step)
                }
            },
            'nodeb': {
                'process':    'main_process',
                'state_type': 'python_dict',
                'functionality':  {
                    'requirement':   'some_requirement',
                    'py_src_reset':  inspect.getsource(nodeb_reset),
                    'py_src_step':   inspect.getsource(nodeb_step)
                }
            }
        },
        'edge': [
            {
                'owner': 'nodea',
                'data':  'python_dict',
                'src':   'nodea.outputs.test_output',
                'dst':   'nodeb.inputs.test_input'
            }
        ],
        'requirement': {
            'some_requirement': {
                'how_to_install'
            }
        },
        'data': {
            'python_dict': 'py_dict'
        }
    }

    return cfg


# -----------------------------------------------------------------------------
def nodea_reset(runtime, cfg, inputs, state, outputs):  # pylint: disable=W0613
    """
    Reset function for node A.

    """
    state['counter'] = 0


# -----------------------------------------------------------------------------
def nodea_step(inputs, state, outputs):  # pylint: disable=W0613
    """
    Step function for node A.

    """
    state['counter'] += 1
    outputs['test_output']['counter'] = state['counter']


# -----------------------------------------------------------------------------
def nodeb_reset(runtime, cfg, inputs, state, outputs):  # pylint: disable=W0613
    """
    Reset function for node B.

    """


# -----------------------------------------------------------------------------
def nodeb_step(inputs, state, outputs):  # pylint: disable=W0613
    """
    Step function for node B.

    """
    if inputs['test_input']['counter'] > 10:
        print('RUN COMPLETED SUCCESSFULLY')
        import xact.signal  # pylint: disable=C0415
        raise xact.signal.Halt(0)


# -----------------------------------------------------------------------------
@pytest.fixture
def dual_process_halt_test_config():
    """
    Return a test configuration for two processes.

    """
    cfg = {
        'system': {
            'id_system': 'xact_dual_process_test'
        },
        'host': {
            'localhost': {
                'hostname':       '127.0.0.1',
                'acct_run':       'xact',
                'acct_provision': 'xact',
                'dirpath_venv':   None
            }
        },
        'process': {
            'first_process': {
                'host': 'localhost'
            },
            'second_process': {
                'host': 'localhost'
            }
        },
        'node': {
            'tx': {
                'process':    'first_process',
                'state_type': 'python_dict',
                'functionality':  {
                    'requirement':   'some_requirement',
                    'py_src_reset':  inspect.getsource(dual_process_tx_reset),
                    'py_src_step':   inspect.getsource(dual_process_tx_step)
                }
            },
            'rx': {
                'process':    'second_process',
                'state_type': 'python_dict',
                'functionality':  {
                    'requirement':   'some_requirement',
                    'py_src_reset':  inspect.getsource(dual_process_rx_reset),
                    'py_src_step':   inspect.getsource(dual_process_rx_step)
                },
                'config': {
                    'filepath': None
                }
            }
        },
        'edge': [
            {
                'owner': 'tx',
                'data':  'python_dict',
                'src':   'tx.outputs.test_output',
                'dst':   'rx.inputs.test_input'
            }
        ],
        'requirement': {
            'some_requirement': {
                'how_to_install'
            }
        },
        'data': {
            'python_dict': 'py_dict'
        }
    }

    return cfg

# -----------------------------------------------------------------------------
def dual_process_tx_reset(runtime, cfg, inputs, state, outputs):  # pylint: disable=W0613
    """
    Reset function for node A.

    """
    state['counter'] = 0


# -----------------------------------------------------------------------------
def dual_process_tx_step(inputs, state, outputs):  # pylint: disable=W0613
    """
    Step function for node A.

    """
    state['counter'] += 1
    outputs['test_output']['counter'] = state['counter']
    if state['counter'] > 10:
        import xact.signal  # pylint: disable=C0415
        return xact.signal.Halt(0)


# -----------------------------------------------------------------------------
def dual_process_rx_reset(runtime, cfg, inputs, state, outputs):  # pylint: disable=W0613
    """
    Reset function for node B.

    """
    state['filepath'] = cfg['filepath']


# -----------------------------------------------------------------------------
def dual_process_rx_step(inputs, state, outputs):  # pylint: disable=W0613
    """
    Step function for node B.

    """
    if inputs['test_input']['counter'] > 10:
        with open(state['filepath'], 'wt') as file:
            file.write('RUN COMPLETED SUCCESSFULLY')
        import xact.signal  # pylint: disable=C0415
        return xact.signal.Halt(0)
