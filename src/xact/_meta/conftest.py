# -*- coding: utf-8 -*-
"""
Test fixtures for xact.cfg.data package functional requirements tests.

"""


import textwrap
import inspect

import pytest


#------------------------------------------------------------------------------
def nodea_reset(runtime, cfg, inputs, state, outputs):
    """
    Reset function for node A.

    """
    state['counter'] = 0


#------------------------------------------------------------------------------
def nodea_step(inputs, state, outputs):
    """
    Step function for node A.

    """
    state['counter'] += 1
    outputs['test_output']['counter'] = state['counter']


#------------------------------------------------------------------------------
def nodeb_reset(runtime, cfg, inputs, state, outputs):
    """
    Reset function for node B.

    """
    pass


#------------------------------------------------------------------------------
def nodeb_step(inputs, state, outputs):
    """
    Step function for node B.

    """
    if inputs['test_input']['counter'] > 10:
        print('RUN COMPLETED SUCCESSFULLY')
        import xact.sys.exception
        raise xact.sys.exception.RunComplete(0)


#------------------------------------------------------------------------------
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