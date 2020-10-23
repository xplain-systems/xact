# -*- coding: utf-8 -*-
"""
Test fixtures for xact.cli package functional requirements tests.

"""


import pytest


#------------------------------------------------------------------------------
@pytest.fixture
def skeleton_config():
    """
    Return a skeleton configuration for testing.

    """
    return {
        'system':  {
            'id_system': 'xact_test_system'
        },
        'host': {
            'some_host': {
                'hostname':       '123.123.123.123',
                'acct_run':       'xact',
                'acct_provision': 'xact'
            }
        },
        'process': {
            'some_process': {
                'host': 'some_host'
             }
        },
        'node': {
            'test_node': {
                'process':        'some_process',
                'state_type':     'some_data_type',
                'req_host_cfg':   'some_requirement_on_host_cfg',
                'functionality': {
                    'py_module':    'some_pkg.importable.module'
                }
            }
        },
        'edge': [
            {
                'owner': 'test_node',
                'data':  'some_data_type',
                'src':   'test_node.outputs.port',
                'dst':   'test_node.inputs.port'
             }
        ],
        'req_host_cfg': {
            'some_requirement_on_host_cfg': {
                'role': ['some_role']
            }
        },
        'role': {
            'some_role': {
                'some_ansible_role_structure'
            }
        },
        'data': {
            'some_data_type':  [
                {
                    'some_field_name': []
                }
            ],
            'some_type_alias': 'py_dict'
        }
    }
