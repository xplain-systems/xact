# -*- coding: utf-8 -*-
"""
Module of functions for validating configuration data.

"""


import copy
import jsonschema

import xact.cfg.exception
import xact.log


#------------------------------------------------------------------------------
def normalized(cfg):
    """
    Validate normalized config data.

    This is the config data as it appears on disk, prior to
    any denormalization and/or expansion.

    """
    return _validate_with_schema(cfg, _normalized_cfg_schema())


#------------------------------------------------------------------------------
def denormalized(cfg):
    """
    Validate denormalized config data.

    This is the config data after it has been processed
    to make implicit information explicit.

    """
    return _validate_with_schema(cfg, _denormalized_cfg_schema())


#------------------------------------------------------------------------------
def _validate_with_schema(cfg, schema):
    """
    Validate config using the specified schema.

    """
    try:
        jsonschema.validate(cfg, schema)
    except jsonschema.exceptions.ValidationError as err:
        msg = '\n\n{msg}\n\n'.format(msg = str(err))
        raise xact.cfg.exception.CfgError(msg)
    _check_consistency(cfg)
    return cfg


#------------------------------------------------------------------------------
def _normalized_cfg_schema():
    """
    Return a schema for normalized config data.

    """
    schema = {
        '$schema': 'http://json-schema.org/draft-07/schema#',
        '$id': 'http://xplain.systems/schemas/cfg_norm_v1.json',
        'definitions': {
            'numeric_string':      { 'type':    'string',
                                     'pattern': '^[0-9]*$' },
            'hex_string':          { 'type':    'string',
                                     'pattern': '^[a-f0-9]*$' },
            'lowercase_name':      { 'type':    'string',
                                     'pattern': '^[a-z0-9_]*$' },
            'lowercase_dot_path':  { 'type':    'string',
                                     'pattern': '^[a-z0-9_.]*$' },
            'entry_point_obj_ref': { 'type':    'string',
                                     'pattern': '^[A-Za-z0-9_.:]*$' },
            'id_edge':             { 'type':    'string',
                                     'pattern': '^[a-z0-9_./-]*$' },
            'id_system':           { '$ref': '#/definitions/lowercase_name' },
            'id_host':             { '$ref': '#/definitions/lowercase_name' },
            'id_run':              { '$ref': '#/definitions/hex_string'     },
            'ts_run':              { '$ref': '#/definitions/numeric_string' },
            'id_cfg':              { '$ref': '#/definitions/hex_string'     },
            'id_state':            { '$ref': '#/definitions/lowercase_name' },
            'id_process':          { '$ref': '#/definitions/lowercase_name' },
            'id_node':             { '$ref': '#/definitions/lowercase_name' },
            'id_req_host_cfg' :    { '$ref': '#/definitions/lowercase_name' },
            'id_data_type':        { '$ref': '#/definitions/lowercase_name' },
            'ipc_type':            { '$ref': '#/definitions/lowercase_name' },
            'path_part':           { '$ref': '#/definitions/lowercase_dot_path' },
            'py_src':              { 'type': 'string' },
            'spec_functionality': {
                'type': 'object',
                'properties': {
                    'py_module':     {
                        '$ref': '#/definitions/entry_point_obj_ref'
                    },
                    'py_src_reset': {
                        '$ref': '#/definitions/py_src'
                    },
                    'py_src_step':  {
                        '$ref': '#/definitions/py_src'
                    }
                },
                'required': [],
            }
        },
        'type': 'object',
        'properties': {
            'system': {
                'type': 'object',
                'properties': {
                    'id_system': { '$ref': '#/definitions/id_system' } },
                'required': [ 'id_system' ],
                'additionalProperties': False
            },
            'host': {
                'type': 'object',
                'propertyNames': { 'type': 'string' },
                'additionalProperties': {
                    'type': 'object',
                    'properties': {
                        'hostname':        { 'type': 'string' },
                        'acct_run':        { 'type': 'string' },
                        'acct_provision':  { 'type': 'string' },
                        'port_range':      { 'type': 'string' },
                        'password':        { 'type': 'string' },
                        'key_filename':    { 'type': 'string' },
                        'dirpath_install': { 'type': 'string' },
                        'dirpath_venv':    { 'type': 'string' },
                        'dirpath_log':     { 'type': 'string' },
                        'log_level':       { 'type': 'string' }
                    },
                    'required': [],
                    'additionalProperties': False
                }
            },
            'process': {
                'type': 'object',
                'propertyNames': { 'type': 'string' },
                'additionalProperties': {
                    'type': 'object',
                    'properties': {
                        'host': { '$ref': '#/definitions/id_host' }
                    },
                    'required': [ 'host' ],
                    'additionalProperties': False
                }
            },
            'node': {
                'type': 'object',
                'propertyNames': { 'type': 'string' },
                'additionalProperties': {
                    'type': 'object',
                    'properties': {
                        'process':       { '$ref': '#/definitions/id_process'         },
                        'req_host_cfg':  { '$ref': '#/definitions/id_req_host_cfg'    },
                        'functionality': { '$ref': '#/definitions/spec_functionality' },
                        'state_type':    { '$ref': '#/definitions/id_data_type'       },
                        'config':        { 'type': 'object'                           }
                    },
                    'required': [ 'process', 'functionality' ],
                    'additionalProperties': False
                }
            },
            'edge': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'owner': { '$ref': '#/definitions/id_node'      },
                        'data':  { '$ref': '#/definitions/id_data_type' },
                        'src':   { '$ref': '#/definitions/path_part'    },
                        'dst':   { '$ref': '#/definitions/path_part'    }
                    },
                    'required': [
                        'owner',
                        'data',
                        'src',
                        'dst'
                    ],
                    'additionalProperties':  False
                }
            },
            'req_host_cfg': {
                'type': 'object'
            },
            'roles': {
                'type': 'object'
            },
            'data': {
                'type': 'object'
            },
            'runtime': {
                'type': 'object',
                'properties': {
                    'opt':  {
                        'type': 'object',
                        'properties': {
                            'do_make_ready':  { 'type': 'boolean' },
                            'is_distributed': { 'type': 'boolean' }
                        },
                        'required': [
                            'do_make_ready',
                            'is_distributed' ],
                        'additionalProperties':  False
                    },
                    'id':  {
                        'type': 'object',
                        'properties': {
                            'id_system':  { '$ref': '#/definitions/id_system'  },
                            'id_cfg':     { '$ref': '#/definitions/id_cfg'     },
                            'id_host':    { '$ref': '#/definitions/id_host'    },
                            'id_process': { '$ref': '#/definitions/id_process' },
                            'id_node':    { '$ref': '#/definitions/id_node'    },
                            'ts_run':     { '$ref': '#/definitions/ts_run'     },
                            'id_run':     { '$ref': '#/definitions/id_run'     }
                        },
                        'required': [
                            'id_host',
                            'id_run',
                            'ts_run',
                            'id_cfg' ],
                        'additionalProperties':  False
                    },
                    'state': { '$ref': '#/definitions/id_state'  }
                },
                'required': [
                    'opt',
                    'id',
                    'state' ],
                'additionalProperties':  False
            }
        },
        'required': ['system',
                     'host',
                     'process',
                     'node',
                     'edge',
                     'data']
    }
    return schema


#------------------------------------------------------------------------------
def _denormalized_cfg_schema():
    """
    Return a schema for denormalized config data.

    """
    schema = copy.deepcopy(_normalized_cfg_schema())

    schema['$id'] = 'http://xplain.systems/schemas/cfg_denorm_v1.json'

    host_schema = schema['properties']['host']['additionalProperties']
    host_schema['properties']['is_inter_host_edge_owner'] = {'type': 'boolean'}
    host_schema['required'].append('is_inter_host_edge_owner')

    node_schema = schema['properties']['node']['additionalProperties']
    node_schema['properties']['host'] = {'type': 'string'}
    node_schema['required'].append('host')

    edge_schema = schema['properties']['edge']['items']
    edge_props  = edge_schema['properties']
    edge_props['id_edge']         = { '$ref': '#/definitions/id_edge'  }
    edge_props['relpath_src']     = { 'type': 'array'                  }
    edge_props['relpath_dst']     = { 'type': 'array'                  }
    edge_props['id_node_src']     = { '$ref': '#/definitions/id_node'  }
    edge_props['id_node_dst']     = { '$ref': '#/definitions/id_node'  }
    edge_props['list_id_process'] = { 'type': 'array'                  }
    edge_props['list_id_host']    = { 'type': 'array'                  }
    edge_props['ipc_type']        = { '$ref': '#/definitions/ipc_type' }
    edge_props['id_host_owner']   = { '$ref': '#/definitions/id_host'  }
    edge_props['id_host_src']     = { '$ref': '#/definitions/id_host'  }
    edge_props['id_host_dst']     = { '$ref': '#/definitions/id_host'  }
    edge_props['idx_edge']        = { 'oneOf': [ { 'type': 'number' },
                                                 { 'type': 'null'   } ]}
    edge_schema['required'] = list(edge_schema['properties'].keys())

    return schema


#------------------------------------------------------------------------------
def _check_consistency(cfg):
    """
    Raise an exception if cfg is inconsistent.

    """
    set_id_host          = set(cfg['host'].keys())
    set_id_process       = set(cfg['process'].keys())
    set_id_node          = set(cfg['node'].keys())
    set_id_data          = set(cfg['data'].keys())

    if 'req_host_cfg' in cfg:
        set_id_req_host_cfg = set(cfg['req_host_cfg'].keys())

    if 'role' in cfg:
        set_id_role = set(cfg['role'].keys())

    for cfg_process in cfg['process'].values():
        id_host = cfg_process['host']
        if id_host not in set_id_host:
            msg = 'Unkown id_host in cfg: {sz}'.format(id = id_host)
            raise xact.cfg.exception.CfgError(msg)

    for cfg_node in cfg['node'].values():
        id_process = cfg_node['process']
        if id_process not in set_id_process:
            msg = 'Unkown id_process in cfg: {id}'.format(id = id_process)
            raise xact.cfg.exception.CfgError(msg)

        if 'state_type' in cfg_node:
            id_data = cfg_node['state_type']
            if id_data not in set_id_data:
                msg = 'Unkown id_data in cfg: {id}'.format(id = id_data)
                raise xact.cfg.exception.CfgError(msg)

        if 'req_host_cfg' in cfg_node:
            id_req_host_cfg = cfg_node['req_host_cfg']
            if id_req_host_cfg not in set_id_req_host_cfg:
                msg = 'Unkown id_req_host_cfg in cfg: {id}'.format(
                                                        id = id_req_host_cfg)
                raise xact.cfg.exception.CfgError(msg)

    set_edge_path = set()
    for cfg_edge in cfg['edge']:
        id_node_owner = cfg_edge['owner']
        if id_node_owner not in set_id_node:
            msg = 'Unkown id_node in cfg: {id}'.format(id = id_node_owner)
            raise xact.cfg.exception.CfgError(msg)

        id_data = cfg_edge['data']
        if id_data not in set_id_data:
            msg = 'Unknown id_data in cfg: {id}'.format(id = id_data)
            raise xact.cfg.exception.CfgError(msg)

        id_node_src = cfg_edge['src'].split('.')[0]
        if id_node_src not in set_id_node:
            msg = 'Unknown id_node in cfg: {id}'.format(id = id_node_src)
            raise xact.cfg.exception.CfgError(msg)

        if cfg_edge['src'].split('.')[1] != 'outputs':
            msg = 'Edge source needs to be an output.'
            raise xact.cfg.exception.CfgError(msg)

        id_node_dst = cfg_edge['dst'].split('.')[0]
        if id_node_dst not in set_id_node:
            msg = 'Unknown id_node in cfg: {id}'.format(id = id_node_dst)
            raise xact.cfg.exception.CfgError(msg)

        if cfg_edge['dst'].split('.')[1] != 'inputs':
            msg = 'Edge destination needs to be an input.'
            raise xact.cfg.exception.CfgError(msg)

        if cfg_edge['src'] in set_edge_path:
            msg = 'Repeated edge source: {src}'.format(src = cfg_edge['src'])
            raise xact.cfg.exception.CfgError(msg)
        set_edge_path.add(cfg_edge['src'])

        if cfg_edge['dst'] in set_edge_path:
            msg = 'Repeated edge destination: {dst}'.format(
                                                        dst = cfg_edge['dst'])
            raise xact.cfg.exception.CfgError(msg)
        set_edge_path.add(cfg_edge['dst'])

    if 'req_host_cfg' in cfg:
        for cfg_req_host_cfg in cfg['req_host_cfg'].values():
            if 'role' not in cfg_req_host_cfg:
                continue
            for id_role in cfg_req_host_cfg['role']:
                if id_role not in set_id_role:
                    msg = 'Unknown id_role in cfg: {id}'.format(id = id_role)
                    raise xact.cfg.exception.CfgError(msg)