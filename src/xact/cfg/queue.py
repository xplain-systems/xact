
# -*- coding: utf-8 -*-
"""
Module of functions that support the configuration of queues.

"""

# -----------------------------------------------------------------------------
def denormalize(cfg):
    """
    Add default queue config.

    """
    if 'queue' not in cfg:
        cfg['queue'] = dict()
        cfg['queue']['inter_process']     = 'xact.queue.multiprocessing'
        cfg['queue']['inter_host_server'] = 'xact.queue.zmq_server'
        cfg['queue']['inter_host_client'] = 'xact.queue.zmq_client'

    return cfg

    # default_rules: (
    #     {'edge_type': 'inter_host_server',
    #      'from_node': ('python',),
    #      'to_node':   ('python',),
    #      'tags':      ('*',),
    #      'logic':     'xact.queue.zmq_server'},
    #     {'edge_type': 'inter_host_client',
    #      'from_node': ('python',),
    #      'to_node':   ('python',),
    #      'tags':      ('*',),
    #      'logic':     'xact.queue.zmq_client'},
    #     {'edge_type': 'inter_process_server',
    #      'from_node': ('python',),
    #      'to_node':   ('python',),
    #      'tags':      ('*',),
    #      'logic':     'xact.queue.multiprocessing'},
    #     {'edge_type': 'inter_process_client',
    #      'from_node': ('python',),
    #      'to_node':   ('python',),
    #      'tags':      ('*',),
    #      'logic':     'xact.queue.multiprocessing'},
    #     {'edge_type': 'intra_process',
    #     'from_node': ('python',),
    #     'to_node':   ('python',),
    #     'tags':      ('*',),
    #     'logic':      None})
    # cfg['queue']['default_rules'] = default_rules


