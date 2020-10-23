# -*- coding: utf-8 -*-
"""
Module of functions that support the configuration of data flow graph edges.

"""


import collections


#------------------------------------------------------------------------------
def denormalize(cfg):
    """
    Add redundant information to each edge to make it more convenient to use.

    """
    map_cfg_host    = cfg['host']
    map_cfg_process = cfg['process']
    map_cfg_node    = cfg['node']
    iter_cfg_edge   = cfg['edge']

    for (id_node, cfg_node) in map_cfg_node.items():
        id_process = cfg_node['process']
        id_host    = map_cfg_process[id_process]['host']
        map_cfg_node[id_node]['host'] = id_host

    set_id_host_remote_owner = set()
    map_idx_edge = collections.defaultdict(int)
    for cfg_edge in iter_cfg_edge:

        path_src          = cfg_edge['src']
        path_dst          = cfg_edge['dst']
        path_parts_src    = path_src.split('.')
        path_parts_dst    = path_dst.split('.')
        id_node_src       = path_parts_src[0]
        id_node_dst       = path_parts_dst[0]
        id_node_owner     = cfg_edge['owner']

        id_process_src    = map_cfg_node[id_node_src]['process']
        id_process_dst    = map_cfg_node[id_node_dst]['process']
        id_host_src       = map_cfg_node[id_node_src]['host']
        id_host_dst       = map_cfg_node[id_node_dst]['host']
        id_host_owner     = map_cfg_node[id_node_owner]['host']

        is_same_process   = (id_process_src == id_process_dst)
        is_same_host      = (id_host_src == id_host_dst)
        is_intra_process  = is_same_host and is_same_process
        is_inter_process  = is_same_host and (not is_same_process)
        is_inter_host     = (not is_same_host) and (not is_same_process)

        if is_intra_process:
            ipc_type = 'intra_process'
        elif is_inter_process:
            ipc_type = 'inter_process'
        elif is_inter_host:
            ipc_type = 'inter_host'
        else:
            raise RuntimeError(
                        'Cannot use one process_id on two different hosts')

        if is_inter_host:
            set_id_host_remote_owner.add(id_host_owner)
            idx_edge = map_idx_edge[id_host_owner]
            map_idx_edge[id_host_owner] += 1
        else:
            idx_edge = None

        cfg_edge['id_edge']         = '-'.join((path_src, path_dst))
        cfg_edge['relpath_src']     = path_parts_src[1:]
        cfg_edge['relpath_dst']     = path_parts_dst[1:]
        cfg_edge['id_node_src']     = id_node_src
        cfg_edge['id_node_dst']     = id_node_dst
        cfg_edge['list_id_process'] = [id_process_src, id_process_dst]
        cfg_edge['list_id_host']    = [id_host_src, id_host_dst]
        cfg_edge['ipc_type']        = ipc_type
        cfg_edge['id_host_owner']   = id_host_owner
        cfg_edge['id_host_src']     = id_host_src
        cfg_edge['id_host_dst']     = id_host_dst
        cfg_edge['idx_edge']        = idx_edge

    for (id_host, cfg_host) in map_cfg_host.items():
        if id_host in set_id_host_remote_owner:
            cfg_host['is_inter_host_edge_owner'] = True
        else:
            cfg_host['is_inter_host_edge_owner'] = False

    return cfg


