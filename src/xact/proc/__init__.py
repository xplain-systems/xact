# -*- coding: utf-8 -*-
"""
Package of functions that support the operation of individual processes.

"""


import collections
import itertools
import multiprocessing

try:
    import setproctitle
except ModuleNotFoundError:
    setproctitle = None

import xact.gen.python
import xact.node
import xact.log
import xact.sys.exception


#------------------------------------------------------------------------------
def start(cfg, id_process, id_process_host, map_queues):
    """
    Start the main loop in the local process and process host.

    """
    list_node = _configure(cfg, id_process, id_process_host, map_queues)
    try:
        return _run_main_loop_with_retry(list_node, id_process)
    except xact.sys.exception.RunComplete:
        return 0
    raise RuntimeError('Termination condition not recognized.')


#------------------------------------------------------------------------------
def _configure(cfg, id_process, id_process_host, map_queues):
    """
    Configure the process and return the list of nodes to be executed.

    """
    xact.log.setup(cfg,
                   id_host    = id_process_host,
                   id_process = id_process)
    name_process = _set_process_title()
    map_node = _instantiate_nodes(cfg, id_process)
    _config_edges(cfg, id_process, id_process_host, map_node, map_queues)
    list_node = _get_list_node_in_runorder(cfg, id_process, map_node)
    return list_node


#------------------------------------------------------------------------------
def _set_process_title():
    """
    Set the title of the current process from its name, where possible.

    I've not been able to install setproctitle
    on Ubuntu 16.04, so I've made it optional
    until I don't have to support that platform
    any more.

    """
    obj_process  = multiprocessing.current_process()
    name_process = obj_process.name
    if setproctitle is not None:
        setproctitle.setproctitle(name_process)
    else:
        pass
    return name_process


#------------------------------------------------------------------------------
def processes_on_host(cfg, id_host):
    """
    Return the set of all processes on the specified host.

    """
    set_id_process   = set()
    dict_cfg_process = cfg['process']
    for id_process in dict_cfg_process.keys():
        if dict_cfg_process[id_process]['host'] == id_host:
            set_id_process.add(id_process)
    return set_id_process


#------------------------------------------------------------------------------
def _instantiate_nodes(cfg, id_process):
    """
    Return a map from id_node to an instantiated node object.

    """
    map_alloc = xact.gen.python.map_allocator(cfg['data'])
    map_node  = dict()
    for (id_node, cfg_node) in cfg['node'].items():
        if cfg_node['process'] == id_process:
            cfg['runtime']['id']['id_process'] = id_process
            cfg['runtime']['id']['id_node']    = id_node
            node = xact.node.Node(cfg, id_node)
            _point(node, ['state_type'], map_alloc[cfg_node['state_type']]())
            map_node[id_node] = node
    return map_node


#------------------------------------------------------------------------------
def _config_edges(cfg, id_process, id_process_host, map_node, map_queues):
    """
    Configure the edges in the data flow graph.

    """
    map_alloc = xact.gen.python.map_allocator(cfg['data'])
    for cfg_edge in cfg['edge']:

        if id_process not in cfg_edge['list_id_process']:
            continue

        id_node_src = cfg_edge['id_node_src']
        id_node_dst = cfg_edge['id_node_dst']
        relpath_src = tuple(cfg_edge['relpath_src'])
        relpath_dst = tuple(cfg_edge['relpath_dst'])
        memory      = map_alloc[cfg_edge['data']]()

        # Intra process comms uses shared memory
        if 'intra_process' == cfg_edge['ipc_type']:
            _point(map_node[id_node_src], relpath_src, memory)
            _point(map_node[id_node_dst], relpath_dst, memory)

        # Inter_process or inter_host comms use a queue.
        else:

            queue = map_queues[cfg_edge['id_edge']]

            is_src_end = (id_node_src in map_node)
            if is_src_end:
                node = map_node[id_node_src]
                _point(node, relpath_src, memory)
                node.output_queues[relpath_src[1:]] = queue

            else:  # is_dst_end
                node = map_node[id_node_dst]
                _point(node, relpath_dst, memory)
                node.input_queues[relpath_dst[1:]] = queue



#------------------------------------------------------------------------------
def _point(node, path, memory):
    """
    Make the specified node and path point to the specified memory.

    """
    ref = node.__dict__
    for name in path[:-1]:
        ref = ref[name]
    ref[path[-1]] = memory


#------------------------------------------------------------------------------
def _get_list_node_in_runorder(cfg, id_process, map_node):
    """
    Return a list of node objects sorted by order of execution.

    """
    list_id_node = _get_list_id_node_in_runorder(cfg, id_process)
    list_node    = list()
    for id_node in list_id_node:
        node = map_node[id_node]
        list_node.append(node)
    return list_node


#------------------------------------------------------------------------------
def _get_list_id_node_in_runorder(cfg, id_process):
    """
    Return a list of node ids sorted by order of execution.

    """
    (map_forward, map_backward) = _local_data_flow(iter_cfg_edge = cfg['edge'],
                                                   id_process    = id_process)
    list_tranches  = _sort_into_execution_tranches(map_forward, map_backward)
    list_id_node   = list(_specify_detailed_execution_order(list_tranches))
    list_id_node.extend(sorted(_get_list_id_node_unscheduled(
                                            cfg, list_id_node, id_process)))
    return list_id_node


#------------------------------------------------------------------------------
def _local_data_flow(iter_cfg_edge, id_process):
    """
    Return the data flow graph for the specified process.

    The data flow graph is returned as a pair of dicts.
    The first dict maps from upstream nodes to downstream
    nodes, and the second dict maps the reverse, from
    downstream nodes to upstream nodes.

    """
    map_forward  = collections.defaultdict(set)
    map_backward = collections.defaultdict(set)

    for cfg_edge in iter_cfg_edge:
        is_intra_process = cfg_edge['ipc_type'] == 'intra_process'
        is_local_process = id_process in cfg_edge['list_id_process']
        if is_intra_process and is_local_process:
            id_node_src = cfg_edge['id_node_src']
            id_node_dst = cfg_edge['id_node_dst']
            map_forward[id_node_src].add(id_node_dst)
            map_backward[id_node_dst].add(id_node_src)

    return (map_forward, map_backward)


#------------------------------------------------------------------------------
def _sort_into_execution_tranches(map_forward, map_backward):
    """
    Return data flow graph nodes as a list of tranches in execution order.

    Execution order is not fully specified by
    a breadth-first forwards traversal of the data
    flow graph, since nodes at the same 'depth'
    in the graph can be executed in arbitrary order.

    To give us the freedom to decide amongst
    those possible orderings later, we return
    a list of sets, where each entry in the
    list is comprised of the set of nodes at
    the same 'depth' in the graph.

    Graphs must be acyclic. If we want to handle
    feedback loops, this algorithm must be
    changed to deal with them in an intelligent
    manner.

    """
    set_node_out     = set(map_forward.keys())   # nodes with outbound edge(s)
    set_node_in      = set(map_backward.keys())  # nodes With inbound edge(s)
    set_node_sources = set_node_out - set_node_in
    set_node_sinks   = set_node_in  - set_node_out

    map_count_in = dict((key, 0) for key in set_node_sources)
    for (key, inbound) in map_backward.items():
        map_count_in[key] = len(inbound)

    set_count_zero = _nodes_at_count_zero(map_count_in)
    _del_items(map_count_in, set_count_zero)

    visited       = set()
    list_tranches = list()
    list_tranches = [set_count_zero]

    for idx in itertools.count():
        set_prev = list_tranches[-1]
        for id_node in _downstream_neighbors(set_prev, map_forward):
            map_count_in[id_node] -= 1
        set_next = _nodes_at_count_zero(map_count_in)
        _del_items(map_count_in, set_next)

        if set_next:
            list_tranches.append(set_next)
            continue
        else:
            break

    return list_tranches


#------------------------------------------------------------------------------
def _nodes_at_count_zero(map_count_in):
    """
    Return the set of id_node with input degree zero.

    """
    return set(key for (key, count) in map_count_in.items() if count == 0)


#------------------------------------------------------------------------------
def _del_items(map_data, set_keys):
    """
    Delete the specified items from the dict.

    """
    for key in set_keys:
        del map_data[key]


#------------------------------------------------------------------------------
def _downstream_neighbors(set_id_node, map_forward):
    """
    Return the set of source nodes from the specified graph.

    The graph should be provided as a dict mapping
    from upstream nodes to downstream nodes.

    """
    set_neighbors = set()
    for id_node in set_id_node:
        if id_node in map_forward:
            set_neighbors |= map_forward[id_node]
    return set_neighbors


#------------------------------------------------------------------------------
def _specify_detailed_execution_order(list_tranches):
    """
    Yield each node in final execution order.

    Within each tranche, execution order
    is determined by sorting on the id_node,
    just to give us a predictable, deterministic
    execution ordering.

    """
    for tranche in list_tranches:
        for id_node in sorted(tranche):
            yield(id_node)


#------------------------------------------------------------------------------
def _get_list_id_node_unscheduled(cfg, list_id_node, id_process):
    """
    Retirm a list of unscheduled node ids in the specified process.

    """
    set_id_node              = set(list_id_node)
    list_id_node_unscheduled = list()
    for (id_node, cfg_node) in cfg['node'].items():
        is_in_process = id_process == cfg_node['process']
        is_scheduled  = (id_node in set_id_node)
        if is_in_process and (not is_scheduled):
            list_id_node_unscheduled.append(id_node)
    return list_id_node_unscheduled


#------------------------------------------------------------------------------
def _run_main_loop_with_retry(list_node, id_process):
    """
    Repeatedly step the specified nodes in order, resetting upon exception.

    """
    idx_try = 0
    while True:
        xact.log.logger.info(
                'Reset and run main loop for {proc} (Attempt {idx})',
                                                            proc = id_process,
                                                            idx  = idx_try)

        try:
            reset(list_node)
            _run_main_loop(list_node)
        except xact.sys.exception.RecoverableError:
            xact.log.logger.info(
                    'Recoverable error for {proc} on attempt {idx}',
                                                            proc = id_process,
                                                            idx  = idx_try)
            idx_try += 1
            continue


#------------------------------------------------------------------------------
def reset(list_node):
    """
    Reset all of the nodes in the list.

    """
    for node in list_node:
        node.reset()


#------------------------------------------------------------------------------
def _run_main_loop(list_node):
    """
    Repeatedly step the specified nodes in order.

    """
    while True:
        step(list_node)


#------------------------------------------------------------------------------
def step(list_node):
    """
    Run a single simulation step.

    """

    raised = []

    # for node in list_node:
    #     node.pre_step()

    for node in list_node:
        try:
            node.step()
        except xact.sys.exception.RunComplete as ex:
            raised.append(ex)

    # for node in list_node:
    #     node.post_step()

    if raised:
        ex = raised[0]
        raise ex
