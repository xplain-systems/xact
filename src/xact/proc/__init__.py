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
    setproctitle = None  # pylint: disable=C0103

import xact.gen.python
import xact.log
import xact.node
import xact.signal
import xact.util


# -----------------------------------------------------------------------------
def start(cfg, id_process, id_process_host, map_queues):
    """
    Start the main loop in the local process and process host.

    """
    list_node = _configure(cfg, id_process, id_process_host, map_queues)
    try:
        return _run_main_loop_with_retry(list_node, id_process)
    except xact.signal.Halt as halt:
        return halt.return_code
    raise RuntimeError('Termination condition not recognized.')


# -----------------------------------------------------------------------------
def _configure(cfg, id_process, id_process_host, map_queues):
    """
    Configure the process and return the list of nodes to be executed.

    """
    xact.log.setup(cfg,
                   id_host    = id_process_host,
                   id_process = id_process)
    map_node = _instantiate_nodes(cfg, id_process)
    _config_edges(cfg, id_process, map_node, map_queues)
    list_node = _get_list_node_in_runorder(cfg, id_process, map_node)
    return list_node


# -----------------------------------------------------------------------------
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
        setproctitle.setproctitle(name_process)  # pylint: disable=I1101
    else:
        pass
    return name_process


# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
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
            _point(node, ['state'], map_alloc[cfg_node['state_type']]())
            map_node[id_node] = node
    return map_node


# -----------------------------------------------------------------------------
def _config_edges(cfg, id_process, map_node, map_queues):
    """
    Configure the edges in the data flow graph.

    """
    map_alloc = xact.gen.python.map_allocator(cfg['data'])
    for cfg_edge in cfg['edge']:

        if id_process not in cfg_edge['list_id_process']:
            continue

        id_node_src = cfg_edge['id_node_src']
        id_node_dst = cfg_edge['id_node_dst']
        relpath_src = tuple(cfg_edge['relpath_src'])  # i.e outputs.output-name
        relpath_dst = tuple(cfg_edge['relpath_dst'])  # i.e inputs.input-name
        memory      = map_alloc[cfg_edge['data']]()

        # Intra process comms uses shared memory
        if cfg_edge['ipc_type'] == 'intra_process':
            _point(map_node[id_node_src], relpath_src, memory)
            _point(map_node[id_node_dst], relpath_dst, memory)

        # Inter_process or inter_host comms use a queue.
        else:

            queue = map_queues[cfg_edge['id_edge']]
            is_src_end = (id_node_src in map_node)
            if is_src_end:
                node = map_node[id_node_src]
                _point(node, relpath_src, memory)
                relpath_queue_src = relpath_src[1:]
                node.output_queues[relpath_queue_src] = queue

            else:  # is_dst_end
                node = map_node[id_node_dst]
                _point(node, relpath_dst, memory)
                relpath_queue_dst = relpath_dst[1:]
                node.input_queues[relpath_queue_dst] = queue


# -----------------------------------------------------------------------------
def _point(node, path, memory):
    """
    Make the specified node and path point to the specified memory.

    """
    ref = node.__dict__
    for name in path[:-1]:
        ref = ref[name]

    if isinstance(ref, xact.util.RestrictedWriteDict):
        ref._xact_framework_internal_setitem(path[-1], memory)
    else:
        ref[path[-1]] = memory


# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
def _get_list_id_node_in_runorder(cfg, id_process):
    """
    Return a list of node ids sorted by order of execution.

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
    (map_forward, map_backward) = _local_acyclic_data_flow(
                                                iter_cfg_edge = cfg['edge'],
                                                id_process    = id_process)
    list_tranches  = xact.util.topological_sort(map_forward, map_backward)
    list_id_node   = list(_specify_detailed_execution_order(list_tranches))
    list_id_node.extend(sorted(_get_list_id_node_unscheduled(
                                            cfg, list_id_node, id_process)))
    return list_id_node


# -----------------------------------------------------------------------------
def _local_acyclic_data_flow(iter_cfg_edge, id_process):
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


# -----------------------------------------------------------------------------
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
            yield id_node


# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
def _run_main_loop_with_retry(list_node, id_process):
    """
    Repeatedly step the specified nodes in order, resetting upon exception.

    """
    idx_try = 0
    while True:
        xact.log.logger.info(
                        'Reset and run {proc} (Attempt {idx})',
                        proc = id_process,
                        idx  = idx_try)

        try:
            reset(list_node)
            _run_main_loop(list_node)
        except xact.signal.ResetAndRetry:
            xact.log.logger.info(
                            'Reset and retry {proc}',
                            proc = id_process,
                            idx  = idx_try)
            idx_try += 1
            continue


# -----------------------------------------------------------------------------
def reset(list_node):
    """
    Reset all of the nodes in the list.

    """
    list_signals = []
    for node in list_node:
        signal = node.reset()
        if signal is not None:
            list_signals.append(signal)

    _handle_signals(list_signals)


# -----------------------------------------------------------------------------
def _run_main_loop(list_node):
    """
    Repeatedly step the specified nodes in order.

    """
    while True:
        step(list_node)


# -----------------------------------------------------------------------------
def step(list_node):
    """
    Run a single simulation step.

    """
    list_signals = []

    for node in list_node:
        signal = node.step()
        if signal is not None:
            list_signals.append(signal)

    _handle_signals(list_signals)


# -----------------------------------------------------------------------------
def _handle_signals(list_signals):
    """
    Handle signals.

    """
    if not list_signals:
        return

    # Signals that sould be raised (in priority order).
    priority_order = (
        'NonRecoverableError',
        'Halt',
        'ResetAndRetry')

    for name in priority_order:
        for signal in list_signals:
            if type(signal).__name__ == name:
                raise signal
