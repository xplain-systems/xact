# -*- coding: utf-8 -*-
"""
Package of functions that support the operation of individual processes.

"""


import collections
import importlib
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
    id_system = cfg['system']['id_system']
    xact.log.setup(cfg_host   = cfg['host'].get(id_process_host, None),
                   id_system  = id_system,
                   id_host    = id_process_host,
                   id_process = id_process)

    map_cfg_node                   = cfg['node']
    iter_cfg_edge                  = cfg['edge']
    map_cfg_data                   = cfg['data']
    runtime                        = cfg['runtime']
    runtime['id']['id_system']     = id_system
    runtime['id']['id_process']    = id_process
    runtime['id']['id_host']       = id_process_host
    runtime['proc']['list_signal'] = []
    runtime['proc']['list_node']   = _configure(id_process,
                                                map_cfg_node,
                                                iter_cfg_edge,
                                                map_queues,
                                                map_cfg_data,
                                                runtime)
    try:
        return _run_main_loop_with_retry(
                        id_process  = id_process,
                        list_node   = runtime['proc']['list_node'],
                        list_signal = runtime['proc']['list_signal'])
    except xact.signal.Halt as halt:
        return halt.return_code
    raise RuntimeError('Termination condition not recognized.')


# -----------------------------------------------------------------------------
def ensure_imported(spec_module):
    """
    Import the specified module or throw a NonRecoverableError

    """
    module = None
    with xact.log.logger.catch():
        module = importlib.import_module(spec_module)
    if module is None:
        raise xact.signal.NonRecoverableError(cause = 'Module not found.')
    return module


# -----------------------------------------------------------------------------
def _configure(id_process,
               map_cfg_node,
               iter_cfg_edge,
               map_queues,
               map_cfg_data,
               runtime):
    """
    Configure the process and return the list of nodes to be executed.

    """
    map_alloc = xact.gen.python.map_allocator(map_cfg_data)
    map_node  = _instantiate_nodes(id_process,
                                   map_cfg_node,
                                   map_alloc,
                                   runtime)

    _configure_edges(id_process,
                     iter_cfg_edge,
                     map_node,
                     map_queues,
                     map_alloc)

    list_node = _get_list_node_in_runorder(id_process,
                                           map_cfg_node,
                                           iter_cfg_edge,
                                           map_node)
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
def _instantiate_nodes(id_process, map_cfg_node, map_alloc, runtime):
    """
    Return a map from id_node to an instantiated node object.

    """
    map_node  = dict()
    for (id_node, cfg_node) in map_cfg_node.items():
        if cfg_node['process'] == id_process:
            node = xact.node.Node(id_node, cfg_node, runtime)
            _point(node, ['state'], map_alloc[cfg_node['state_type']]())
            map_node[id_node] = node
    return map_node


# -----------------------------------------------------------------------------
def _configure_edges(id_process,
                     iter_cfg_edge,
                     map_node,
                     map_queues,
                     map_alloc):
    """
    Configure the edges in the data flow graph.

    """
    for cfg_edge in iter_cfg_edge:

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
def _get_list_node_in_runorder(id_process,
                               map_cfg_node,
                               iter_cfg_edge,
                               map_node):
    """
    Return a list of node objects sorted by order of execution.

    """
    return list(map_node[id_node] for id_node in
                    _get_list_id_node_in_runorder(
                                    id_process, map_cfg_node, iter_cfg_edge))


# -----------------------------------------------------------------------------
def _get_list_id_node_in_runorder(id_process, map_cfg_node, iter_cfg_edge):
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
                                            iter_cfg_edge = iter_cfg_edge,
                                            id_process    = id_process)
    list_tranches  = xact.util.topological_sort(map_forward, map_backward)
    list_id_node   = list(_specify_detailed_execution_order(list_tranches))
    list_id_node.extend(sorted(_get_list_id_node_unscheduled(
                                                        map_cfg_node,
                                                        list_id_node,
                                                        id_process)))
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
        is_feedforward   = cfg_edge['dirn'] == 'feedforward'
        is_intra_process = cfg_edge['ipc_type'] == 'intra_process'
        is_local_process = id_process in cfg_edge['list_id_process']
        if is_intra_process and is_local_process:
            id_node_src = cfg_edge['id_node_src']
            id_node_dst = cfg_edge['id_node_dst']
            if is_feedforward:
                map_forward[id_node_src].add(id_node_dst)
                map_backward[id_node_dst].add(id_node_src)
            else:  # is_feedback
                map_backward[id_node_src].add(id_node_dst)
                map_forward[id_node_dst].add(id_node_src)

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
def _get_list_id_node_unscheduled(map_cfg_node, list_id_node, id_process):
    """
    Retirm a list of unscheduled node ids in the specified process.

    """
    set_id_node              = set(list_id_node)
    list_id_node_unscheduled = list()
    for (id_node, cfg_node) in map_cfg_node.items():
        is_in_process = id_process == cfg_node['process']
        is_scheduled  = (id_node in set_id_node)
        if is_in_process and (not is_scheduled):
            list_id_node_unscheduled.append(id_node)
    return list_id_node_unscheduled


# -----------------------------------------------------------------------------
def _run_main_loop_with_retry(id_process, list_node, list_signal):
    """
    Repeatedly step the specified nodes in order, resetting upon exception.

    """
    idx_try = 0
    while True:
        xact.log.logger.info(
                    'Reset and run {proc} (Attempt {idx})', proc = id_process,
                                                            idx  = idx_try)

        try:
            reset(list_node, list_signal)
            _run_main_loop(list_node, list_signal)
        except xact.signal.ResetAndRetry:
            xact.log.logger.info('Reset and retry {proc}', proc = id_process,
                                                           idx  = idx_try)
            idx_try += 1
            continue


# -----------------------------------------------------------------------------
def reset(list_node, list_signal):
    """
    Reset all of the nodes in the list.

    """
    list_signal.clear()
    for node in list_node:
        signal = node.reset()
        if signal is not None:
            list_signal.append(signal)

    _handle_signals(list_signal)


# -----------------------------------------------------------------------------
def _run_main_loop(list_node, list_signal):
    """
    Repeatedly step the specified nodes in order.

    """
    while True:
        step(list_node, list_signal)


# -----------------------------------------------------------------------------
def step(list_node, list_signal):
    """
    Run a single simulation step.

    """
    list_signal.clear()
    for node in list_node:
        signal = node.step()
        if signal is not None:
            list_signal.append(signal)
    _handle_signals(list_signal)


# -----------------------------------------------------------------------------
def _handle_signals(list_signal):
    """
    Handle signals.

    """
    if not list_signal:
        return

    # Signals that sould be raised (in priority order).
    priority_order = (
        'NonRecoverableError',
        'Halt',
        'ResetAndRetry')

    for name in priority_order:
        for signal in list_signal:
            if type(signal).__name__ == name:
                raise signal
