# -*- coding: utf-8 -*-
"""
Module of utility functions and classes used across the xact system.

"""


import collections
import collections.abc
import itertools
import string


# -----------------------------------------------------------------------------
def clear_outputs(outputs,
                  list_name_output,
                  list_field_to_clear,
                  has_ena = True):
    """
`   Clear the specified fields in the specified outputs.

    """
    map_ctors = {
        'list': list,
        'map':  dict,
        'set':  set,
    }
    for id_out in list_name_output:
        if has_ena:
            outputs[id_out]['ena'] = False
        for id_field in list_field_to_clear:
            if id_field not in outputs[id_out]:
                outputs[id_out][id_field] = map_ctors[id_field]()
            else:
                outputs[id_out][id_field].clear()


# -----------------------------------------------------------------------------
def format_all_strings(map_data):
    """
    Return the specified data structure with formatted string fields.

    The input shall be a python dict, list or tuple,
    which may be nested to an arbitrary depth.
    This data structure can be seen as a tree.
    The internal branches of this tree shall be
    of some dict, list or tuple type, and the
    leaves of the tree shall be values of some
    boolean, numeric, or string type.

    Leaves which are of string type may either be
    PEP3101 format strings, or they may be 'vanilla'
    python strings.

    For all PEP3101 format strings in the data
    structure, the name of the format string
    arguments shall be a dot delimited path which
    references some other string leaf in the input
    data structure.

    In this way, each argument found in a format
    strings leaf shall provide a reference to some
    other string (format string or otherwise) in
    the data structure, forming an implicit
    directed graph of references.

    This implicit directed graph shall be acyclic.

    The purpose of this function is to replace the
    format strings with their 'formatted' output,
    using the topological ordering of the implicit
    graph of references to determine the sequence
    of formatting operations.

    """
    map_dst = collections.defaultdict(set)  # id_field -> {path_to_dst}
    map_src = dict()                        # id_field -> path_to_src
    for (tup_path, leaf) in walkobj(map_data,
                                    gen_leaf    = True,
                                    gen_nonleaf = False,
                                    gen_path    = True,
                                    gen_obj     = True):

        # Any field is a potential source of a parameter value.
        id_field          = '.'.join(str(item) for item in tup_path)
        map_src[id_field] = tup_path

        # Format strings are "destinations" consuming parameter values.
        if is_format_string(leaf):
            for id_field in iter_format_string_fields(leaf):
                map_dst[id_field].add(tup_path)

    # Build the dependency graph between leaves in the content tree.
    map_edge = collections.defaultdict(set)  # path_to_src -> {path_to_dst}
    for (id_field, set_path_dst) in map_dst.items():
        assert id_field in map_src
        path_src = map_src[id_field]
        map_edge[path_src].update(set_path_dst)

    # Format content in topological order.
    for set_rank in topological_sort(map_edge):
        for tup_path in sorted(set_rank):
            cursor = map_data
            parent = None
            for key in tup_path:
                parent = cursor
                cursor = cursor[key]
            if is_format_string(cursor):
                parent[tup_path[-1]] = cursor.format(**map_data)

    return map_data


# -----------------------------------------------------------------------------
def walkobj(obj,                                  # pylint: disable=R0912,R0913
            gen_leaf    = False,
            gen_nonleaf = False,
            gen_path    = False,
            gen_obj     = False,
            path        = (),
            memo        = None):
    """
    Generate a walk over a treelike structure of mappings and other iterables.

    Adapted from:
    http:/code.activestate.com/recipes/577982-recursively-walk-python-objects/

    This function performs a (depth-first left to
    right recursive) traversal over the provided
    data structure, which we assume to consist of
    a finite; treelike arrangement of nested
    collections.Mapping and collections.Iterable
    types.

    This function can be configured to yield
    information about nodes in the tree: leaf
    nodes; non-leaf (internal) nodes or both.

    The information that is yielded may also be
    configured: the path to the node can be
    delivered as can the object at the node
    itself.

    ---
    type:   generator
    ...

    """
    # If the object is elemental, it cannot be
    # decomposed, so we must bottom out the
    # recursion and yield the object and its'
    # path before returning control back up
    # the stack.
    #
    is_itable    = isinstance(obj, collections.abc.Iterable)
    is_leaf      = (not is_itable) or is_string(obj)

    if is_leaf:
        if gen_leaf:
            if gen_path and gen_obj:
                yield (path, obj)
            elif gen_path:
                yield path
            elif gen_obj:
                yield obj
        return

    # Since this is a recursive function, we need
    # to be on our guard against any references
    # to objects back up the call stack (closer
    # to the root of the tree). Any such
    # references would be circular, leading to
    # an infinite tree, and causing us to blow
    # our stack in a fit of unbounded recursion.
    #
    # If we detect that we've already visited
    # this object (using identity not equality),
    # then the safe thing to do is to halt the
    # recursive descent and return control back
    # up the stack.
    #
    _id = id(obj)
    if memo is None:
        memo = set()
    if _id in memo:
        return
    memo.add(_id)

    # If the object is not elemental (i.e. it is
    # an Iterable), then it may be decomposed, so
    # we should recurse down into each component,
    # yielding the results as we go. Of course,
    # we need different iteration functions for
    # mappings vs. other iterables.
    #
    def mapiter(mapping):
        """
        Return an iterator over the specified mapping or other iterable.

        This function selects the appropriate
        iteration function to use.

        """
        return getattr(mapping, 'iteritems', mapping.items)()
    itfcn = mapiter if isinstance(obj, collections.abc.Mapping) else enumerate

    for pathpart, component in itfcn(obj):

        childpath = path + (pathpart,)
        if gen_nonleaf:
            if gen_path and gen_obj:
                yield (childpath, component)
            elif gen_path:
                yield childpath
            elif gen_obj:
                yield component

        for result in walkobj(obj         = component,
                              gen_leaf    = gen_leaf,
                              gen_nonleaf = gen_nonleaf,
                              gen_path    = gen_path,
                              gen_obj     = gen_obj,
                              path        = childpath,
                              memo        = memo):
            yield result

    # We only need to guard against infinite
    # recursion within a branch of the call-tree.
    # There is no danger in visiting the same item
    # instance in sibling branches, so we can
    # forget about objects once we are done
    # with them and about to pop the stack.
    #
    memo.remove(_id)
    return


# -----------------------------------------------------------------------------
def is_format_string(maybe_fmt):
    """
    Return true if the supplied string is a format string.

    """
    if not is_string(maybe_fmt):
        return False
    iter_fields   = iter_format_string_fields(maybe_fmt)
    is_fmt_string = any(field is not None for field in iter_fields)
    return is_fmt_string


# -----------------------------------------------------------------------------
def is_string(obj):
    """
    Return true if obj is a string. Works in Python 2 and Python 3.

    """
    return isinstance(obj, string_types())


# -----------------------------------------------------------------------------
def string_types():
    """
    Return the string types for the current Python version.

    """
    is_python_2 = str is bytes
    if is_python_2:  # pylint: disable=R1705
        return (str, unicode)  # pylint: disable=E0602
    else:
        return (str, bytes)


# -----------------------------------------------------------------------------
def iter_format_string_fields(fmt_string):
    """
    Return an iterable over the field names in the specified format string.

    """
    return (tup[1] for tup in string.Formatter().parse(fmt_string)
                                                        if tup[1] is not None)


# -----------------------------------------------------------------------------
def topological_sort(map_forward, map_backward = None):
    """
    Return graph nodes as list of sets of equivalent rank in topological order.

    """
    # If the backward map has not been
    # specified, we can easily build it
    # by inverting the (bijective) forward
    # mapping.
    #
    if map_backward is None:
        map_backward = collections.defaultdict(set)
        for (key, set_value) in map_forward.items():
            for value in set_value:
                map_backward[value].add(key)

    set_node_out     = set(map_forward.keys())   # nodes with outbound edge(s)
    set_node_in      = set(map_backward.keys())  # nodes With inbound edge(s)
    set_node_sources = set_node_out - set_node_in

    # Build a map from id_node -> indegree
    # (count of inbound edges), populated
    # from the map_backward dict.
    #
    map_indegree = dict((key, 0) for key in set_node_sources)
    for (key, inbound) in map_backward.items():
        map_indegree[key] = len(inbound)

    # The output of the topological sort
    # is a partial ordering represented
    # as a list of sets; each set representing
    # nodes of equal rank. Here we create
    # the output list and fill it with
    # nodes at rank zero, removing them
    # from the graph.
    #
    set_rank_zero = _nodes_at_count_zero(map_indegree)
    list_set_ranks = [set_rank_zero]
    _del_items(map_indegree, set_rank_zero)

    # The topological sort algorithm
    # uses breadth first search. An
    # indegree number is maintained
    # for all nodes remaining in the
    # graph. At each iteration, the
    # 'source' nodes (indegree zero)
    # are taken from the graph and
    # added to the output, and the
    # indegree of immediate downstream
    # neighbors is decremented.
    #
    while True:

        # Maintain indegree number.
        set_prev = list_set_ranks[-1]
        for id_node in _list_downstream_neighbors(set_prev, map_forward):
            map_indegree[id_node] -= 1

        # Find the next rank - terminate
        # if it does not exist.
        #
        set_next = _nodes_at_count_zero(map_indegree)
        if not set_next:
            break

        # If the next rank does exist,
        # remove it from the graph and
        # add it to the list of ranks.
        #
        _del_items(map_indegree, set_next)
        list_set_ranks.append(set_next)

    return list_set_ranks


# -----------------------------------------------------------------------------
def _nodes_at_count_zero(map_indegree):
    """
    Return the set of id_node with input degree zero.

    """
    return set(key for (key, count) in map_indegree.items() if count == 0)


# -----------------------------------------------------------------------------
def _del_items(map_data, set_keys):
    """
    Delete the specified items from the dict.

    """
    for key in set_keys:
        del map_data[key]


# -----------------------------------------------------------------------------
def _list_downstream_neighbors(set_id_node, map_forward):
    """
    Return the list of downstream neighbors from the specified edge map.

    The edge map should be provided as a dict
    mapping from upstream nodes to downstream
    nodes.

    """
    list_neighbors = list()
    for id_node in set_id_node:
        if id_node in map_forward:
            for id_node_downstream in map_forward[id_node]:
                list_neighbors.append(id_node_downstream)
    return list_neighbors


# =============================================================================
class PathDict(collections.UserDict):  # pylint: disable=R0901
    """
    Custom dictionary class supporting path-tuple based access.

    """

    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        Return a PathDict instance.

        """
        self.delim = '.'
        super().__init__(*args, **kwargs)

    # -------------------------------------------------------------------------
    def __getitem__(self, key):
        """
        Return a reference to the specified item in data.

        """
        reference = self.data
        for name in _ensure_list(key, delim = self.delim):
            reference = reference[name]
        return reference

    # -------------------------------------------------------------------------
    def __setitem__(self, key, value):
        """
        Return a reference to the specified item in data.

        """
        reference = self.data

        # If we want to disable string
        # to list conversion, we can
        # set delim to None.
        #
        if self.delim is not None:
            key = _ensure_list(key, delim = self.delim)

        reference = self.data
        for name in key[:-1]:
            if name not in reference:
                reference[name] = dict()
            reference = reference[name]
        if key:
            reference[key[-1]] = value
        else:
            reference = value


# -------------------------------------------------------------------------
def _ensure_list(key, delim):
    """
    Ensure that key is represented as a list of names.

    """
    if isinstance(key, str):
        list_str  = key.split(delim)
        list_name = []
        for str_name in list_str:
            try:
                name = int(str_name)
            except ValueError:
                name = str_name
            list_name.append(name)
        key = list_name
    return key


# -----------------------------------------------------------------------------
def first(iterable):
    """
    Return the first item in the specified iterable.

    """
    return next(iter(iterable))


# -----------------------------------------------------------------------------
def gen_path_value_pairs_depth_first(map):
    """
    Yield (path, value) pairs taken from map in depth first order.

    """
    stack = collections.deque(_reversed_key_value_pairs(map))

    while True:

        try:
            (path_parent, value_parent) = stack.pop()
        except IndexError:
            return

        if not isinstance(path_parent, list):
            path_parent = [path_parent]

        if is_container(value_parent):
            for (key_child, value_child) in _reversed_key_value_pairs(
                                                                value_parent):

                stack.append((path_parent + [key_child], value_child))

        yield (tuple(path_parent), value_parent)


# -----------------------------------------------------------------------------
def _reversed_key_value_pairs(value):
    """
    Return a reversed list of the items taken from the specified dict or list.

    Input value must be a dict or a list.

    """
    if isinstance(value, dict):
        iter_key_value_pairs = value.items()

    if isinstance(value, list):
        iter_key_value_pairs = enumerate(value)

    return sorted(iter_key_value_pairs, reverse = True)


# -----------------------------------------------------------------------------
def is_container(value):
    """
    Return True iff the specified value is of a type that contains children.

    The biggest difficulty here is in distinguishing
    strings, byte arrays etc... from other sequence
    containers -- All we can really do is check
    explicitly whether they are instances of list()
    or tuple(). We can be more generic with Mappings
    and Sets since we have a suitable abstract base
    type for these.

    """
    is_map   = isinstance(value, collections.abc.Mapping)
    is_set   = isinstance(value, collections.abc.Set)
    is_list  = isinstance(value, list)
    is_tuple = isinstance(value, tuple)
    return is_map or is_set or is_list or is_tuple


# -------------------------------------------------------------------------
def function_from_source(string_source):
    """
    Return the first function found in the specified source listing.

    This calls exec on the supplied string.

    """
    list_functions = list()
    fcn_locals     = dict()
    exec(string_source, dict(), fcn_locals)  # pylint: disable=W0122
    for value in fcn_locals.values():
        if callable(value):
            list_functions.append(value)
    assert len(list_functions) == 1
    return list_functions[0]
