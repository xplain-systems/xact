# -*- coding: utf-8 -*-
"""
Module of utility functions and classes used across the xact system.

"""


import collections
import collections.abc
import itertools
import string


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
                parent[key] = cursor.format(**map_data)

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
    if is_python_2:
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


#------------------------------------------------------------------------------
def topological_sort(map_edge):
    """
    Return graph nodes as a list of sets of equivalent rank in topological order.

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
    map_forward  = map_edge
    map_backward = collections.defaultdict(set)
    for (key, set_value) in map_forward.items():
        for value in set_value:
            map_backward[value].add(key)

    set_node_out     = set(map_forward.keys())   # nodes with outbound edge(s)
    set_node_in      = set(map_backward.keys())  # nodes With inbound edge(s)
    set_node_sources = set_node_out - set_node_in
    set_node_sinks   = set_node_in  - set_node_out

    map_count_in = dict((key, 0) for key in set_node_sources)
    for (key, inbound) in map_backward.items():
        map_count_in[key] = len(inbound)

    set_count_zero = _nodes_at_count_zero(map_count_in)
    _del_items(map_count_in, set_count_zero)

    visited    = set()
    list_ranks = list()
    list_ranks = [set_count_zero]

    for idx in itertools.count():
        set_prev = list_ranks[-1]
        for id_node in _downstream_neighbors(set_prev, map_forward):
            map_count_in[id_node] -= 1
        set_next = _nodes_at_count_zero(map_count_in)
        _del_items(map_count_in, set_next)

        if set_next:
            list_ranks.append(set_next)
            continue
        else:
            break

    return list_ranks


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


# =============================================================================
class PathDict(collections.UserDict):
    """
    Custom dictionary class supporting path-tuple based access.

    """

    # -------------------------------------------------------------------------
    def __getitem__(self, key):
        """
        Return a reference to the specified item in data.

        """
        reference = self.data
        for name in self._ensure_list(key):
            reference = reference[name]
        return reference

    # -------------------------------------------------------------------------
    def __setitem__(self, key, value):
        """
        Return a reference to the specified item in data.

        """
        reference = self.data
        key       = self._ensure_list(key)

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
    def _ensure_list(self, key):
        """
        Ensure that key is represented as a list of names.

        """
        if isinstance(key, str):
            list_str  = key.split('.')
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
    exec(string_source, dict(), fcn_locals)
    for value in fcn_locals.values():
        if callable(value):
            list_functions.append(value)
    assert len(list_functions) == 1
    return list_functions[0]
