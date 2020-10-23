# -*- coding: utf-8 -*-
"""
Module defining how atomic data types are named and handled.

"""

import collections

import numpy


# -----------------------------------------------------------------------------
def as_dict():
    """
    Return a lookup table of information about atomic data types as a dict.

    Each entry in the table is a list of TypeInfo 
    named tuples. Each TypeInfo instance contains 
    the information that is needed to allocate, 
    serialize and deserialize the corresponding 
    atomic (fundamnetal) data type in each 
    of the supported programming languages.

    The table is indexed using a string.

    Xact is intended to be used to manage and 
    distribute compute bound workloads, and to 
    support interoperability between Python and 
    C.

    We use Numpy as a lingua franca to communicate 
    between compute nodes, as Numpy data types can 
    be used from both languages and are already 
    familiar to many developers of quantitative 
    Python applications.

    """

    map_typeinfo = dict()
    for typeinfo in _as_tuple():
        map_typeinfo[typeinfo.id] = dict(typeinfo._asdict())
    return map_typeinfo


# -----------------------------------------------------------------------------
def _as_tuple():
    """
    Return the lookup table as a tuple of named tuples.

    Each TypeInfo data structure describes how to 
    map between data types in different languages.

    """
    TypeInfo = collections.namedtuple('TypeInfo',
                    ['id',     # type id.
                     'py_eq',  # True if python type is binary-compatible.
                     'c_eq',   # True if c type is binary-compatible.
                     'align',  # True if can be aligned.
                     'py',     # python type.
                     'np',     # numpy type.
                     'c'])     # c type.

    return tuple(TypeInfo._make(tup) for tup in (
        ('py_bytearray', True,  False, True,  bytearray, None,             None,             ),
        ('py_bool',      True,  False, True,  bool,      None,             None,             ),
        ('py_str',       True,  False, True,  str,       None,             None,             ),
        ('py_bytes',     True,  False, True,  bytes,     None,             None,             ),
        ('py_dict',      True,  False, True,  dict,      None,             None,             ),
        ('py_list',      True,  False, True,  list,      None,             None,             ),
        ('py_set',       True,  False, True,  set,       None,             None,             ),
        ('bool_',        True,  False, False, bool,      numpy.bool_,      None,             ),
        ('bool8',        False, False, True,  bool,      numpy.bool8,      None,             ),
        ('byte',         False, True,  True,  int,       numpy.byte,       'char',           ),
        ('short',        False, True,  True,  int,       numpy.short,      'short',          ),
        ('intc',         False, True,  True,  int,       numpy.intc,       'int',            ),
        ('int',          False, True,  False, int,       numpy.int_,       'int',            ),
        ('int_',         False, True,  False, int,       numpy.int_,       'long',           ),
        ('longlong',     False, True,  True,  int,       numpy.longlong,   'long long',      ),
        ('intp',         False, True,  True,  int,       numpy.intp,       'void *',         ),
        ('int8',         False, True,  True,  int,       numpy.int8,       'int8_t',         ),
        ('int16',        False, True,  True,  int,       numpy.int16,      'int16_t',        ),
        ('int32',        False, True,  True,  int,       numpy.int32,      'int32_t',        ),
        ('int64',        False, True,  True,  int,       numpy.int64,      'int64_t',        ),
        ('ubyte',        False, True,  True,  int,       numpy.ubyte,      'unsigned char',  ),
        ('ushort',       False, True,  True,  int,       numpy.ushort,     'unsigned short', ),
        ('uintc',        False, True,  True,  int,       numpy.uintc,      'unsigned int',   ),
        ('uint',         True,  False, True,  int,       numpy.uint,       None,             ),
        ('ulonglong',    False, True,  True,  int,       numpy.ulonglong,  'long long',      ),
        ('uintp',        False, True,  True,  int,       numpy.uintp,      'void *',         ),
        ('uint8',        False, True,  True,  int,       numpy.uint8,      'uint8_t',        ),
        ('uint16',       False, True,  True,  int,       numpy.uint16,     'uint16_t',       ),
        ('uint32',       False, True,  True,  int,       numpy.uint32,     'uint32_t',       ),
        ('uint64',       False, True,  True,  int,       numpy.uint64,     'uint64_t',       ),
        ('half',         False, False, True,  float,     numpy.half,       None,             ),
        ('single',       False, True,  True,  float,     numpy.single,     'float',          ),
        ('double',       False, True,  True,  float,     numpy.double,     'double',         ),
        ('float_',       True,  False, False, float,     numpy.float_,     None,             ),
        ('longfloat',    False, True,  True,  float,     numpy.longfloat,  'long float',     ),
        ('float16',      False, False, True,  float,     numpy.float16,    None,             ),
        ('float32',      False, False, True,  float,     numpy.float32,    None,             ),
        ('float64',      False, False, True,  float,     numpy.float64,    None,             ),
        ('float128',     False, False, True,  float,     numpy.float128,   None,             ),
        ('csingle',      False, False, True,  float,     numpy.csingle,    None,             ),
        ('complex_',     True,  False, False, complex,   numpy.complex_,   None,             ),
        ('clongfloat',   False, False, True,  complex,   numpy.clongfloat, None,             ),
        ('complex64',    False, False, True,  complex,   numpy.complex64,  None,             ),
        ('complex128',   False, False, True,  complex,   numpy.complex128, None,             ),
        ('complex256',   False, False, True,  complex,   numpy.complex256, None,             )))
