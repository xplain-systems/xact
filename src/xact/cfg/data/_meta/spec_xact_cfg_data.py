# -*- coding: utf-8 -*-
"""
Functional specification for the xact.cfg.data package.

"""


import pytest


# =============================================================================
class SpecifyDenormalize:
    """
    Spec for the xact.cfg.data.denormalize function.

    """

    # -------------------------------------------------------------------------
    def it_returns_valid_data(self, valid_partly_denormalized_config):
        """
        The denormalize function returns a valid denormalized config structure.

        """
        import xact.cfg.data
        import xact.cfg.validate
        denormalized_config = xact.cfg.data.denormalize(
                                            valid_partly_denormalized_config)
        xact.cfg.validate.denormalized(denormalized_config)


# =============================================================================
class Specify_ExpandNode:
    """
    Spec for the xact.cfg.data.denormalize function.

    """

    # -------------------------------------------------------------------------
    def it_returns_an_expanded_node(self):
        """
        The denormalize function returns a valid denormalized config structure.

        """
        import xact.cfg.data
        named_type = xact.cfg.data.FieldCategory.named_type
        SubsTab    = xact.cfg.util.SubstitutionTable
        output     = xact.cfg.data._expand_node(
                            node = xact.cfg.data.Node(line     = 1,
                                                      col      = 2,
                                                      level    = 3,
                                                      path     = [],
                                                      name     = 'name',
                                                      spec     = 'my_param',
                                                      category = named_type),
                            subs = SubsTab({'my_param': 'some_float_type'}), 
                            typeinfo = {'some_float_type': {'py': float}}, 
                            idx = 4)

        expected_output = {
            '_node_info': {
                'category':     'named_type',
                'memory_order': 'C',
                'preset':       0.0,
                'shape':        None,
                'src_line':     1,
                'src_col':      2,
                'src_level':    3,
                'src_path':     ['name'],
                'src_seqnum':   4,
                'typeinfo': {
                    'py': float
                }
            }
        }

        assert output == expected_output
