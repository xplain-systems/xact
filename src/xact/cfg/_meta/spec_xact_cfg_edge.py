# -*- coding: utf-8 -*-
"""
Functional specification for the xact.cfg.edge module.

"""


import pytest


# =============================================================================
class SpecifyDenormalize:
    """
    Spec for the edge.denormalize function.

    """

    # -------------------------------------------------------------------------
    def it_adds_fields(self, valid_normalized_config):
        """
        The edge.denormalize function adds fields to the config structure.

        """
        import xact.cfg.edge

        cfg_denorm = xact.cfg.edge.denormalize(valid_normalized_config)

        for cfg_edge in cfg_denorm['edge']:
            assert 'id_edge' in cfg_edge
            assert 'relpath_src' in cfg_edge
            assert 'relpath_dst' in cfg_edge
            assert 'id_node_src' in cfg_edge
            assert 'id_node_dst' in cfg_edge
            assert 'list_id_process' in cfg_edge
            assert 'list_id_host' in cfg_edge
            assert 'ipc_type' in cfg_edge
            assert 'id_host_owner' in cfg_edge
            assert 'id_host_src' in cfg_edge
            assert 'id_host_dst' in cfg_edge
            assert 'idx_edge' in cfg_edge

        for cfg_host in cfg_denorm['host'].values():
            assert 'is_inter_host_edge_owner' in cfg_host
