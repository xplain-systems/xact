# -*- coding: utf-8 -*-
"""
Functional specification for the xact.cfg.builder module.

"""


import pytest


# =============================================================================
class SpecifySetSystemId:
    """
    Spec for the builder.set_system_id function.

    """

    # -------------------------------------------------------------------------
    def it_returns_valid_configuration(self, valid_normalized_config):
        """
        The builder.set_system_id function returns valid configuration.

        """
        import xact.cfg.builder
        import xact.cfg.validate
        xact.cfg.builder.set_system_id(cfg       = valid_normalized_config,
                                       id_system = 'new_system_id')
        xact.cfg.validate.normalized(valid_normalized_config)


# =============================================================================
class SpecifyAddHost:
    """
    Spec for the builder.add_host function.

    """

    # -------------------------------------------------------------------------
    def it_returns_valid_configuration(self, valid_normalized_config):
        """
        The builder.add_host function returns valid configuration.

        """
        import xact.cfg.builder
        import xact.cfg.validate
        xact.cfg.builder.add_host(cfg     = valid_normalized_config,
                                  id_host = 'new_host_id')
        xact.cfg.validate.normalized(valid_normalized_config)


# =============================================================================
class SpecifyRemoveHost:
    """
    Spec for the builder.remove_host function.

    """

    # -------------------------------------------------------------------------
    def it_returns_valid_configuration(self, valid_normalized_config):
        """
        The builder.remove_host function returns valid configuration.

        """
        import xact.cfg.builder
        import xact.cfg.validate
        xact.cfg.builder.add_host(cfg     = valid_normalized_config,
                                  id_host = 'new_host_id')
        xact.cfg.builder.remove_host(cfg     = valid_normalized_config,
                                     id_host = 'new_host_id')
        xact.cfg.validate.normalized(valid_normalized_config)


# =============================================================================
class SpecifyAddProcess:
    """
    Spec for the builder.add_process function.

    """

    # -------------------------------------------------------------------------
    def it_returns_valid_configuration(self, valid_normalized_config):
        """
        The builder.add_process function returns valid configuration.

        """
        import xact.cfg.builder
        import xact.cfg.validate
        xact.cfg.builder.add_process(cfg        = valid_normalized_config,
                                     id_process = 'new_process_id',
                                     id_host    = 'some_host')
        xact.cfg.validate.normalized(valid_normalized_config)


# =============================================================================
class SpecifyRemoveProcess:
    """
    Spec for the builder.remove_process function.

    """

    # -------------------------------------------------------------------------
    def it_returns_valid_configuration(self, valid_normalized_config):
        """
        The builder.remove_process function returns valid configuration.

        """
        import xact.cfg.builder
        import xact.cfg.validate
        xact.cfg.builder.add_process(cfg        = valid_normalized_config,
                                     id_process = 'new_process_id',
                                     id_host    = 'some_host')
        xact.cfg.builder.remove_process(cfg        = valid_normalized_config,
                                        id_process = 'new_process_id')
        xact.cfg.validate.normalized(valid_normalized_config)


# =============================================================================
class SpecifyAddNode:
    """
    Spec for the builder.add_node function.

    """

    # -------------------------------------------------------------------------
    def it_returns_valid_configuration(self, valid_normalized_config):
        """
        The builder.add_node function returns valid configuration.

        """
        import xact.cfg.builder
        import xact.cfg.validate
        xact.cfg.builder.add_node(
                        cfg           = valid_normalized_config,
                        id_node       = 'new_node_id',
                        functionality = {
                            'requirement': 'some_requirement',
                            'py_module':   'some.importable.module'},
                        id_process    = 'some_process')
        xact.cfg.validate.normalized(valid_normalized_config)


# =============================================================================
class SpecifyRemoveNode:
    """
    Spec for the builder.remove_node function.

    """

    # -------------------------------------------------------------------------
    def it_returns_valid_configuration(self, valid_normalized_config):
        """
        The builder.remove_node function returns valid configuration.

        """
        import xact.cfg.builder
        import xact.cfg.validate
        xact.cfg.builder.add_node(
                        cfg           = valid_normalized_config,
                        id_node       = 'new_node_id',
                        functionality = {'py_module': 'some.importable.module'},
                        id_process    = 'some_process')
        xact.cfg.builder.remove_node(cfg     = valid_normalized_config,
                                     id_node = 'new_node_id')
        xact.cfg.validate.normalized(valid_normalized_config)


# =============================================================================
class SpecifyAddEdge:
    """
    Spec for the builder.add_edge function.

    """

    # -------------------------------------------------------------------------
    def it_returns_valid_configuration(self, valid_normalized_config):
        """
        The builder.add_edge function returns valid configuration.

        """
        import xact.cfg.builder
        import xact.cfg.validate
        xact.cfg.builder.add_edge(cfg     = valid_normalized_config,
                                  id_src  = 'some_node',
                                  src_ref = 'outputs.foo',
                                  id_dst  = 'some_node',
                                  dst_ref = 'inputs.foo',
                                  data    = 'some_data_type')
        xact.cfg.validate.normalized(valid_normalized_config)


# =============================================================================
class SpecifyRemoveEdge:
    """
    Spec for the builder.remove_edge function.

    """

    # -------------------------------------------------------------------------
    def it_returns_valid_configuration(self, valid_normalized_config):
        """
        The builder.remove_edge function returns valid configuration.

        """
        import xact.cfg.builder
        import xact.cfg.validate
        xact.cfg.builder.add_edge(cfg     = valid_normalized_config,
                                  id_src  = 'some_node',
                                  src_ref = 'output.new_port',
                                  id_dst  = 'some_node',
                                  dst_ref = 'input.new_port',
                                  data    = 'some_data_type')
        xact.cfg.builder.remove_edge(cfg = valid_normalized_config,
                                     src = 'some_node.output.new_port',
                                     dst = 'some_node.input.new_port')
        xact.cfg.validate.normalized(valid_normalized_config)


# =============================================================================
class SpecifyAddData:
    """
    Spec for the builder.add_data function.

    """

    # -------------------------------------------------------------------------
    def it_returns_valid_configuration(self, valid_normalized_config):
        """
        The builder.add_data function returns valid configuration.

        """
        import xact.cfg.builder
        import xact.cfg.validate
        xact.cfg.builder.add_data(cfg       = valid_normalized_config,
                                  id_data   = 'new_type_alias',
                                  spec_data = 'py_dict')
        xact.cfg.validate.normalized(valid_normalized_config)


# =============================================================================
class SpecifyRemoveData:
    """
    Spec for the builder.remove_data function.

    """

    # -------------------------------------------------------------------------
    def it_returns_valid_configuration(self, valid_normalized_config):
        """
        The builder.remove_data function returns valid configuration.

        """
        import xact.cfg.builder
        import xact.cfg.validate
        xact.cfg.builder.add_data(cfg       = valid_normalized_config,
                                  id_data   = 'new_type_alias',
                                  spec_data = 'py_dict')
        xact.cfg.builder.remove_data(cfg     = valid_normalized_config,
                                     id_data = 'new_type_alias')
        xact.cfg.validate.normalized(valid_normalized_config)
