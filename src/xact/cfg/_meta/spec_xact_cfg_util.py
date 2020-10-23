# -*- coding: utf-8 -*-
"""
Functional specification for the xact.cfg.util module.

"""


# =============================================================================
class SpecifyApply:
    """
    Spec for the xact.cfg.util.apply() function.

    """

    # -------------------------------------------------------------------------
    def it_modifies_nested_struct_field_values_by_path(self):
        """
        The apply function can be used to modify nested struct fields.

        """
        import xact.cfg.util

        data = {'a': {'b': 1}}
        data = xact.cfg.util.apply(data, 'a.b', 2)
        assert data['a']['b'] == 2

    # -------------------------------------------------------------------------
    def it_can_use_different_delimiters(self):
        """
        The apply function can be used to modify nested struct fields.

        """
        import xact.cfg.util

        data = {'a': {'b': 1}}
        data = xact.cfg.util.apply(data, 'a:b', 2, delim_cfg_addr=':')
        assert data['a']['b'] == 2
