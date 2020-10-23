# -*- coding: utf-8 -*-
"""
Functional specification for the xact.cfg.atomic_types module.

"""


# =============================================================================
class SpecifyAsDict:
    """
    Spec for the xact.cfg.data.atomic_types.as_dict function.

    """

    # -------------------------------------------------------------------------
    def it_returns_a_dict(self):
        """
        The as_dict function returns a dict

        """
        import xact.cfg.data.atomic_types
        lookup_table = xact.cfg.data.atomic_types.as_dict()
        assert isinstance(lookup_table, dict)


# =============================================================================
class Specify_AsTuple:
    """
    Spec for the _as_tuple function.

    """

    # -------------------------------------------------------------------------
    def it_returns_a_tuple(self):
        """
        The as_dict function returns a dict

        """
        import xact.cfg.data.atomic_types
        lookup_table = xact.cfg.data.atomic_types._as_tuple()
        assert isinstance(lookup_table, tuple)