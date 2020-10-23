# -*- coding: utf-8 -*-
"""
Functional specification for the xact.cfg.validate module.

"""


import pytest


# =============================================================================
class SpecifyNormalized:
    """
    Spec for the normalized validation function.

    """

    # -------------------------------------------------------------------------
    def it_accepts_valid_data(self, valid_normalized_config):
        """
        The normalized function does not throw when given valid data.

        """
        import xact.cfg.validate

        xact.cfg.validate.normalized(valid_normalized_config)

    # -------------------------------------------------------------------------
    def it_rejects_invalid_data(self, invalid_config):
        """
        The normalized function raises an exception when given invalid data.

        """
        import xact.cfg.exception
        import xact.cfg.validate

        with pytest.raises(xact.cfg.exception.CfgError):
            xact.cfg.validate.normalized(invalid_config)


# =============================================================================
class SpecifyDenormalized:
    """
    Spec for the denormalized validation function.

    """

    # -------------------------------------------------------------------------
    def it_accepts_valid_data(self, valid_normalized_config):
        """
        The normalized function does not throw when given valid data.

        """
        import xact.cfg
        import xact.cfg.validate

        valid_denormalized_config = xact.cfg.denormalize(valid_normalized_config)
        xact.cfg.validate.denormalized(valid_denormalized_config)

    # -------------------------------------------------------------------------
    def it_rejects_invalid_data(self, invalid_config):
        """
        The normalized function raises an exception when given invalid data.

        """
        import xact.cfg.exception
        import xact.cfg.validate

        with pytest.raises(xact.cfg.exception.CfgError):
            xact.cfg.validate.denormalized(invalid_config)
