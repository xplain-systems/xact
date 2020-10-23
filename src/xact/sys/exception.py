# -*- coding: utf-8 -*-
"""
Logic for exceptions in the xact system.

"""


#==============================================================================
class ControlException(Exception):
    """
    Base class for custom exceptions used for xact system flow control.

    """
    pass

#==============================================================================
class RunComplete(ControlException):
    """
    Thrown to trigger graceful system shutdown when the run is finished.

    """
    
    #--------------------------------------------------------------------------
    def __init__(self, return_code):
        """
        """
        self.return_code = return_code

#==============================================================================
class RecoverableError(ControlException):
    """
    Thrown to trigger a reset-and-retry.

    """
    pass

#==============================================================================
class NonRecoverableError(ControlException):
    """
    Thrown to trigger an error condition system halt.

    """
    pass


