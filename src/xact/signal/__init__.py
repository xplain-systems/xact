# -*- coding: utf-8 -*-
"""
Package of classes representing various xact control signals.

"""


# =============================================================================
class ControlSignal(Exception):
    """
    Base class for custom exceptions used for controlling xact.

    """


# =============================================================================
class NonRecoverableError(ControlSignal):
    """
    Thrown to trigger an immediate shutdown in an unplanned erroneous condition.

    This signal is used to initiate an immediate
    shutdown of the current process when in an
    exceptional and unplanned condition of
    operation.

    This sort of condition is often referred to as
    a 'FatalError' in other systems.

    """

    # -------------------------------------------------------------------------
    def __init__(self, cause):
        """
        Return an instance of a NonRecoverableError ControlSignal object.

        """
        self.cause = cause
        super().__init__()



# =============================================================================
class Halt(ControlSignal):
    """
    Thrown to trigger graceful process shutdown when the run is finished.

    This signal is used to initiate a clean and
    systematic shutdown of the current process
    when in an expected and planned condition of
    operation.

    """

    # -------------------------------------------------------------------------
    def __init__(self, return_code):
        """
        Return an instance of a Halt ControlSignal object.

        """
        self.return_code = return_code
        super().__init__()


# =============================================================================
class ResetAndRetry(ControlSignal):
    """
    Thrown to trigger a reset-and-retry on the current process.

    This can be used for contolled recovery from
    an error or exception.

    This sort of condition is often referred to as
    a 'NonFatalError' in other systems.

    """

    pass
