# -*- coding: utf-8 -*-
"""
Module of utility functions related to the xact command line interface.

"""


import click
import click.testing
import pytest

import xact.cfg
import xact.cfg.validate
import xact.cli.command


#==============================================================================
class OrderedGroup(click.Group):
    """
    A click command group enabling control over the ordering of commands.

    The help text for a click command group normally
    lists each command in arbitrary order.

    This subclass ensures that the help text for
    the corresponding click command group will
    appear in the same order in which they were
    added. I.e. the same order that they are
    defined in the source file.

    """

    #--------------------------------------------------------------------------
    def __init__(self, name = None, commands = None, **attrs):
        """
        Return an OrderedGroup instance.

        """
        super(OrderedGroup, self).__init__(name, commands, **attrs)

        # From Python 3.5 dict() is order preserving, so
        # we simply replace the default commands container
        # (a set) with a normal python dict.
        #
        self.commands = commands or dict()

    #--------------------------------------------------------------------------
    def list_commands(self, ctx):
        """
        Return an ordered list of the commands in the group.

        """
        return self.commands


#------------------------------------------------------------------------------
def run_test(cfg,
             expected_exit_code    = 0,
             do_expect_stdout      = False,
             do_expect_stderr      = False,
             expected_stdout_value = None,
             expected_stderr_value = None):
    """
    Run a test via the command line interface.

    """
    runner   = click.testing.CliRunner(mix_stderr = False)
    response = runner.invoke(xact.cli.command.grp_main,
                             ['system', 'start', '--no-distribute',
                              '--cfg', xact.util.serialization.serialize(cfg)])

    if not _isok(response, expected_exit_code,
                 do_expect_stdout, do_expect_stderr,
                 expected_stdout_value, expected_stderr_value):

        msg = ('exit_code:\n{exit_code}\n\n'
               'stdout:\n{stdout}\n\n'
               'stderr:\n{stderr}\n\n').format(
                                            exit_code = response.exit_code,
                                            stdout    = response.stdout,
                                            stderr    = response.stderr)
        pytest.fail(msg = msg, pytrace = False)


#------------------------------------------------------------------------------
def _isok(response,
          expected_exit_code    = 0,
          do_expect_stdout      = False,
          do_expect_stderr      = False,
          expected_stdout_value = None,
          expected_stderr_value = None):
    """
    Return true iff response is OK.

    """
    if response.exit_code != expected_exit_code:
        return False

    has_stdout = response.stdout != ''
    has_stderr = response.stderr != ''

    if has_stdout != do_expect_stdout:
        return False
    if has_stderr != do_expect_stderr:
        return False

    do_check_stdout_value = has_stdout and (expected_stdout_value is not None)
    do_check_stderr_value = has_stderr and (expected_stderr_value is not None)

    if do_check_stdout_value and (response.stdout != expected_stdout_value):
        return False
    if do_check_stderr_value and (response.stderr != expected_stderr_value):
        return False

    return True
