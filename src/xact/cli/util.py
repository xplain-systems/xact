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


# =============================================================================
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

    # -------------------------------------------------------------------------
    def __init__(self, name = None, commands = None, **attrs):
        """
        Return an OrderedGroup instance.

        """
        super().__init__(name, commands, **attrs)

        # From Python 3.5 dict() is order preserving, so
        # we simply replace the default commands container
        # (a set) with a normal python dict.
        #
        self.commands = commands or dict()

    # -------------------------------------------------------------------------
    def list_commands(self, ctx):
        """
        Return an ordered list of the commands in the group.

        """
        return self.commands


# -----------------------------------------------------------------------------
def run_test(cfg,  # pylint: disable=R0913
             expected_exit_code = 0,
             do_expect_stdout   = False,
             do_expect_stderr   = False,
             expected_stdout    = None,
             expected_stderr    = None):
    """
    Run a test via the command line interface.

    """
    runner   = click.testing.CliRunner(mix_stderr = False)
    response = runner.invoke(xact.cli.command.grp_main,
                             ['system', 'start', '--local',
                              '--cfg', xact.util.serialization.serialize(cfg)])

    isok_exit_code = response.exit_code == expected_exit_code
    isok_stdout    = _isok(response.stdout, do_expect_stdout, expected_stdout)
    isok_stderr    = _isok(response.stderr, do_expect_stderr, expected_stderr)

    isok = (isok_exit_code and isok_stdout and isok_stderr)

    if isok:
        return

    msg = ''
    if not isok_exit_code:
        msg += 'exit_code:\n{exit_code}\n\n'.format(
                                                exit_code = response.exit_code)

    if not isok_stdout:
        msg += 'stdout:\n{stdout}\n\n'.format(stdout = response.stdout)

    if not isok_stderr:
        msg += 'stderr:\n{stderr}\n\n'.format(stderr = response.stderr)

    pytest.fail(msg = msg, pytrace = False)


# -----------------------------------------------------------------------------
def _isok(response_output, do_expect_output, expected_output):
    """
    Return true iff response is OK.

    """
    has_output = response_output != ''
    if has_output != do_expect_output:
        return False

    if expected_output is not None:
        if response_output != expected_output:
            return False

    return True
