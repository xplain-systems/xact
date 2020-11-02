# -*- coding: utf-8 -*-
"""
Functional specification for xact.

"""


import os
import sys

import click.testing
import pytest


# =============================================================================
class SpecifyXact:
    """
    Spec for xact at a system level.

    """
    def it_runs_a_simple_system(self, simple_test_config):
        """
        xact.cli.command.grp_main runs a simple xact system.

        """
        import xact.cli.command
        import xact.util.serialization

        cfg    = simple_test_config
        runner = click.testing.CliRunner()
        args   = ['system', 'start', '--no-distribute',
                    '--cfg', xact.util.serialization.serialize(cfg)]
        response = runner.invoke(xact.cli.command.grp_main, args)
        assert response.output == 'RUN COMPLETED SUCCESSFULLY\n'
        assert response.exit_code == 0


# -----------------------------------------------------------------------------
@pytest.fixture
def expected_help_text_main():
    """
    Expected help text for xact.cli.command.grp_main

    """
    return (
        'Usage: main [OPTIONS] COMMAND [ARGS]...\n\n'
        '  Xact command line interface.\n\n'
        '  The xact command line interface provides the\n'
        '  user with the ability to start, stop, pause\n'
        '  and step an xact system.\n\n'
        '  An xact system is composed of one or more\n'
        '  process-hosts, each of which contains one or\n'
        '  more processes, each of which contains one or\n'
        '  more compute nodes.\n\n'
        'Options:\n'
        '  --help  Show this message and exit.\n\n'
        'Commands:\n'
    )


# =============================================================================
class SpecifyGrpMain:
    """
    Spec for the xact.cli.command.grp_main command group.

    """

    # -------------------------------------------------------------------------
    def it_displays_help_text_when_called_with_no_args(
                                                self, expected_help_text_main):
        """
        xact.cli.command.grp_main prints help text when called with no args.

        """
        import textwrap

        import click.testing

        import xact.cli.command

        runner        = click.testing.CliRunner()
        response      = runner.invoke(xact.cli.command.grp_main)
        response_text = ' '.join(line.strip() for line in
                                        response.output.splitlines())
        expected_text = ' '.join(line.strip() for  line in
                                        expected_help_text_main.splitlines())

        assert response.exit_code == 0
        assert response_text.startswith(expected_text)

    # -------------------------------------------------------------------------
    def it_displays_help_text_when_called_with_help_arg(
                                                self, expected_help_text_main):
        """
        xact.cli.command.grp_main prints help text when called with a help arg.

        """
        import textwrap

        import click.testing

        import xact.cli.command

        runner        = click.testing.CliRunner()
        response      = runner.invoke(xact.cli.command.grp_main, ['--help'])
        response_text = ' '.join(line.strip() for line in
                                        response.output.splitlines())
        expected_text = ' '.join(line.strip() for  line in
                                        expected_help_text_main.splitlines())

        assert response.exit_code == 0
        assert response_text.startswith(expected_text)
