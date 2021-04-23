# -*- coding: utf-8 -*-
"""
Functional specification for xact.

"""


import click.testing
import pytest


# -----------------------------------------------------------------------------
def count_to_ten(inputs, state, outputs):  # pylint: disable=W0613
    """
    Step function for a simple test node that counts to ten.

    """
    if 'count' not in state:
        state['count'] = 0
    else:
        state['count'] += 1
    outputs['output']['count'] = state['count']
    if state['count'] >= 10:
        import xact.signal  # pylint: disable=C0415
        return xact.signal.Halt(0)


# -----------------------------------------------------------------------------
def message_on_ten(inputs, state, outputs):  # pylint: disable=W0613
    """
    Step function for a test node that prints a message after ten steps.

    """
    import xact.test.util
    if inputs['input']['count'] >= 10:
        xact.test.util.send(message = 'TEST OK',
                            port    = xact.test.util.TEST_PORT)
        import xact.signal  # pylint: disable=C0415
        raise xact.signal.Halt(0)


# -----------------------------------------------------------------------------
def coro_count_to_ten(runtime, cfg, inputs, state, outputs):  # pylint: disable=W0613
    """
    Coroutine for a simple test node that counts to ten.

    """
    import xact.signal

    count  = -1
    signal = None

    while True:
        inputs = yield (outputs, signal)

        count += 1
        outputs['output']['count'] = count
        if count >= 10:
            signal = xact.signal.Halt(0)


# -----------------------------------------------------------------------------
def coro_message_on_ten(runtime, cfg, inputs, state, outputs):  # pylint: disable=W0613
    """
    Step function for a test node that prints a message after ten steps.

    """
    import xact.signal
    import xact.test.util

    signal = None

    while True:
        inputs = yield (outputs, signal)

        if inputs['input']['count'] >= 10:
            xact.test.util.send(message = 'TEST OK',
                                port    = xact.test.util.TEST_PORT)
            signal = xact.signal.Halt(0)


# =============================================================================
class SpecifyXact:
    """
    Spec for xact at a system level.

    """

    # -------------------------------------------------------------------------
    def it_runs_on_a_single_process(self):
        """
        xact.cli.command.grp_main halts_two_processes.

        """
        import xact.test.util
        xact.test.util.run(
                env = xact.test.util.env(filepath = __file__),
                cfg = xact.test.util.simple_pipeline(
                                repr   = 'py_dill',
                                iface  = 'step',
                                proc_a = {'node_a': count_to_ten,
                                          'node_b': message_on_ten}),
                expected_output = {xact.test.util.TEST_PORT: 'TEST OK'},
                is_local        = True)

    # -------------------------------------------------------------------------
    def it_runs_across_multiple_processes(self):
        """
        xact.cli.command.grp_main halts_two_processes.

        """
        import xact.test.util
        xact.test.util.run(
                env = xact.test.util.env(filepath = __file__),
                cfg = xact.test.util.simple_pipeline(
                                repr   = 'py_dill',
                                iface  = 'step',
                                proc_a = {'node_a': count_to_ten},
                                proc_b = {'node_b': message_on_ten}),
                expected_output = {xact.test.util.TEST_PORT: 'TEST OK'},
                is_local        = False)


    # -------------------------------------------------------------------------
    def it_runs_functions_specified_as_source_strings(self):
        """
        xact.cli.command.grp_main halts_two_processes.

        """
        import xact.test.util
        xact.test.util.run(
                env = xact.test.util.env(filepath = __file__),
                cfg = xact.test.util.simple_pipeline(
                                repr   = 'py_src',
                                iface  = 'step',
                                proc_a = {'node_a': count_to_ten,
                                          'node_b': message_on_ten}),
                expected_output = {xact.test.util.TEST_PORT: 'TEST OK'},
                is_local        = True)

    # -------------------------------------------------------------------------
    def it_runs_coroutines(self):
        """
        xact.cli.command.grp_main halts_two_processes.

        """
        import xact.test.util
        xact.test.util.run(
                env = xact.test.util.env(filepath = __file__),
                cfg = xact.test.util.simple_pipeline(
                                repr   = 'py_dill',
                                iface  = 'coro',
                                proc_a = {'node_a': coro_count_to_ten,
                                          'node_b': coro_message_on_ten}),
                expected_output = {xact.test.util.TEST_PORT: 'TEST OK'},
                is_local        = True)


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
        import xact.cli.command  # pylint: disable=C0415

        runner        = click.testing.CliRunner()
        response      = runner.invoke(xact.cli.command.grp_main)
        response_text = ' '.join(line.strip() for line in
                                        response.output.splitlines())
        expected_text = ' '.join(line.strip() for line in
                                        expected_help_text_main.splitlines())

        assert response.exit_code == 0
        assert response_text.startswith(expected_text)

    # -------------------------------------------------------------------------
    def it_displays_help_text_when_called_with_help_arg(
                                                self, expected_help_text_main):
        """
        xact.cli.command.grp_main prints help text when called with a help arg.

        """
        import xact.cli.command  # pylint: disable=C0415

        runner        = click.testing.CliRunner()
        response      = runner.invoke(xact.cli.command.grp_main, ['--help'])
        response_text = ' '.join(line.strip() for line in
                                        response.output.splitlines())
        expected_text = ' '.join(line.strip() for line in
                                        expected_help_text_main.splitlines())

        assert response.exit_code == 0
        assert response_text.startswith(expected_text)


# -----------------------------------------------------------------------------
@pytest.fixture
def expected_help_text_main():
    """
    Return the expected help text for xact.cli.command.grp_main.

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
