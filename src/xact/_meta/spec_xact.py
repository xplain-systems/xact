# -*- coding: utf-8 -*-
"""
Functional specification for xact.

"""


import click.testing
import pytest


# =============================================================================
class SpecifyXact:
    """
    Spec for xact at a system level.

    """

    # -------------------------------------------------------------------------
    def it_supports_single_process_execution(self):
        """
        xact.cli.command.grp_main supports single process execution.

        """
        import xact.test.util
        xact.test.util.run(
                env = xact.test.util.env(filepath = __file__),
                cfg = xact.test.util.simple_pipeline(
                            repr           = 'py_dill',
                            iface          = 'step',
                            is_closed_loop = False,
                            proc_a         = {'node_a': simple_counter,
                                              'node_b': simple_messager}),
                expected_output = {xact.test.util.TEST_PORT: 'TEST OK'},
                is_local        = True)

    # -------------------------------------------------------------------------
    def it_supports_multiprocess_execution(self):
        """
        xact.cli.command.grp_main supports multiprocess execution.

        """
        import xact.test.util
        xact.test.util.run(
                env = xact.test.util.env(filepath = __file__),
                cfg = xact.test.util.simple_pipeline(
                            repr           = 'py_dill',
                            iface          = 'step',
                            is_closed_loop = False,
                            proc_a         = {'node_a': simple_counter},
                            proc_b         = {'node_b': simple_messager}),
                expected_output = {xact.test.util.TEST_PORT: 'TEST OK'},
                is_local        = False)


    # -------------------------------------------------------------------------
    def it_supports_functions_specified_as_source_strings(self):
        """
        xact.cli.command.grp_main supports nodes specified as source strings.

        """
        import xact.test.util
        xact.test.util.run(
                env = xact.test.util.env(filepath = __file__),
                cfg = xact.test.util.simple_pipeline(
                            repr           = 'py_src',
                            iface          = 'step',
                            is_closed_loop = False,
                            proc_a         = {'node_a': simple_counter,
                                              'node_b': simple_messager}),
                expected_output = {xact.test.util.TEST_PORT: 'TEST OK'},
                is_local        = True)

    # -------------------------------------------------------------------------
    def it_supports_coroutines(self):
        """
        xact.cli.command.grp_main supports nodes specified as coroutines.

        """
        import xact.test.util
        xact.test.util.run(
                env = xact.test.util.env(filepath = __file__),
                cfg = xact.test.util.simple_pipeline(
                            repr           = 'py_dill',
                            iface          = 'coro',
                            is_closed_loop = False,
                            proc_a         = {'node_a': coro_counter,
                                              'node_b': coro_messager}),
                expected_output = {xact.test.util.TEST_PORT: 'TEST OK'},
                is_local        = True)


    # -------------------------------------------------------------------------
    def it_supports_feedback_loops(self):
        """
        xact.cli.command.grp_main supports nodes arranged in a feedback loop.

        """
        import xact.test.util
        xact.test.util.run(
                env = xact.test.util.env(filepath = __file__),
                cfg = xact.test.util.simple_pipeline(
                            repr           = 'py_dill',
                            iface          = 'coro',
                            is_closed_loop = True,
                            proc_a         = {'node_a': feedback_counter,
                                              'node_b': feedback_decisioner}),
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


# -----------------------------------------------------------------------------
def simple_counter(inputs, state, outputs):  # pylint: disable=W0613
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
def simple_messager(inputs, state, outputs):  # pylint: disable=W0613
    """
    Step function for a test node that prints a message after ten steps.

    """
    import xact.test.util
    if inputs['input']['count'] >= 10:
        xact.test.util.send(message = 'TEST OK')
        import xact.signal  # pylint: disable=C0415
        raise xact.signal.Halt(0)


# -----------------------------------------------------------------------------
def coro_counter(runtime, cfg, inputs, state, outputs):  # pylint: disable=W0613
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
def coro_messager(runtime, cfg, inputs, state, outputs):  # pylint: disable=W0613
    """
    Coroutine for a test node that prints a message after ten steps.

    """
    import xact.signal
    import xact.test.util

    signal = None

    while True:
        inputs = yield (outputs, signal)

        if inputs['input']['count'] >= 10:
            xact.test.util.send(message = 'TEST OK')
            signal = xact.signal.Halt(0)


# -----------------------------------------------------------------------------
def feedback_counter(runtime, cfg, inputs, state, outputs):  # pylint: disable=W0613
    """
    Coroutine for a simple test node that counts to ten.

    """
    count = 0
    while True:
        outputs['output']['count'] = count
        inputs = yield (outputs, None)
        count += 1

        if inputs['input']['do_halt']:
            import xact.test.util
            xact.test.util.send(message = 'TEST OK')
            import xact.signal
            yield (outputs, xact.signal.Halt(0))


# -----------------------------------------------------------------------------
def feedback_decisioner(runtime, cfg, inputs, state, outputs):  # pylint: disable=W0613
    """
    Coroutine for a test node that decides to halt after ten steps.

    """
    outputs['output']['do_halt'] = False
    signal = None
    while True:
        inputs = yield (outputs, signal)
        if inputs['input']['count'] >= 10:
            outputs['output']['do_halt'] = True
