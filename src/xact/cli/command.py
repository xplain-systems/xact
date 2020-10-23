# -*- coding: utf-8 -*-
"""
Module of commands and command groups for the xact command line interface.

The command line interface consists of a number
of commands, organized into command groups. This
module contains definitions for all of the
command groups and commands used by xact.

Argument parsing and the display of command line
help messages is implemented with the help of the
python click library.

    https://click.palletsprojects.com/en/7.x/

Our cli module provides a thin wrapper around the
click library as well as some light customization.

Note that the xact.cli.util.OrderedGroup command
class uses the order in which command functions
are defined  in this file to  determine the order
in which the corresponding help text appears.
Please be aware of this when adding, removing or
re-ordering commands.

Finally, please note that since the click library
supports running multiple commands in a single
invocation, it makes the design decision to throw
away the return value from each command function
rather than  propagating it back to the command
line return value. For this reason, we make an
explicit call  to sys.exit() inside each command
function so that we can terminate execution and
return to the command line with an appropriate
return code.


"""


import sys

import click

import xact.cli.util
import xact.log

_set_envvar = set()

xact.log.setup()


#------------------------------------------------------------------------------
def _envvar(name):
    """
    Return the specified name with the common envvar prefix prepended.

    This function accepts as input a single string and returns as output the
    same string prepended with the common prefix that is used to identify
    environment variables of the xact system.

    For example, if the string 'FOO' was given as input, the string 'XACT_FOO'
    would be returned.

    A record is kept of all environment variables in the module level variable
    _set_envvar.

    """
    name_envvar = 'XACT_' + name
    _set_envvar.add(name_envvar)
    return name_envvar


#------------------------------------------------------------------------------
@click.group(name = 'main',
             cls  = xact.cli.util.OrderedGroup)
def grp_main():
    """
    Xact command line interface.

    The xact command line interface provides the user with the ability to
    start, stop, pause and step an xact system.

    An xact system is composed of one or more process-hosts, each of which
    contains one or more processes, each of which contains one or more compute
    nodes.

    """
    pass


#------------------------------------------------------------------------------
@grp_main.group(name = 'system',
                cls  = xact.cli.util.OrderedGroup)
def grp_system():
    """
    Commands to control the system as a whole.

    """
    pass


#------------------------------------------------------------------------------
@grp_main.group(name = 'host',
                cls  = xact.cli.util.OrderedGroup)
def grp_host():
    """
    Commands to control a single process host.

    """
    pass


#------------------------------------------------------------------------------
@grp_system.command()
@click.option(
    '-p', '--cfg-path', 'path_cfg',
    help     = 'File system path for configuration file(s).',
    required = False,
    default  = None,
    type     = click.Path(exists = True),
    nargs    = 1,
    envvar   = _envvar('CFG_PATH'))
@click.option(
    '-c', '--cfg', 'cfg',
    help     = 'Serialized configuration data.',
    required = False,
    default  = None,
    type     = click.STRING,
    nargs    = 1,
    envvar   = _envvar('CFG'))
@click.option(
    '--makeready/--no-makeready',
    help     = 'Provision and deploy to all hosts before running.',
    required = False,
    default  = False,
    envvar   = _envvar('MAKEREADY'))
@click.option(
    '--distribute/--no-distribute',
    help     = 'Distribute compute nodes across processes and hosts.',
    required = False,
    default  = True,
    envvar   = _envvar('DISTRIBUTE'))
@click.option(
    '-s', '--cfg-addr-delim', 'delim_cfg_addr',
    help     = 'The character to use as a delimiter in config override addresses.',
    required = False,
    default  = '.',
    type     = click.STRING,
    nargs    = 1,
    envvar   = _envvar('CFG_ADDR_DELIM'))
@click.argument(
    'cfg_override',
    required = False,
    default  = None,
    type     = click.STRING,
    nargs    = -1,
    envvar   = _envvar('CFG_OVERRIDE'))
def start(path_cfg       = None,
          cfg            = None,
          makeready      = False,
          distribute     = True,
          delim_cfg_addr = '.',
          cfg_override   = None):
    """
    Start the specified system.

    This command exposes options and arguments which are useful during
    development and test.

    In particular, CFG_OVERRIDE consists of a sequence of alternating address
    value pairs so that any number of configuration items may be overridden
    from the command line.

    > xact dev start first:key first_value second:key second_value

    """
    import xact.cfg
    import xact.cfg.exception
    import xact.sys

    with xact.log.logger.catch(onerror = lambda _: sys.exit(1)):
        try:
            cfg = xact.cfg.prepare(path_cfg       = path_cfg,
                                   string_cfg     = cfg,
                                   do_make_ready  = makeready,
                                   is_distributed = distribute,
                                   delim_cfg_addr = delim_cfg_addr,
                                   tup_overrides  = cfg_override)
        except xact.cfg.exception.CfgError as err:
            print(err, file = sys.stderr)  # Custom message (no stack trace)
            sys.exit(1)
        else:
            try:
                sys.exit(xact.sys.start(cfg))
            except xact.sys.exception.NonRecoverableError as err:

                # A NonRecoverableError exception
                # will be thrown when we need to
                # display either a custom error
                # message, or no error message
                # at all.
                #
                err_msg = str(err)
                if err_msg != '':
                    print(err_msg, file = sys.stderr)
                sys.exit(1)


#------------------------------------------------------------------------------
@grp_system.command()
@click.option(
    '-p', '--cfg-path', 'path_cfg',
    help     = 'Directory path for configuration files.',
    required = False,
    default  = None,
    type     = click.Path(exists = True),
    nargs    = 1,
    envvar   = _envvar('CFG_PATH'))
def stop(path_cfg = None):
    """
    Stop the specified system.

    """
    import xact.cfg
    import xact.sys
    sys.exit(
        xact.sys.stop(
            xact.cfg.prepare(path_cfg = path_cfg)))


#------------------------------------------------------------------------------
@grp_system.command()
@click.option(
    '-p', '--cfg-path', 'path_cfg',
    help     = 'Directory path for configuration files.',
    required = False,
    default  = None,
    type     = click.Path(exists = True),
    nargs    = 1,
    envvar   = _envvar('CFG_PATH'))
def pause(path_cfg = None):
    """
    Pause the specified system.

    """
    import xact.cfg
    import xact.sys
    sys.exit(
        xact.sys.pause(
            xact.cfg.prepare(path_cfg = path_cfg)))


#------------------------------------------------------------------------------
@grp_system.command()
@click.option(
    '-p', '--cfg-path', 'path_cfg',
    help     = 'Directory path for configuration files.',
    required = False,
    default  = None,
    type     = click.Path(exists = True),
    nargs    = 1,
    envvar   = _envvar('CFG_PATH'))
def step(path_cfg = None):
    """
    Single step the specified system.

    """
    import xact.cfg
    import xact.sys
    sys.exit(
        xact.sys.step(
            xact.cfg.prepare(path_cfg = path_cfg)))


#------------------------------------------------------------------------------
@grp_host.command()
@click.argument(
    'cfg',
    required = True,
    type     = click.STRING,
    nargs    = 1,
    envvar   = _envvar('CFG'))
def start_host(cfg = None):
    """
    Start the specified process host.

    This command takes a single argument, CFG, which is expected to be a
    serialized configuration structure.

    """
    import xact.util.serialization
    import xact.host
    sys.exit(
        xact.host.start(
            xact.util.serialization.deserialize(cfg)))


#------------------------------------------------------------------------------
@grp_host.command()
@click.argument(
    'cfg',
    required = True,
    type     = click.STRING,
    nargs    = 1,
    envvar   = _envvar('CFG'))
def stop_host(cfg = None):
    """
    Stop the specified process host.

    This command takes a single argument, CFG, which is expected to be a
    serialized configuration structure.

    """
    import xact.util.serialization
    import xact.host
    sys.exit(
        xact.host.stop(
            xact.util.serialization.deserialize(cfg)))


#------------------------------------------------------------------------------
@grp_host.command()
@click.argument(
    'cfg',
    required = True,
    type     = click.STRING,
    nargs    = 1,
    envvar   = _envvar('CFG'))
def pause_host(cfg = None):
    """
    Pause the specified process host.

    This command takes a single argument, CFG, which is expected to be a
    serialized configuration structure.

    """
    import xact.util.serialization
    import xact.host
    sys.exit(
        xact.host.pause(
            xact.util.serialization.deserialize(cfg)))


#------------------------------------------------------------------------------
@grp_host.command()
@click.argument(
    'cfg',
    required = True,
    type     = click.STRING,
    nargs    = 1,
    envvar   = _envvar('CFG'))
def step_host(cfg = None):
    """
    Single step the specified process host.

    This command takes a single argument, CFG, which is expected to be a
    serialized configuration structure.

    """
    import xact.util.serialization
    import xact.host
    sys.exit(
        xact.host.step(
            xact.util.serialization.deserialize(cfg)))
