# -*- coding: utf-8 -*-
"""
Functional specification for the xact.cli.util module.

"""


import pytest


# =============================================================================
class SpecifyOrderedGroup_listCommands:
    """
    Spec for the OrderedGroup.list_commands method.

    """

    # -------------------------------------------------------------------------
    def it_returns_a_string_prefixed_with_xact(self):
        """
        OrderedGroup.list_commands returns commands in order.

        """
        import click

        import xact.cli.util

        tup_names = ('C1', 'C2', 'C3')
        group = xact.cli.util.OrderedGroup()

        for name in tup_names:
            group.add_command(click.Command(name=name))
        outputs = group.list_commands(ctx=None)

        for (input_name, output_name) in zip(tup_names, outputs):
            assert input_name == output_name
