# -*- coding: utf-8 -*-
"""
Package with functionality to provision and deploy applications to remote hosts.

"""


import paramiko
import xact.sys.orchestration.ansible


#------------------------------------------------------------------------------
def ensure_ready_to_run(cfg):
    """
    Ensure that hosts are ready to run their allocated functionality.

    """
    ensure_provisioned(cfg)
    ensure_deployed(cfg)


#------------------------------------------------------------------------------
def ensure_provisioned(cfg):
    """
    Ensure that hosts are provisioned for the components that they will run.

    """
    return xact.sys.orchestration.ansible.ensure_provisioned(cfg)


#------------------------------------------------------------------------------
def ensure_deployed(cfg):
    """
    Ensure that application components are deployed to hosts as per cfg.

    """
    return xact.sys.orchestration.ansible.ensure_deployed(cfg)

