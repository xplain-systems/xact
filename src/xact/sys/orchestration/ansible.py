# -*- coding: utf-8 -*-
"""
Module containing Ansible integration logic.

"""


import shutil
import subprocess
import tempfile
import json

import ansible.constants
import ansible.context
import ansible.executor.task_queue_manager
import ansible.inventory.manager
import ansible.module_utils.common.collections
import ansible.parsing.dataloader
import ansible.plugins.callback
import ansible.vars.manager
import yaml


# -----------------------------------------------------------------------------
def ensure_deployed(cfg):
    """
    Ensure that application components are deployed to hosts as per cfg.

    """


# -----------------------------------------------------------------------------
def ensure_provisioned(cfg):
    """
    Ensure that hosts are provisioned for the components that they will run.

    """
    list_plays = []
    list_hosts = []
    for id_host in cfg['host'].keys():
        if id_host == 'localhost':
            continue
        cfg_host = cfg['host'][id_host]
        list_hosts.append(cfg_host['hostname'])
        list_plays.append({
            'hosts':       cfg_host['hostname'],
            'remote_user': cfg_host['acct_provision'],
            'tasks':       _list_tasks(cfg, id_host)
        })

    with tempfile.NamedTemporaryFile(mode = 'wt') as file_hosts:

        file_hosts.write('\n'.join(list_hosts))
        file_hosts.flush()

        with tempfile.NamedTemporaryFile(mode = 'wt') as file_play:

            file_play.write(yaml.dump(list_plays))
            file_play.flush()

            subprocess.run(
                    ['ansible-playbook', '-i',
                     file_hosts.name, file_play.name],
                    check = False)


# -----------------------------------------------------------------------------
def _list_tasks(cfg, id_host):
    """
    Return the list of tasks for the specified host.

    """
    for key in ('req_host_cfg', 'role'):
        if key not in cfg:
            return []

    set_id_proc_on_host   = _processes_on_host(cfg, id_host)
    set_id_cfg_for_host   = _config_required_by_host(cfg, set_id_proc_on_host)
    list_id_role_for_host = _roles_for_host(cfg, set_id_cfg_for_host)
    list_tasks_for_host   = _tasks_for_host(cfg, list_id_role_for_host)
    return list_tasks_for_host


# -----------------------------------------------------------------------------
def _tasks_for_host(cfg, list_id_role_for_host):
    """
    Return a list of the tasks required to configure the specified host.

    """
    list_tasks_for_host = []
    for id_role in list_id_role_for_host:
        list_tasks_for_host.extend(cfg['role'][id_role]['tasks'])
    return list_tasks_for_host


# -----------------------------------------------------------------------------
def _processes_on_host(cfg, id_host):
    """
    Return a set of the process ids found on the specified host.

    """
    set_id_proc_on_host = set()
    for (id_proc, cfg_proc) in cfg['process'].items():
        if cfg_proc['host'] == id_host:
            set_id_proc_on_host.add(id_proc)
    return set_id_proc_on_host


# -----------------------------------------------------------------------------
def _config_required_by_host(cfg, set_id_proc_on_host):
    """
    Return a set of the config ids required by the specified host.

    """
    set_id_cfg_for_host = set()
    for cfg_node in cfg['node'].values():
        if cfg_node['process'] in set_id_proc_on_host:
            set_id_cfg_for_host.add(cfg_node['req_host_cfg'])
    return set_id_cfg_for_host


# -----------------------------------------------------------------------------
def _roles_for_host(cfg, set_id_cfg_for_host):
    """
    Return a list of role ids for the current host.

    """
    list_id_role_for_host = []
    for id_config_for_host in set_id_cfg_for_host:
        cfg_for_host_part = cfg['req_host_cfg'][id_config_for_host]
        if 'role' in cfg_for_host_part:
            list_id_role_for_host.extend(cfg_for_host_part['role'])
    return list_id_role_for_host


# -----------------------------------------------------------------------------
def _call_ansible(hostname, list_tasks):
    """
    Call ansible for a specific host.

    """
    list_module_path = []  # ['/to/mymodules', '/usr/share/ansible']
    ImmutableDict    = ansible.module_utils.common.collections.ImmutableDict
    ansible.context.CLIARGS = ImmutableDict(
                        connection    = 'smart',
                        module_path   = list_module_path,
                        forks         = 10,
                        become        = 'xact',
                        become_method = None,
                        become_user   = None,
                        check         = False,
                        diff          = False)

    list_hosts = [hostname]
    sources    = ','.join(list_hosts)
    if len(list_hosts) == 1:
        sources += ','

    loader           = ansible.parsing.dataloader.DataLoader()
    passwords        = dict(vault_pass = 'secret')
    results_callback = ResultsCollectorJSONCallback()
    inventory        = ansible.inventory.manager.InventoryManager(
                                                    loader  = loader,
                                                    sources = sources)
    variable_manager = ansible.vars.manager.VariableManager(
                                                    loader    = loader,
                                                    inventory = inventory)

    tqm = ansible.executor.task_queue_manager.TaskQueueManager(
                                        inventory        = inventory,
                                        variable_manager = variable_manager,
                                        loader           = loader,
                                        passwords        = passwords,
                                        stdout_callback  = results_callback)

    play_source = dict(
        name         = "Ansible Play",
        hosts        = list_hosts,
        gather_facts = 'no',
        tasks        = [
            {
                'action': {
                    'module': 'shell',
                    'args':   'ls'
                },
                'register': 'shell_out'
            }
        ]
    )

    play = ansible.playbook.play.Play().load(
                                        play_source,
                                        variable_manager = variable_manager,
                                        loader           = loader)

    try:
        result = tqm.run(play)
    finally:
        tqm.cleanup()
        if loader:
            loader.cleanup_all_tmp_files()

    shutil.rmtree(
        ansible.constants.DEFAULT_LOCAL_TMP, True)   # pylint: disable=E1101

    print("UP ***********")
    for host, result in results_callback.host_ok.items():
        print('{0} >>> {1}'.format(
                host, result._result['stdout']))  # pylint: disable=W0212

    print("FAILED *******")
    for host, result in results_callback.host_failed.items():
        print('{0} >>> {1}'.format(
                host, result._result['msg']))  # pylint: disable=W0212

    print("DOWN *********")
    for host, result in results_callback.host_unreachable.items():
        print('{0} >>> {1}'.format(
                host, result._result['msg']))  # pylint: disable=W0212


# =============================================================================
class ResultsCollectorJSONCallback(ansible.plugins.callback.CallbackBase):
    """
    A sample callback plugin used for performing an action as results come in.

    """

    # If you want to collect all results
    # into a single object for processing
    # at the end of the execution, look
    # into utilizing the ``json`` callback
    # plugin or writing your own custom
    # callback plugin.

    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        Return an instance of a ResultsCollectorJSONCallback object.

        """
        super().__init__(*args, **kwargs)
        self.host_ok = {}
        self.host_unreachable = {}
        self.host_failed = {}

    # -------------------------------------------------------------------------
    def v2_runner_on_unreachable(self, result):
        """
        Handle on_unreachable events.

        """
        host = result._host  # pylint: disable=W0212
        self.host_unreachable[host.get_name()] = result

    # -------------------------------------------------------------------------
    def v2_runner_on_ok(self, result, *args, **kwargs):
        """
        Print a json representation of the result.

        Also, store the result in an instance attribute for retrieval later

        """
        host = result._host  # pylint: disable=W0212
        self.host_ok[host.get_name()] = result
        print(json.dumps({host.name: result._result},  # pylint: disable=W0212
                         indent=4))

    # -------------------------------------------------------------------------
    def v2_runner_on_failed(self, result, ignore_errors = False):
        """
        Handle on_failed events.

        """
        host = result._host  # pylint: disable=W0212
        self.host_failed[host.get_name()] = result
