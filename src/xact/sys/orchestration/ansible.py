# -*- coding: utf-8 -*-
"""
Module containing Ansible integration logic.

"""


import shutil
import subprocess
import sys
import tempfile

import ansible.constants
import ansible.context
import ansible.executor.task_queue_manager
import ansible.inventory.manager
import ansible.module_utils.common.collections
import ansible.parsing.dataloader
import ansible.plugins.callback
import ansible.vars.manager
import paramiko
import yaml

import xact.cfg
import xact.host


#------------------------------------------------------------------------------
def ensure_deployed(cfg):
    """
    Ensure that application components are deployed to hosts as per cfg.

    """
    pass


#------------------------------------------------------------------------------
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

            proc = subprocess.run([
                        'ansible-playbook',
                        '-i', file_hosts.name,
                        file_play.name],
                        check = False)


#------------------------------------------------------------------------------
def _list_tasks(cfg, id_host):
    """
    Return the list of tasks for the specified host.

    """
    set_id_proc_on_host = set()
    for (id_proc, cfg_proc) in cfg['process'].items():
        if cfg_proc['host'] == id_host:
            set_id_proc_on_host.add(id_proc)

    set_id_required_config_for_host = set()
    for cfg_node in cfg['node'].values():
        if cfg_node['process'] in set_id_proc_on_host:
            set_id_required_config_for_host.add(cfg_node['req_host_cfg'])

    if 'req_host_cfg' not in cfg:
        return []

    if 'role' not in cfg:
        return []

    list_id_role_for_host = []
    for id_config_for_host in set_id_required_config_for_host:
        cfg_for_host_part = cfg['req_host_cfg'][id_config_for_host]
        if 'role' in cfg_for_host_part:
            list_id_role_for_host.extend(cfg_for_host_part['role'])

    list_tasks_for_host = []
    for id_role in list_id_role_for_host:
        list_tasks_for_host.extend(cfg['role'][id_role]['tasks'])

    return list_tasks_for_host


#------------------------------------------------------------------------------
def _call_ansible(hostname, list_tasks):
    """
    Call ansible for a specific host

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
        import pudb; pu.db
        result = tqm.run(play)
    finally:
        tqm.cleanup()
        if loader:
            loader.cleanup_all_tmp_files()

    shutil.rmtree(ansible.constants.DEFAULT_LOCAL_TMP, True)

    print("UP ***********")
    for host, result in results_callback.host_ok.items():
        print('{0} >>> {1}'.format(host, result._result['stdout']))

    print("FAILED *******")
    for host, result in results_callback.host_failed.items():
        print('{0} >>> {1}'.format(host, result._result['msg']))

    print("DOWN *********")
    for host, result in results_callback.host_unreachable.items():
        print('{0} >>> {1}'.format(host, result._result['msg']))


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

    #--------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        """
        super(ResultsCollectorJSONCallback, self).__init__(*args, **kwargs)
        self.host_ok = {}
        self.host_unreachable = {}
        self.host_failed = {}

    #--------------------------------------------------------------------------
    def v2_runner_on_unreachable(self, result):
        """
        """
        host = result._host
        self.host_unreachable[host.get_name()] = result

    #--------------------------------------------------------------------------
    def v2_runner_on_ok(self, result, *args, **kwargs):
        """
        Print a json representation of the result.

        Also, store the result in an instance attribute for retrieval later

        """
        host = result._host
        self.host_ok[host.get_name()] = result
        print(json.dumps({host.name: result._result}, indent=4))

    #--------------------------------------------------------------------------
    def v2_runner_on_failed(self, result, *args, **kwargs):
        """
        """
        host = result._host
        self.host_failed[host.get_name()] = result