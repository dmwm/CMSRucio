#! /usr/bin/env python3
from __future__ import print_function

import json
from collections import defaultdict

import requests
from rucio.client.client import Client
from rucio.common.exception import RSEAttributeNotFound, Duplicate

TO_STRIP = ['_Disk', '_Tape', '_Temp', '_Test', '_Disk_Test', '_Tape_Test', '_Ceph']

CRIC_USERS_API = 'https://cms-cric.cern.ch/api/accounts/user/query/list/?json&preset=roles'


def set_rse_manager(client, rse_name, site_managers, alt_rse=None):
    if not alt_rse:
        alt_rse = rse_name.lower()
    else:
        alt_rse = alt_rse.lower()
    try:
        client.delete_rse_attribute(rse=rse_name, key='rule_approvers')
        client.delete_rse_attribute(rse=rse_name, key='quota_approvers')
    except RSEAttributeNotFound:
        pass
    rule_approvers = ','.join(site_managers[alt_rse])
    print("Setting managers for %s to %s" % (rse_name, rule_approvers))

    client.add_rse_attribute(rse=rse_name, key='rule_approvers', value=rule_approvers)
    # For now, quota approvers are also rule approvers
    client.add_rse_attribute(rse=rse_name, key='quota_approvers', value=rule_approvers)

    # This is perhaps temporary
    for account in site_managers[alt_rse]:
        try:
            client.add_account_attribute(account=account, key='country-XX', value='admin')
        except Duplicate:
            pass

def sync_roles_to_rses():
    result = requests.get(CRIC_USERS_API, verify=False)  # Pods don't like the CRIC certificate
    all_cric_users = json.loads(result.text)

    site_managers = defaultdict(set)
    for user in all_cric_users:
        roles = user['ROLES']
        username = user['LOGIN']
        if 'data-manager' in roles:
            for thing in roles['data-manager']:
                if thing.startswith('site:'):
                    site = (thing.replace('site:', '', 1)).replace('-', '_')
                    site_managers[site].add(username)

    client = Client()
    all_rses = client.list_rses()

    for rse in all_rses:
        rse_name = rse['rse']
        if rse_name.lower() in site_managers:
            print("Setting manager for %s" % rse_name)
            set_rse_manager(client, rse_name, site_managers)
        else:
            set_approvers = False
            for suffix in TO_STRIP:
                test_name = rse_name.replace(suffix, '', 1)
                if test_name.lower() in site_managers:
                    print("Setting alternate for %s" % rse_name)
                    set_rse_manager(client, rse_name, site_managers, test_name)
                    set_approvers = True
                    break
            if not set_approvers:
                print("No site manager found for %s" % rse_name)


if __name__ == '__main__':
    """
    Sync site data manager roles to RSE attributes
    """
    sync_roles_to_rses()
