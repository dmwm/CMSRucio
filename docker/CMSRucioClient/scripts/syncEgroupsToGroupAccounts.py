#! /usr/bin/env python3
from __future__ import print_function

import json

import requests
from rucio.client.client import Client
from rucio.common.exception import AccountNotFound

CRIC_EGROUP_API = 'https://cms-cric-dev.cern.ch/api/accounts/group/query/?json&role=rucio-group'


def sync_egroups_to_group_accounts():
    result = requests.get(CRIC_EGROUP_API, verify=False)  # Pods don't like the CRIC certificate
    all_cric_groups = json.loads(result.text)

    client = Client()

    for group, data in all_cric_groups.items():

        if 'egroups' in data and data['egroups'] and 'users' in data and data['users']:
            group_name = group.replace('-', '_')
            group_email = data['egroups'][0] + '@cern.ch'

            try:
                client.get_account(group_name)
            except AccountNotFound:
                print('Adding group account %s with %s' % (group_name, group_email))
                client.add_account(group_name, 'GROUP', group_email)

            # Note: This does not pick up email changes with the same DN
            group_info = {user['dn']: user['email'] for user in data['users']}

            current_identities = set(identity['identity'] for identity in client.list_identities(group_name))
            target_identities = set(group_info.keys())
            add_identities = target_identities - current_identities
            del_identities = current_identities - target_identities

            for identity in add_identities:
                print('Adding %s to %s with %s' % (identity, group_name, group_info[identity]))
                client.add_identity(account=group_name, identity=identity, authtype='X509', email=group_info[identity])
            for identity in del_identities:
                print('Deleting %s from %s' % (identity, group_name))
                client.del_identity(account=group_name, identity=identity, authtype='X509')


if __name__ == '__main__':
    """
    Synchronize e-groups with the Rucio role from CRIC to rucio
    """
    sync_egroups_to_group_accounts()
