#! /usr/bin/env python
from __future__ import print_function

import json
import ssl
import urllib2

from rucio.client.client import Client
from rucio.common.exception import AccountNotFound

# Pods don't like the CRIC certificate
SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

CRIC_EGROUP_API = 'https://cms-cric-dev.cern.ch/api/accounts/group/query/?json&role=rucio-group'


def sync_egroups_to_group_accounts():
    all_cric_groups = json.load(urllib2.urlopen(CRIC_EGROUP_API, context=SSL_CONTEXT))

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
