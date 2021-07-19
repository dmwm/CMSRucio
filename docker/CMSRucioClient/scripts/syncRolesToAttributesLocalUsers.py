#! /usr/bin/env python3
from __future__ import print_function

import json
import os
from collections import defaultdict

import requests
from rucio.client.client import Client
from rucio.common.exception import RSEAttributeNotFound, Duplicate, AccountNotFound, InvalidObject

TO_STRIP = ['_Disk', '_Tape', '_Temp', '_Test', '_Disk_Test', '_Tape_Test', '_Ceph']

CRIC_USERS_API = 'https://cms-cric.cern.ch/api/accounts/user/query/list/?json&preset=roles'
CRIC_SITE_API = 'https://cms-cric.cern.ch/api/cms/site/query/?json'
PROXY = os.getenv('X509_USER_PROXY')


def build_site_facility_map():
    """
    Build a dictionary mapping lower case site name to lower case facility name (used as an e-mail address)
    :return:
    """

    result = requests.get(CRIC_SITE_API, cert=PROXY, verify=False)  # Pods don't like the CRIC certificate
    all_sites = json.loads(result.text)
    site_map = {}
    for site, values in all_sites.items():
        site_map[site.lower()] = values['facility'].lower()

    return site_map


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


def set_local_identities(client, site, dns=None, user_map=None, site_map=None):
    dns = dns or set()
    user_map = user_map or {}

    account = site + '_local_users'
    if len(account) > 25:
        account = site + '_local'
    account = account.replace('-', '_')

    # Go back and forth since sites are underscore, emails are dash
    site_key = site.replace('-', '_')
    email = 'cms-' + site_map[site_key] + '-local@cern.ch'
    email = email.replace('_', '-')

    try:
        print('Checking for account %s in Rucio' % account)
        client.get_account(account)
    except AccountNotFound:
        print('Adding group account %s with %s' % (account, email))
        client.add_account(account, 'GROUP', email)

    current_identities = set(identity['identity'] for identity in client.list_identities(account))
    target_identities = dns
    add_identities = target_identities - current_identities
    del_identities = current_identities - target_identities

    for identity in add_identities:
        print('Adding %s to %s with %s' % (identity, account, user_map[identity]))
        client.add_identity(account=account, identity=identity, authtype='X509', email=user_map[identity])
    for identity in del_identities:
        print('Deleting %s from %s' % (identity, account))
        client.del_identity(account=account, identity=identity, authtype='X509')


def sync_roles_to_rses():
    result = requests.get(CRIC_USERS_API, cert=PROXY, verify=False)  # Pods don't like the CRIC certificate
    all_cric_users = json.loads(result.text)
    site_map = build_site_facility_map()

    site_managers = defaultdict(set)
    local_users = defaultdict(set)
    dn_account_map = {}
    for user in all_cric_users:
        roles = user['ROLES']
        username = user['LOGIN']
        dn = user['DN']
        if 'data-manager' in roles:
            for thing in roles['data-manager']:
                if thing.startswith('site:'):
                    site = (thing.replace('site:', '', 1)).replace('-', '_')
                    site_managers[site].add(username)
                    # Add data managers to local users as well
                    user_site = (thing.replace('site:', '', 1))
                    local_users[user_site].add(dn)
                    dn_account_map[dn] = username

        if 'local-data-manager' in roles:
            for thing in roles['local-data-manager']:
                if thing.startswith('site:'):
                    site = (thing.replace('site:', '', 1))
                    local_users[site].add(dn)
                    dn_account_map[dn] = username

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

    for site, dns in local_users.items():
        try:
            set_local_identities(client=client, site=site, dns=dns, user_map=dn_account_map, site_map=site_map)
        except (KeyError, InvalidObject):
            print("Could not make account for %s. Perhaps the facility is not defined or the name is too long." % site)

if __name__ == '__main__':
    """
    Sync site data manager roles to RSE attributes
    """
    sync_roles_to_rses()
