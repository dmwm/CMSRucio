#! /usr/bin/env python3
import json
import os
from collections import defaultdict

import requests
from rucio.client.client import Client
from rucio.common.exception import RSEAttributeNotFound, Duplicate, AccountNotFound, InvalidObject

TO_STRIP = ['_Disk', '_Tape', '_Temp', '_Test', '_Disk_Test', '_Tape_Test', '_Ceph']

CRIC_USERS_API = 'https://cms-cric.cern.ch/api/accounts/user/query/list/?json&preset=roles'
CRIC_SITE_API = 'https://cms-cric.cern.ch/api/cms/site/query/?json'
CRIC_GROUP_API = 'https://cms-cric.cern.ch/api/accounts/group/query/?json'
PROXY = os.getenv('X509_USER_PROXY')


def make_request(api_endpoint, cert=PROXY, verify=False):
    # Pods don't like the CRIC certificate
    result = requests.get(api_endpoint, cert=cert, verify=verify)
    return json.loads(result.text)


def build_site_facility_map():
    """
    Build a dictionary mapping lower case site name to lower case facility name (used as an e-mail address)
    :return:
    """

    # Pods don't like the CRIC certificate
    all_sites = make_request(CRIC_SITE_API)
    site_map = {}
    for site, values in all_sites.items():
        # site-tags in in user roles use kebab-case
        site_map[site.replace('_', '-').lower()] = values['facility'].lower()

    return site_map


def sync_role_to_rse_attributes(client, users):
    """
    Sync site data manager roles to RSE attributes
    """

    users = users or make_request(CRIC_USERS_API)
    all_rses = client.list_rses()

    site_managers = defaultdict(set)

    for user in users:
        roles = user['ROLES']
        if 'data-manager' in roles:
            for tag in roles['data-manager']:
                site = tag.replace('site:', '', 1).replace('-', '_')
                site_managers[site].add(user['LOGIN'])

    for rse in all_rses:
        rse_name = rse['rse']
        if rse_name.lower() in site_managers:
            print(f"Setting manager for {rse_name}")
            set_rse_manager(client, rse_name, site_managers)
        else:
            set_approvers = False
            for suffix in TO_STRIP:
                test_name = rse_name.replace(suffix, '', 1)
                if test_name.lower() in site_managers:
                    print(f"Setting alternate for {rse_name}")
                    set_rse_manager(client, rse_name, site_managers, test_name)
                    set_approvers = True
                    break
            if not set_approvers:
                print(f"No site manager found for {rse_name}")


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
    print(f"Setting managers for {rse_name} to {rule_approvers}")

    client.add_rse_attribute(rse=rse_name, key='rule_approvers', value=rule_approvers)
    # For now, quota approvers are also rule approvers
    client.add_rse_attribute(rse=rse_name, key='quota_approvers', value=rule_approvers)

    # This is perhaps temporary
    for account in site_managers[alt_rse]:
        try:
            client.add_account_attribute(account=account, key='country-XX', value='admin')
        except Duplicate:
            pass


def get_egroup_map(all_groups, tag_relation="facility", role="local-data-manager"):
    egroup_map = defaultdict(set)
    for _, values in all_groups.items():
        if values['tag_relation'] == tag_relation and values['role'] == role:
            tag_name = values['tag_name'].lower()
            for egroup in values['egroups']:
                egroup_map[tag_name].add(egroup)

    return egroup_map


def sync_roles_to_groups(client, users):
    """
    Sync roles to respctive group accounts
    - local_users: site groups
    - group_users: physics groups
    """

    local_users = defaultdict(set)
    group_users = defaultdict(set)

    # Both can be used as one as well
    dn_email_map_local = {}
    dn_email_map_group = {}

    site_map = build_site_facility_map()
    all_groups = make_request(CRIC_GROUP_API)
    # get groups for group email accounts
    facility_egroups = get_egroup_map(all_groups, tag_relation="facility", role="local-data-manager")
    group_egroups = get_egroup_map(all_groups, tag_relation="group", role="group-data-manager")

    for user in users:
        roles = user['ROLES']
        dn = user['DN']
        email = user['EMAIL']

        # data-managers: are site managers / admins?
        # We also add them to list of local users
        if 'data-manager' in roles:
            for tag in roles['data-manager']:
                if tag.startswith('site:'):
                    site = tag.replace('site:', '', 1)
                    local_users[site].add(dn)
                    dn_email_map_local[dn] = email

        # local-data-managers: set of users that can place data on the site
        if 'local-data-manager' in roles:
            for tag in roles['local-data-manager']:
                if tag.startswith('site:'):
                    site = tag.replace('site:', '', 1)
                    local_users[site].add(dn)
                    dn_email_map_local[dn] = email

        # group-data-manager: set of users that manage data for a physics group
        if 'group-data-manager' in roles:
            for tag in roles['group-data-manager']:
                if tag.startswith('group:'):
                    group = tag.replace("group:", '', 1)
                    group_users[group].add(dn)
                    dn_email_map_group[dn] = email

    for site, dns in local_users.items():
        try:
            # At this point site-map does not contain deleted/disabled sites
            # (i.e it only contains site with state=ACTIVE) this is the default result from the CRIC api
            # These sites-tags are however still present under users
            # TODO: Should we include / exclude them?
            if site in site_map:
                facility = site_map[site]

                account = site + '_local_users'
                if len(account) > 25:
                    account = site + '_local'
                account = account.replace('-', '_')

                # ideally should be of unit length, we try to catch the missing egroup value
                egroup_list = list(facility_egroups[facility])
                if len(egroup_list) == 0:
                    print(f"WARNING: {site} is missing an associated e-group for local-data-manager role")
                    continue
                egroup = egroup_list[0]

                set_identities(client=client, account=account, egroup=egroup, target_identities=dns, dn_email_map=dn_email_map_local)

        except (KeyError, InvalidObject) as e:
            print(f"Could not make account for {site}. Perhaps the facility is not defined or the name is too long.")

    for group, dns in group_users.items():
        try:

            account = group + '_group'
            account = account.replace('-', '_')

            # ideally should be of unit length, we try to catch the missing egroup value
            egroup_list = list(group_egroups[group])
            if len(egroup_list) == 0:
                print(f"WARNING: {group} is missing an associated e-group for group-data-manager role")
                continue
            egroup = egroup_list[0]

            set_identities(client=client, account=account, egroup=egroup, target_identities=dns, dn_email_map=dn_email_map_group)

        except (KeyError, InvalidObject):
            print(f"Could not make account for {group}.")


def set_identities(client, account, egroup, dn_email_map, target_identities=set()):
    """
    Creates a group account if needed
    Updates set of assiciated identities of the group account
    """

    email = egroup + '@cern.ch'

    # Check if required group account exists in Rucio
    # If not create one
    try:
        print(f"Checking for account {account} in Rucio")
        client.get_account(account)
    except AccountNotFound:
        print(f"Adding group account {account} with {email}")
        client.add_account(account, 'GROUP', email)

    # Update rucio identities to match target_identities (cric)
    current_identities = set(identity['identity'] for identity in client.list_identities(account))

    add_identities = target_identities - current_identities
    del_identities = current_identities - target_identities

    for identity in add_identities:
        print(f"Adding {identity} to {account} with {dn_email_map[identity]}")
        client.add_identity(account=account, identity=identity, authtype='X509', email=dn_email_map[identity])
    for identity in del_identities:
        if identity.startswith("SUB="):
            print("OIDC identity, skipping delete")
            continue
        print(f"Deleting {identity} from {account}")
        client.del_identity(account=account, identity=identity, authtype='X509')


if __name__ == '__main__':
    """
    Sync User Roles:
        - site data manager roles to RSE attributes
        - local data manager roles to local group identities
        - group data manager roles to physics group identities
    """

    rucio_client = Client()
    all_cric_users = make_request(CRIC_USERS_API)

    sync_role_to_rse_attributes(client=rucio_client, users=all_cric_users)
    sync_roles_to_groups(client=rucio_client, users=all_cric_users)
