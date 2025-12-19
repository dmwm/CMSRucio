#! /usr/bin/env python3

import getopt
import json
import os
import pprint
import sys

import requests
from rucio.client import Client
from rucio.common.exception import AccountNotFound, Duplicate, InvalidObject

from institute_policy import InstitutePolicy

sys.path.insert(1, './tests')
from policy_test import TestPolicy
from cric_user import CricUser
from utils import rfc2253dn

PROXY = os.getenv('X509_USER_PROXY')

client = Client()
institute_policy = InstitutePolicy()
test_policy = TestPolicy()
cric_user_list = []

"""
This function loads the JSON file by CRIC API or by local file depending on the dry_run option.
"""


def load_cric_users(policy, dry_run):
    if not dry_run:
        result = requests.get(policy.CRIC_USERS_API, cert=PROXY, verify=False)  # Pods don't like the CRIC certificate
        worldwide_cric_users = json.loads(result.text)
    else:
        sys.stdout.write('\t- dry_run version with the new fake user loaded\n')
        with open('fake_cric_users.json') as json_file:
            worldwide_cric_users = json.load(json_file)
    print("Found % users from CRIC" % len(worldwide_cric_users))
    return worldwide_cric_users


"""
For each CRIC user build a CricUser object with all the info needed to apply the US CMS policy in Rucio.
"""


def map_cric_users(country, option, dry_run):
    worldwide_cric_users = load_cric_users(institute_policy, dry_run)
    print('Dry run %s' % dry_run)
    for key, user in worldwide_cric_users.items():
        if option == 'delete-all':
            try:
                username = user['profiles'][0]['login']
            except (Exception, KeyError):
                continue
            for rse, val in client.get_account_limits(username).items():
                client.delete_account_limit(username, rse)

        institute_country = user['institute_country']
        institute = user['institute']
        dns = {rfc2253dn(user['dn'])}
        email = user['email']
        account_type = "USER"
        policy = ''

        try:
            username = user['profiles'][0]['login']
            if not institute or not institute_country:
                pass
                # policy = test_policy
                # raise Exception
            elif country != "" and country in institute_country:
                if username == 'perichmo':
                    continue
                policy = institute_policy
        except (Exception, KeyError):
            continue

        # Collect other DNs
        try:
            profiles = user['profiles']
            for profile in profiles:
                if 'dn' in profile:
                    dns.add(rfc2253dn(profile['dn']))
        except KeyError:
            continue

        dns = list(dns)
        cric_user = CricUser(username, email, dns, account_type, institute, institute_country, policy, option)
        cric_user_list.append(cric_user)
        set_rucio_limits(cric_user, dry_run)


"""
This function sets the Rucio limits, and if needed it also create a Rucio account.
"""


def set_rucio_limits(cric_user, dry_run):
    # FIXME: Add and subtract identities
    # FIXME: Pay attention to mode and add/subtract quotas
    # Move into cric user class
    try:
        account = cric_user.username
        email = cric_user.email
        print("Add account for %s %s" % (account, email))

        try:
            rucio_account = client.get_account(account)
            if rucio_account['email'] != email:
                # Update the account:
                if dry_run:
                    print('DRY-RUN: CRIC email is different from rucio email. Rucio email should be updated')
                else:
                    print('CRIC email is different from rucio email. Updating')
                    client.update_account(account, 'email', email)
        except AccountNotFound:
            if dry_run:
                print(f'DRY-RUN: Add account {account} with type {cric_user.account_type} and {email}.')
            else:
                client.add_account(account, cric_user.account_type, email)

        try:
            if dry_run:
                print(f'DRY-RUN: Add scope for account {account}.')
            else:
                client.add_scope(account, 'user.%s' % account)
            print('Scope added for user %s' % account)
        except Duplicate:
            print('Scope for user %s already existed' % account)

        if dry_run:
            print(f'DRY-RUN: Add identities to rucio with client {client}.')
        else:
            cric_user.add_identities_to_rucio(client=client)

        # Clear out old quotas. May want to remove this soon.

        limits = dict(client.get_local_account_limits(account=account))

        # if cric_user.rses_list:
        #     for rse in client.get_local_account_limits(account=account):
        #         client.delete_local_account_limit(account=account, rse=rse)

        for rse in cric_user.rses_list:
            if rse.quota > limits.get(rse.sitename, 0):
                print(" quota at %s: %s" % (rse.sitename, rse.quota))
                if dry_run:
                    print(f'DRY-RUN: set local account limit for account {account}, rse.sitename {rse.sitename}, and rse.quota {rse.quota}.')
                else:
                    client.set_local_account_limit(account, rse.sitename, rse.quota)
    except InvalidObject:
        print("Warning: could not add account or quota to account described by %s" % pprint.pformat(cric_user))


def get_cric_user(username):
    for user in cric_user_list:
        if user.username == username:
            return user
    raise KeyError


"""
This function modify the policy of one user.
"""


def change_cric_user_policy(username, policy):
    cric_user = get_cric_user(username)
    cric_user.change_policy(policy)
    set_rucio_limits(cric_user)


def usage():
    print("Command:\tuser_to_site_mapping.py [-o] [-d]")
    print("Options:")
    print("\t-h, --help")
    print("\t-o, --option=\tset-new-only|reset-all|delete-all")
    print("\t-d, --dry_run=\tt|f")


def main():
    option = 'set-new-only'
    dry_run = False

    # FIXME: Make dry run work in the standard way

    try:
        opts, args = getopt.getopt(sys.argv[1:], "ho:o:d:", ["help", "option=", "dry_run="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(str(err))  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    for o, a in opts:
        if o in ("-o", "--option"):
            option = a
        elif o in ("-d", "--dry_run"):
            dry_run = a
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        else:
            usage()
            sys.exit(2)

    if option not in ['set-new-only', 'delete-all', 'reset-all']:
        usage()
        sys.exit(2)

    map_cric_users('US', option, dry_run)


if __name__ == '__main__':
    main()
