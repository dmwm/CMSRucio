#! /usr/bin/env python3


"""
createAccounts.py and dumpAccounts.py work in tandem

dumpAccounts can be run against multiple Rucio servers and will accumulate, in accounts.json,
a list of all the accounts and associated DNs on those servers.

Once the list is complete, make a copy of accounts.json into "special_accounts.json"
and edit the latter file, removing any accounts and/or DNs which should not be added to the new
server. Run createAccounts.py which will add all the accounts and identities which were missing,
looking up the associated e-mails and account names in CRIC.

In the future we expect this script will grow into one which is used to do a full user sync of CRIC
to Rucio
"""

from __future__ import absolute_import, division, print_function

import json
import ssl
import urllib.request as urllib2

from rucio.client.accountclient import AccountClient
from rucio.common.exception import Duplicate, InvalidObject, AccountNotFound

CRIC_W_ROLES = 'https://cms-cric.cern.ch/api/accounts/user/query/?json&preset=roles'
CRIC_USERS = 'https://cms-cric.cern.ch/api/accounts/user/query/?json'

# Read in the dictionary of special accounts
try:
    with open('special_accounts.json', 'r') as json_file:
        special_accounts = json.load(json_file)
except IOError:
    special_accounts = {}

# Get the entire DN <-> user map from CRIC
# Should we ever need to run this again, it should be updated to use a certificate for CRIC and to "requests"
cric_data = json.load(urllib2.urlopen(CRIC_W_ROLES, context=ssl._create_unverified_context()))
cric_users = json.load(urllib2.urlopen(CRIC_USERS, context=ssl._create_unverified_context()))

current_dns = set()
new_accounts = set()
identity_map = {}
email_map = {}

# Build up lists of accounts to make, which DNs to make accounts form, and the correlation
for account in special_accounts:
    dns = special_accounts[account]['dns']
    new_accounts.add(account)
    for dn in dns:
        current_dns.add(dn)
        if dn in identity_map:
            identity_map[dn].add(account)
        else:
            identity_map[dn] = set([account])

# Also find the ordinary username for each of these accounts
for user_record in cric_data:
    dn = user_record['DN']
    username = user_record['LOGIN']
    if dn in current_dns:
        new_accounts.add(username)
        identity_map[dn].add(username)

# And the email for each DN
for email, record in cric_users.items():
    if record['dn'] in current_dns:
        email_map[record['dn']] = email

# Now we have new_accounts which tells us which accounts to create
# and identity_map which tells us how to map the identites to those accounts

ac = AccountClient()

# Make all the accounts from this list
for account in new_accounts:
    try:
        print('Add account %s' % account)
        ac.add_account(account=account, type='USER', email=None)
    except (Duplicate, InvalidObject):
        print(' Account %s already exists or invalid' % account)

# Add all the identities to the list
for dn, accounts in identity_map.items():
    email = email_map.get(dn, 'cms-rucio-dev@cern.ch')
    for account in accounts:
        try:
            print('Add identity %s for %s' % (dn, account))
            ac.add_identity(account=account, identity=dn, authtype='X509', email=email,
                            default=False)
        except (Duplicate, InvalidObject, AccountNotFound):
            print(' Identity %s for %s already exists or invalid' % (dn, account))
