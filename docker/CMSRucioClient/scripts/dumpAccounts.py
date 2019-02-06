#! /usr/bin/env python

"""
createAccounts.py and dumpAccounts.py work in tandem. See the documentation in createAccounts.py
"""

from __future__ import absolute_import, division, print_function

import json

from rucio.client.accountclient import AccountClient

try:
    with open('accounts.json', 'r') as json_file:
        all_accounts = json.load(json_file)
except IOError:
    all_accounts = {}
try:
    with open('dn_emails.json', 'r') as json_file:
        dn_emails = json.load(json_file)
except IOError:
    dn_emails = {}

ac = AccountClient()

for account in ac.list_accounts():
    account_name = account['account']
    account_email = account['email']

    print(account)

    if account_name not in all_accounts:
        all_accounts.update({account_name: {'dns': set()}})
    else:
        all_accounts[account_name]['email'] = account_email
        all_accounts[account_name]['dns'] = set(all_accounts[account_name]['dns'])

    for identity in ac.list_identities(account_name):
        if identity['type'] == 'X509':
            all_accounts[account_name]['dns'].add(identity['identity'])
            if 'email' in identity:
                dn_emails.update({identity['identity']: identity['email']})

for account in all_accounts:
    all_accounts[account]['dns'] = list(all_accounts[account]['dns'])

with open('accounts.json', 'w') as json_file:
    json.dump(all_accounts, json_file, indent=1)
with open('dn_emails.json', 'w') as json_file:
    json.dump(dn_emails, json_file, indent=1)
