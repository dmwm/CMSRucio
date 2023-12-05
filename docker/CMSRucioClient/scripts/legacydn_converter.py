#!/usr/bin/env python3
import os

from rucio.client.accountclient import AccountClient

PROXY = os.getenv('X509_USER_PROXY')

client = AccountClient()

def rfc2253dn(legacy_dn: str) -> str:
    """Converts legacy slash DB to comma separated DN"""
    if not legacy_dn.startswith('/'):
        return legacy_dn

    legacy_dn = legacy_dn.replace(',', r'\,')
    parts = legacy_dn.split('/')[1:]

    return ','.join(parts)


def convert_identities(account_type: str, dry_run: bool=True):
    """Fetches and converts identities to rfc2253dn"""
    accounts = client.list_accounts(account_type=account_type)

    for account in accounts:
        identities = [i for i in client.list_identities(account["account"]) if i['type'] == 'X509']
        for identity in identities:
            new_dn = rfc2253dn(identity["identity"])
            print(f"old_dn: {identity['identity']} new_dn: {new_dn}")

            if not dry_run:
                # add identities
                # client.add_identity(account['account'], new_dn, 'X509', identity['email'])

                # delete old identity(?)
                # client.del_identity(account['account], identity['identity'], 'X509')
                pass


def main():
    # convert identities
    convert_identities("GROUP", dry_run=True)
    convert_identities("SERVICE", dry_run=True)



if __name__ == "__main__":
    main()
