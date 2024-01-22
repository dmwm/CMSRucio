#!/usr/bin/env python3

"""legacydn_converter.py

A script that retrieves identities from a Rucio instance, converts it to
an RFC2253 compliant DN, then adds it back to the account identity.
"""

import argparse
from itertools import groupby
import os

from pprint import pprint

from rucio.client.accountclient import AccountClient

PROXY = os.getenv('X509_USER_PROXY')

RFC_ATTRIBUTE_ORDER = ["CN", "L", "ST", "O", "OU", "C", "STREET", "DC", "UID"]

client = AccountClient()


def get_attribute(attribute: str) -> str:
    """Extracts attribute key"""
    try:
        return attribute[:attribute.index("=")]
    except ValueError:
        # print(f"element lacks attribute, {attribute}, {dn}")
        return ""


def rfc2253dn(legacy_dn: str, verbose: bool = False) -> str:
    """Converts legacy slash DB to comma separated DN"""
    if not legacy_dn.startswith('/'):
        if verbose:
            print(f'DN does not start with /: {legacy_dn}')
        return legacy_dn

    # replace commas with an escape character
    legacy_dn = legacy_dn.replace(',', r'\,')
    parts = legacy_dn.split('/')[1:]

    # parts are reversed. See https://datatracker.ietf.org/doc/html/rfc2253#section-2.3
    elements = parts[::-1]
    attributes = [get_attribute(e) for e in elements]

    # skip any DNs who have an element without an attribute
    if "" in attributes:
        return ""

    if verbose:
        print(f"Attributes: {attributes}")
        print(f"Expected order: {RFC_ATTRIBUTE_ORDER}")

    indexes = []
    for attr in attributes:
        try:
            indexes.append(RFC_ATTRIBUTE_ORDER.index(attr))
        except ValueError:
            # skips any DNs that don't have a attribute in the RFC_ATTRIBUTE_ORDER
            pass

    # sort existing attributes based on expected attribute order
    result = [a[0] for a in sorted(zip(elements, attributes, indexes), key=lambda x: x[2])]

    if verbose:
        print(f"original: {legacy_dn}\nindexes: {indexes}\nconverted: {','.join(result)}")

    return ','.join(result)


def convert_identities(account_type: str, dry_run: bool=True, verbose: bool=False):
    """Fetches and converts identities to rfc2253dn"""
    accounts = client.list_accounts(account_type=account_type)

    for account in accounts:
        # filter only X509 identities
        print(f"\n##### Checking account: {account['account']} #####")

        # get only X509 identities
        identities = [i for i in client.list_identities(account["account"]) if i['type'] == 'X509']
        print("Current identities\n-------------------")
        pprint(identities)

        # get a list of only the identities for an account
        ids = [i['identity'] for i in identities]

        added = []
        existing = []
        failed = []
        for identity in identities:
            if not identity['identity'].startswith('/'):
                continue

            new_dn = rfc2253dn(identity["identity"], verbose)

            if not new_dn:
                if verbose:
                    print(f"DN was invalid {identity['identity']}, will skip")
                continue

            if new_dn in ids:
                idx = ids.index(identity['identity'])
                if verbose:
                    print(f"Identity exists, skipping:\n{new_dn}\n{ids[idx]}")
                if not ids[idx].startswith('/'):
                    existing.append(new_dn)
                continue


            print(f"{'DRY RUN ' if dry_run else ''}Will add identity for account {account['account']}: '{new_dn}', type: X509, email: {identity['email']}")

            print("Adding to Rucio")
            # add identities
            try:
                if not dry_run:
                    client.add_identity(account['account'], new_dn, 'X509', identity['email'])
            except Exception as e:
                print(f"Could not add identity: {new_dn}, Exception: {e}")
                failed.append(identity)
                continue

                # delete old identity(?)
                # client.del_identity(account['account], identity['identity'], 'X509')
                    
            print(f"Successfully Added Identity, {'DRY RUN' if dry_run else ''}")

            added.append(new_dn)

        print(f"{len(existing)} identities already exist")
        print(f"{len(added)} identiites added")
        print(f"{len(failed)} identities failed to be added")
        print(f"##### Account {account['account']} Done #####")


def main():
    parser = argparse.ArgumentParser(
        description='LegacyDN Converter',
        epilog='Converts legacy DN to rfc2253 filtered by account type')
    parser.add_argument('--account_type',
                        action='store',
                        required=True,
                        choices=['GROUP', 'SERVICE'])
    parser.add_argument('--dry_run',
                        action='store_true')
    parser.add_argument('--verbose',
                        action='store_true')

    args = parser.parse_args()

    convert_identities(args.account_type, dry_run=args.dry_run, verbose=args.verbose)


if __name__ == "__main__":
    main()
