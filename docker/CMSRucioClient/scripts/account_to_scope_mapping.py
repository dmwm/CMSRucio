#! /usr/bin/env python3

import argparse
from rucio.client import Client
from rucio.common.exception import Duplicate


def add_group_account_scopes(rclient, include_local_users_accounts, only_include_accounts_with_group_suffix, dry_run=True):
    group_accounts = rclient.list_accounts(account_type="GROUP")
    group_scopes = [scope for scope in rclient.list_scopes() if scope.startswith("group.")]

    desired_group_scopes = set()
    existing_group_scopes = set()

    for account in group_accounts:
        account_name = account["account"]
        scope_name = f"group.{account_name}"
        if account_name.endswith("_local_users") or account_name.endswith("_local"):
            if include_local_users_accounts:
                desired_group_scopes.add((account_name, scope_name))
        elif only_include_accounts_with_group_suffix:
            if account_name.endswith("_group"):
                desired_group_scopes.add((account_name, scope_name))
        else:
            desired_group_scopes.add((account_name, scope_name))

    group_prefix_len = len("group.")
    for scope in group_scopes:
        account = scope[group_prefix_len:]
        existing_group_scopes.add((account, scope))

    scopes_to_add = desired_group_scopes - existing_group_scopes

    if dry_run:
        for account, scope in scopes_to_add:
            print(f'Scope: {scope} added for Group:{account}')
    else:
        for account, scope in scopes_to_add:
            try:
                rclient.add_scope(account, scope)
                print(f'Scope: {scope} added for Group:{account}')
            except Duplicate:  # should never happen since we eliminated such accounts already
                print(f'Scope for Group: {account} already exists')


if __name__ == '__main__':
    """
    Add group account scopes:
        - only_include_accounts_with_group_suffix: set to true to exclude legacy group accounts for scope creation  
        - include_local_users_accounts: set to true to create scopes for local user group accounts
    """

    rucio_client = Client()

    parser = argparse.ArgumentParser(
        prog='Group Account Scopes',
        description='Adds groups account scopes for existing group accounts in Rucio'
    )

    parser.add_argument("-d", "--dry-run", action="store_true", help="do not create scopes, just print them")
    parser.add_argument("--include-local-users-accounts", action="store_true")
    parser.add_argument("--only-include-accounts-with-group-suffix", action="store_true")

    args = parser.parse_args()

    add_group_account_scopes(
        rclient=rucio_client,
        include_local_users_accounts=args.include_local_users_accounts,
        only_include_accounts_with_group_suffix=args.only_include_accounts_with_group_suffix,
        dry_run=args.dry_run
    )
