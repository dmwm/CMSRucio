import json
import os
from collections import defaultdict
from unittest import result

import requests
from rucio.client.client import Client
from rucio.common.exception import AccountNotFound

# We query for role="group-data-manager"
CRIC_GROUP_ACCOUNTS_API = "https://cms-cric.cern.ch/api/accounts/group/query/?json&role=group-data-manager"

PROXY = os.getenv('X509_USER_PROXY')


def sync_group_accounts(client):
    """
    This function parses the "group-data-manager" role in the ACCOUNT/GROUP endpoint of the CRIC API
    - Creates a new group account if does not exists
    - Adds and deletes identities to the group account to match the state in CRIC 
    """

    result = requests.get(CRIC_GROUP_ACCOUNTS_API, cert=PROXY, verify=False)
    cric_group_data_managers = json.loads(result.text)

    for _, values in cric_group_data_managers:

        # TODO: confirm that `tag_name` is `group name`
        account = values['tag_name']
        try:
            client.get_account(account)
        except AccountNotFound:
            email = values['egroups'][0]+"@cern.ch"
            print(f"Adding group account {account} with email {email}")
            client.add_account(account, 'GROUP', email)

        current_identities = set()  # identities in rucio
        target_identities = set()  # identities according to CRIC
        identity_email_map = {}

        # filtering out OIDC identities (TODO: Should I or should I not?)
        for user_id in client.list_identities(account):
            if user_id['type'] == 'X509':
                current_identities.add(user_id['identity'])

        # TODO: Confirm that CRIC only has X509 identities
        for users in values['users']:
            for profile in users['sslprofiles']:
                target_identities.add(profile['dn'])
                identity_email_map[profile['dn']] = profile['email']

        identities_to_add = target_identities - current_identities
        identities_to_remove = current_identities - target_identities

        for identity in identities_to_add:
            print(
                f"Adding {identity} to {account} with {identity_email_map[identity]}")
            client.add_identity(account=account, identity=identity,
                                authtype='X509', email=identity_email_map[identity])

        for identity in identities_to_remove:
            print(f"Deleting {identity} from {account}")
            client.del_identity(
                account=account, identity=identity, authtype='X509')


if __name__ == '__main__':
    """
    Sync CRIC Group Accounts to Rucio Group Accounts
    """
    client = Client()
    sync_group_accounts(client)
