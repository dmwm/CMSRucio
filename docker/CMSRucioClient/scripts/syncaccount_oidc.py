#! /bin/env python3
"""
Functions and scripts to sync OIDC identities on user accounts
"""

import os
import json
import requests

import liboidcagent as agent
from rucio.client.client import Client

# DO NOT TOUCH THIS
# looks like a bug, but any value different from 100 is ignored
page_length = 100

iam_server = "https://cms-auth.web.cern.ch/"

def  get_oidc_identities(n_iteration, headers):

    params = { 'startIndex': n_iteration*page_length + 1 }
    r = requests.get(iam_server+"scim/Users", headers=headers, params=params)

    tids = []
    usernames = []
    emails = []

    for user in json.loads(r.text)["Resources"]:
        username = user["displayName"]
        tid = user['id']
        for _email in user['emails']:
            if _email['primary']:
                email = _email['value']
                break
        if not email:
            print(f"NO PRIMARY EMAILS -- SKIPPING {tid}")
            continue
        #"emails":[{"type":"work","value":"omar.alterkait@colorado.edu","primary":true}]
        if 'urn:indigo-dc:scim:schemas:IndigoUser' in user:
            if 'oidcIds' in user['urn:indigo-dc:scim:schemas:IndigoUser']:
                for oidcIds in user['urn:indigo-dc:scim:schemas:IndigoUser']['oidcIds']:
                    if "https://auth.cern.ch/auth/realms/cern" in oidcIds["issuer"]:
                        username = oidcIds['subject']

        tids.append(tid)
        usernames.append(username)
        emails.append(email)

    return tids, usernames, emails

def sync_identities(client, tid, account, email):

    try:
        current_identities = set(identity['identity'] for identity in client.list_identities(account))
    except:
        print(f"ERROR: failed to found account {account} {tid}")
        return

    target_identities = set([f"SUB={tid}, ISS={iam_server}"])
    add_identities = target_identities - current_identities
    del_identities = current_identities - target_identities

    for identity in add_identities:
        print('Adding %s to %s with %s' % (identity, account, email))
        client.add_identity(account=account, identity=identity, authtype='OIDC', email=email)
    for identity in del_identities:
        if not identity.startswith("SUB="):
            print('X509 identity, skipping delete')
            continue
        print('Deleting %s from %s' % (identity, account))
        client.del_identity(account=account, identity=identity, authtype='OIDC')

if __name__ == '__main__':
    """
    sync OIDC identities on user accounts
    """

    try:
        iam_server = os.environ.get(
            "IAM_SERVER", "https://cms-auth.web.cern.ch/")
        iam_client_id = os.environ.get("IAM_CLIENT_ID")
        iam_client_secret = os.environ.get("IAM_CLIENT_SECRET")
    except Exception as ex:
        print(ex)
        exit(1)

    token = None
    try:
        request_data = {
            "client_id": iam_client_id,
            "client_secret": iam_client_secret,
            "grant_type": "client_credentials",
            "username": "not_needed",
            "password": "not_needed",
            "scope": "scim:read"
        }
        r = requests.post(iam_server+"token", data=request_data)
        response = json.loads(r.text)

        print(iam_client_id, iam_client_secret, response)
        token = response['access_token']
    except Exception as e:
        raise RuntimeError("ERROR oidc get token: {}".format(e))

    params = {}
    headers = {'Authorization': 'Bearer %s' % token,
               'Content-type': 'application/json'}
    r = requests.get(iam_server+"scim/Users", headers=headers, params=params)


    #print(r.text)
    n_results = json.loads(r.text)['totalResults']

    n_iteration = 0
    tids_list = []
    usernames_list = []
    emails_list = []

    while (n_iteration*page_length) < n_results:

        tids, usernames, emails = get_oidc_identities(n_iteration, headers)

        tids_list += tids
        usernames_list += usernames
        emails_list += emails

        n_iteration += 1

    #for tid, account, email in zip(tids_list, usernames_list, emails_list):
    #    print( tid, account, email )

    print(f"Syncing {len(tids_list)} users")

    client = Client()

    for tid, account, email in zip(tids_list, usernames_list, emails_list):
        sync_identities(client, tid, account,email)
