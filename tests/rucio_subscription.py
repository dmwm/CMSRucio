#!/usr/bin/env python
from __future__ import print_function
from rucio.client.client import Client

client = Client(account='root')
print(client.whoami())

filter = {
    'did_type': 'CONTAINER',
    'scope': ['cms'],
}

rules = [
    {
        "copies": 1,
        "rse_expression": "region=Region_1&cms_type=test",
        "lifetime": 3600*24*30,
        "activity": "Functional Test"
    },
    {
        "copies": 1,
        "rse_expression": "region=Region_2&cms_type=test",
        "lifetime": 3600*24*30,
        "activity": "Functional Test"
    },
]

res = client.add_subscription(
    name='Placement_NanoAODv4',
    account='transfer_ops',
    filter=filter,
    replication_rules=rules,
    comments='Start of million file test',
    lifetime=False,
    # retroactive=True, # Not a supported feature
    # dry_run=True  # BUG: this is ignored
)
print(res)

res = client.list_subscriptions(account='transfer_ops', name='Placement_NanoAODv4')
print(res)
