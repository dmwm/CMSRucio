#!/usr/bin/env python
from __future__ import print_function
from rucio.client.client import Client

client = Client(account='transfer_ops')


filter = {
    'pattern': '^/.*/NANOAOD(|SIM)$',
    'did_type': 'CONTAINER',
    'scope': ['cms'],
    'account': ['wma_prod'],
    # https://github.com/amaltaro/WMCore/blob/6cab0ce17731617f7f0b1f69b62410572263d194/src/python/WMCore/Services/Rucio/Rucio.py#L19
    'project': ['Production'],
}

rules = [
    {
        "copies": 2,
        "rse_expression": "region=A&cms_type=real",
        "activity": "Production Output",
        "grouping": "ALL",
    },
    {
        "copies": 2,
        "rse_expression": "region=B&cms_type=real",
        "activity": "Production Output",
        "grouping": "ALL",
    },
    {
        "copies": 1,
        "rse_expression": "region=C&cms_type=real",
        "activity": "Production Output",
        "grouping": "ALL",
    },
    {
        "copies": 1,
        "rse_expression": "region=D&cms_type=real",
        "activity": "Production Output",
        "grouping": "ALL",
    },
]

res = client.add_subscription(
    name='ProductionNanoAOD',
    account='transfer_ops',
    filter=filter,
    replication_rules=rules,
    comments='Controls the distribution of production output NanoAOD',
    lifetime=False,
    retroactive=False,  # Not a supported feature
    dry_run=False,  # BUG: this is ignored
)
print(res)


# follow https://github.com/CMSCompOps/WmAgentScripts/blob/1573cb17509ff780452750c52eb40e434034c3ad/Unified/closor.py#L499-L518
filter = {
    'pattern': '^/.*/NANOAOD(|SIM)$',
    'did_type': 'CONTAINER',
    'scope': ['cms'],
    'account': ['wma_prod'],
    # https://github.com/amaltaro/WMCore/blob/6cab0ce17731617f7f0b1f69b62410572263d194/src/python/WMCore/Services/Rucio/Rucio.py#L19
    'project': ['RelVal'],
}

rules = [
    {
        "copies": 1,
        "rse_expression": "T2_CH_CERN",
        "activity": "Production Output",
        "grouping": "ALL",
    },
]

res = client.add_subscription(
    name='RelValNanoAOD',
    account='transfer_ops',
    filter=filter,
    replication_rules=rules,
    comments='Controls the distribution of RelVal output NanoAOD',
    lifetime=False,
    retroactive=False,  # Not a supported feature
    dry_run=False,  # BUG: this is ignored
)
print(res)
