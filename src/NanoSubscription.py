#!/usr/bin/env python
from __future__ import print_function
from rucio.client.client import Client


def create_subscription(cl):
    filter = {
        'pattern': '^/.*/.*(NanoAODv5|Nano1June2019).*/NANOAOD(|SIM)$',
        'did_type': 'CONTAINER',
        'scope': ['cms'],
    }

    rules = [
        {
            "copies": 1,
            "rse_expression": "region=Region_1&cms_type=test",
            "lifetime": 3600*24*30,
            "activity": "Functional Test",
            "grouping": "ALL",
        },
        {
            "copies": 2,
            "rse_expression": "region=Region_2&cms_type=test",
            "lifetime": 3600*24*30,
            "activity": "Functional Test",
        },
        {
            "copies": 1,
            "rse_expression": "region=Region_3&cms_type=test",
            "lifetime": 3600*24*30,
            "activity": "Functional Test",
        },
        {
            "copies": 1,
            "rse_expression": "region=Region_4&cms_type=test",
            "lifetime": 3600*24*30,
            "activity": "Functional Test",
        },
    ]

    res = cl.add_subscription(name='Placement_NanoAODv5',
                            account='transfer_ops',
                            filter=filter,
                            replication_rules=rules,
                            comments='Million file test v2',
                            lifetime=False,
                            # retroactive=True,  # not supported
                            # dry_run=True  # BUG: this is ignored
                            )
    subscription_id = res
    return subscription_id


if __name__ == '__main__':
    cl = Client(account='root')

    # create_subscription(cl)

    # dumb update to move 'NEW' to 'UPDATED' since retroactive=True leaves in 'NEW'
    # res = cl.update_subscription(account='transfer_ops', name='Placement_NanoAODv5', comments='Million file test v2.', retroactive=False)

    res = cl.list_subscriptions(account='transfer_ops')
    print(list(res))
    
    res = cl.list_subscription_rules(account='transfer_ops', name='Placement_NanoAODv5')
    print(list(res))
