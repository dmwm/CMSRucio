#!/usr/bin/env python
from __future__ import print_function
from rucio.client.client import Client

cl = Client(account='root')
print(cl.whoami())

def extract_scope(did):
    # Try to extract the scope from the DSN
    if did.find(':') > -1:
        scope, name = did.split(':')[0], did.split(':')[1]
        if name.endswith('/'):
            name = name[:-1]
        return scope, name
    else:
        scope = did.split('.')[0]
        if did.startswith('user') or did.startswith('group'):
            scope = ".".join(did.split('.')[0:2])
        if did.endswith('/'):
            did = did[:-1]
        return scope, did





with open('nanoaodv4.txt') as datasets:
    for did in datasets:
        if not did:
            continue
        print('Fixing up %s' % did)
        did = did.strip()
        scope, name = extract_scope(did)
        try:
            cl.set_metadata(scope, name, 'is_new', True)
        except:
            print(' failed')
