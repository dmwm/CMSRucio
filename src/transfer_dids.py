#! /usr/bin/env python
import os
import argparse
from rucio.client.client import Client

# cannot seem to switch RUCIO_HOME at runtime, so hardcode all parameters here :(
INSTANCES = {
    'dev': {
        'rucio_host': 'https://cms-rucio-dev.cern.ch',
        'auth_host': 'https://cms-rucio-auth-dev.cern.ch',
        'account': os.environ['RUCIO_ACCOUNT'],
        'ca_cert': '/etc/grid-security/certificates/',
        'auth_type': 'x509_proxy',
        'creds': {'client_proxy': os.environ['X509_USER_PROXY']},
    },
    'int': {
        'rucio_host': 'https://cms-rucio-int.cern.ch',
        'auth_host': 'https://cms-rucio-auth-int.cern.ch',
        'account': os.environ['RUCIO_ACCOUNT'],
        'ca_cert': '/etc/grid-security/certificates/',
        'auth_type': 'x509_proxy',
        'creds': {'client_proxy': os.environ['X509_USER_PROXY']},
    },
}

def transfer(args):
    src_client = Client(**INSTANCES[args.src])
    dst_client = Client(**INSTANCES[args.dst])
    
    dids = (did.split(':') for did in args.DIDs)
    dids = [{'scope': d[0], 'name': d[1]} for d in dids]
    replicas = src_client.list_replicas(dids, rse_expression=args.rse)

    available = []
    for replica in replicas:
        if replica['states'][args.rse] == 'AVAILABLE':
            available.append({'scope': replica['scope'], 'name': replica['name']})
            if args.verbose:
                print('Available replica:', replica['name'])
        elif args.verbose:
            print('Unavailable replica:', replica)
    
    print("Total replicas: %d" % len(available))

    if not args.dry:
        res = dst_client.add_replicas(args.rse, available)
        if res:
            print("Replicas added successfully")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Transfer DIDs between identical RSEs on two Rucio instances')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable debug printouts')
    parser.add_argument('--src', type=str, help='Source instance', choices=INSTANCES.keys())
    parser.add_argument('--dst', type=str, help='Destination instance', choices=INSTANCES.keys())
    parser.add_argument('--rse', type=str, help='RSE name common to  both source and destination (assumes same LFN-to-PFN!!)')
    parser.add_argument('--dry', action='store_true', help='Dry-run (do not inject replicas into destination)')
    parser.add_argument('DIDs', type=str, nargs='+', help='DIDs to copy')
    args = parser.parse_args()

    transfer(args)
