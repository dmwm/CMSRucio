#! /usr/bin/env python3
# Script for creating RSEs and updating their attributes.
# Initially will use PhEDEx nodes information as input,
# should be easily transformed/extended for using other sources.

import argparse
import logging
# import sys
# import pprint
#
# import urlparse
# import requests
# import json
# import re
#
# from functools import wraps
#
# from rucio.client.accountclient import AccountClient
# from rucio.client.client import Client
# from rucio.common.exception import Duplicate, RSEProtocolPriorityError, \
#     RSEProtocolNotSupported, RSENotFound, InvalidObject, CannotAuthenticate
#
import json
from CMSRSE import CMSRSE

SKIP_SITES = ['T2_FI_HIP', 'T2_ES_IFCA', 'T2_CH_CERN', 'T1_US_FNAL_Disk', 'T1_US_FNAL', 'T1_UK_RAL']
DO_SITES = ['T1_DE_KIT_Tape']

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''Create or update RSEs and their attributes
        based on JSON files''',
        epilog="""This is a test version use with care!"""
    )
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='increase output verbosity')
    parser.add_argument('-t', '--dry-run', action='store_true',
                        help='only printout what would have been done')
    parser.add_argument('--json', default='EXAMPLE_STORAGE.json', help=' a JSON file to retrieve site defintions from')

    args = parser.parse_args()
    if args.verbose:
        print(args)

    if args.json:
        with open(args.json, 'r') as json_file:
            sites = json.load(json_file)
        for site in sites:
            print('Checking %s' % site['rse'])
            if ((DO_SITES and site['rse'] in DO_SITES) or not DO_SITES) and site['rse'] not in SKIP_SITES:
                rse = CMSRSE(site)
                if rse.update():
                    logging.warning('RSE %s and type %s changed', rse.rse_name, rse.rucio_rse_type)

    # rse_client = Client()
