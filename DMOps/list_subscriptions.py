#! /usr/bin/env python


"""
get list of existing subscriptions
usage: python list_subscriptions.py --account transfer_ops --name Placement_NanoAODv5
"""

from __future__ import division, print_function
from rucio.client.client import Client
from rucio.common.exception import RucioException
import argparse

client = Client()
client.whoami()

class ArgumentParser():
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='list_subscriptions')
        self.parser.add_argument("--account", help="Specify account type", default="transfer_ops", action="store", dest="account")        
        self.parser.add_argument("--name", help="Specify subscription name", default=None, action="store", dest="name")

def get_subscriptions_list(args):
    try:
        subscriptions = client.list_subscriptions(account=args.account, name=args.name)
        for subscription in subscriptions:
            print(subscription)
    except RucioException as err:
        print(err)

if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    get_subscriptions_list(args)
