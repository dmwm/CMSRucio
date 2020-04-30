#! /usr/bin/env python


"""
returns given account info
usage: python update_account.py --acc transfer_ops --key email --value cms-comp-ops-transfer-team@cern.ch
"""

from __future__ import division, print_function
from rucio.client.client import Client
from rucio.common.exception import AccountNotFound
import argparse

client = Client()
client.whoami()

class ArgumentParser():
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='update_account')
        self.parser.add_argument("--acc", help="Specify account",  action="store", dest="acc", required=True)
        self.parser.add_argument("--key", help="Specify key name", action="store", dest="key", required=True)
        self.parser.add_argument("--value", help="Specify value for the key", action="store", dest="value", required=True)

def update_account(args):
    try:
        updated_account = client.update_account(account=args.acc, key=args.key, value=args.value)
        print("account was updated")
        check_updated_account = client.get_account(args.acc)
        print("new values" , check_updated_account)
    except AccountNotFound as err:
        print(err)

if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    update_account(args)
