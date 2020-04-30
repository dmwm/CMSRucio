#! /usr/bin/env python


"""
shows given account info
"""

from __future__ import division, print_function
from rucio.client.client import Client
from rucio.common.exception import AccountNotFound
import argparse

client = Client()
client.whoami()

class ArgumentParser():
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='get_account')
        self.parser.add_argument("--acc", help="provide account", action="store", dest="acc", required=True)

def get_account(args):
    try:
        account = client.get_account(args.acc)
        print(account)
    except AccountNotFound as err:
        print(err)

if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    get_account(args)
