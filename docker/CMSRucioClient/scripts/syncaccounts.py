#! /bin/env python
"""
Functions and scripts for creating the accounts for data synchronizarion
"""

import argparse
import logging
import os
import re

from rucio.client.client import Client
from rucio.common.exception import AccountNotFound, DatabaseException, Duplicate

SYNC_ACCOUNT_FMT = 'sync_%s'

class SyncAccounts(object):
    """
    Class for sync accounts for a list of 'real' RSEs
    """

    def __init__(self, rses=None, rsefilter=None, identity=None):

        self.rcli = Client(account='root', auth_type=None)

        self._get_identity(identity)

        if rses is None:
            rses = []
            for rse in self.rcli.list_rses():
                attrs = self.rcli.list_rse_attributes(rse=rse['rse'])
                if 'cms_type' in attrs and attrs['cms_type'] == 'real':
                    rses.append(rse['rse'])

        self.accounts = []

        for rse in rses:
            if rsefilter is None or re.match(rsefilter, rse):
                self.accounts.append(SYNC_ACCOUNT_FMT % rse.lower())

    def _get_identity(self, identity):

        if identity is None:
            identity = {'from': os.environ['RUCIO_ACCOUNT']}

        if 'from' in identity:
            identity = list(self.rcli.list_identities(
                account=identity['from']))[0]

        self.identity = identity


    def _create_account(self, account, dry=False):

        missing = False
        try:
            self.rcli.get_account(account)
        except AccountNotFound:
            missing = True

        if missing and dry:
            logging.info('creating account %s. Dry run',
                         account)
        elif missing:
            try:
                self.rcli.add_account(
                    account=account,
                    type='USER',
                    email=None
                )
                logging.debug('created account %s',
                              account)
            except DatabaseException:
                logging.warn('Could not create account %s',
                              account)

        return missing

    def _add_account_attr(self, account, dry=False):

        attrs = list(self.rcli.list_account_attributes(account))[0]
        attr = {u'key': u'admin', u'value': u'true'}
        # depending on rucio version also this can be a return value
        attr_alt = {u'key': u'admin', u'value': True}

        missing = (attr not in attrs) and (attr_alt not in attrs)

        if missing and dry:
            logging.info('setting attribute for account %s. Dry run',
                         account)
        elif missing:
            self.rcli.add_account_attribute(
                account=account,
                key=attr['key'],
                value=attr['value']
            )
            logging.debug('set attribute for account %s.',
                          account)

        return missing

    def _add_identity(self, account, dry=False):

        identities = list(self.rcli.list_identities(account=account))
        idmissing = self.identity not in identities

        if idmissing and dry:
            logging.info('adding %s for account %s. Dry run', self.identity, account)
        elif idmissing:
            try:
                self.rcli.add_identity(account=account, identity=self.identity['identity'],
                                       authtype=self.identity['type'], email=None)
                logging.debug('added %s for account %s', self.identity, account)
            except Duplicate:  # Sometimes idmissing doesn't seem to work
                logging.warn('identity %s for account %s existed', self.identity, account)
                return False
        return idmissing

    def update(self, dry=False):
        """
        Created or updates the sync accounts
        """

        stat = {
            'account': [],
            'attribute': [],
            'identity': [],
            'tot': []
        }

        for account in self.accounts:
            logging.debug("Considering %s", account)
            stat['tot'].append(account)

            created = self._create_account(account, dry)
            if created:
                stat['account'].append(account)

            if created and dry:
                stat['attribute'].append(account)
                stat['identity'].append(account)

            else:
                try:
                    logging.debug("Adding attribute to %s", account)

                    if self._add_account_attr(account, dry):
                        stat['attribute'].append(account)
                    logging.debug("Adding identity to %s", account)

                    if self._add_identity(account, dry):
                        stat['identity'].append(account)
                except AccountNotFound:
                    logging.warn('Attributes and identiy not added')

        return stat

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='''CLI for creating and updating the sync accounts.''',
    )
    PARSER.add_argument('-v', '--verbose', dest='debug', action='store_true',
                        help='increase output verbosity')
    PARSER.add_argument('-t', '--dry', dest='dry', action='store_true',
                        help='only printout what would have been done')
    PARSER.add_argument('--rse', dest='rse', help='PhEDEx node name. Can be multiple.\
                         Default all.', action='append', default=[])
    PARSER.add_argument('--rsefilter', dest='rsefilter', default=None,
                        help='Create accounts only for the rses that match the rsefilter.\
                        Default: no rsefilter')
    PARSER.add_argument('--identity', dest='identity', default=None,
                        help='Identity to be added to the account.\
                        Default: use id of current account')
    PARSER.add_argument('--type', dest='type', default=None,
                        help='Identity type to be added to the account.\
                        Default: use id type of current account')
    PARSER.add_argument('--fromaccount', dest='fromaccount', default=None,
                        help='add the identity from account. Default: current account.')
    OPTIONS = PARSER.parse_args()

    if OPTIONS.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if OPTIONS.rse == []:
        OPTIONS.rse = None

    if OPTIONS.identity is not None:
        IDENTITY = {'identity': OPTIONS.identity, 'type': OPTIONS.type}
    elif OPTIONS.fromaccount is not None:
        IDENTITY = {'from': OPTIONS.fromaccount}
    else:
        IDENTITY = None

    logging.info("Adding %s to accounts", IDENTITY)

    RESULT = SyncAccounts(
        rses=OPTIONS.rse,
        rsefilter=OPTIONS.rsefilter,
        identity=IDENTITY
    ).update(dry=OPTIONS.dry)


    logging.info("%d RSEs considered: %s", len(RESULT['tot']),
                 ', '.join(RESULT['tot']))
    logging.info("%d account created: %s", len(RESULT['account']),
                 ', '.join(RESULT['account']))
    logging.info("%d account attribute set: %s", len(RESULT['attribute']),
                 ', '.join(RESULT['attribute']))
    logging.info("%d account identity added: %s", len(RESULT['identity']),
                 ', '.join(RESULT['identity']))
