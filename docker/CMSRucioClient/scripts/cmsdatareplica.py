#! /bin/env python
"""
Command line tool for registering a CMS dataset into rucio
"""

from __future__ import absolute_import, division, print_function

import re
import time
from random import randint
import argparse
import traceback
import multiprocessing
from multiprocessing_logging import install_mp_handler


from instrument import timer, get_timing
from custom_logging import logging, get_levels
from rucio.client.client import Client
from rucio.common.exception import DataIdentifierNotFound, CannotAuthenticate, FileAlreadyExists,\
    DataIdentifierAlreadyExists, DatabaseException

from phedex import PhEDEx
from syncaccounts import SYNC_ACCOUNT_FMT

DEFAULT_RSE_FMT = '%s'
DEFAULT_SCOPE = 'cms'

#pylint: disable=too-many-instance-attributes
class CMSRucioDatasetReplica(object):
    """
    Class repeesenting the replica at a site af a CMS Dataset (PhEDEx FileBlock)
    """
    #pylint: disable=too-many-arguments
    def __init__(self, rds, pnn, rse=None, scope=DEFAULT_SCOPE,
                 lifetime=None, pcli=None, rcli=None):
        """
        Get the status of replica of pditem at pnn
        considering only closed blocks completely replicated at site.

        :pnn:    PhEDEx node name.
        :rds:    Rucio Dataset (PhEDEx FileBlock) name.
        :rse:    Rucio RSE. If None (default) inferred by the pnn using DEFAULT_RSE_FMT.
        :scope:  Scope. Default: DEFAULT_SCOPE.
        :pcli:   Reference to a phedex.PhEDEx object or a dict
                 {'instance': <instance>, 'dasgoclient': <path>, 'datasvc': <url>}
                 none of the keys is mandatory. Default is {}.
        :rcli:   Reference to a rucio Client() instance or a dict
                 {'accont': ..., ... } none of the keys is mandatory.
                 Default is {'account': <sync account>}
        """

        self.pnn = pnn

        self._get_pcli(pcli)

        self._get_rcli(rcli)

        if rse is None:
            self.rse = self.rcli.list_rses('cms_type=real&pnn=%s' %
                                           self.pnn)[0]['rse']
        else:
            self.rse = rse

        self.container = self.pcli.check_data_item(pditem=rds)['pds']

        self.dataset = rds

        self.scope = scope

        self.lifetime = lifetime

        self.block_at_pnn()

        if self.is_at_pnn:
            self.replicas = self.pcli.fileblock_files(pnn=pnn, pfb=rds)
        else:
            self.replicas = {}

    def _get_pcli(self, pcli):
        if pcli is None:
            pcli = {}

        if isinstance(pcli, dict):
            self.pcli = PhEDEx(**pcli)
        elif isinstance(pcli, PhEDEx):
            #pylint: disable=redefined-variable-type
            self.pcli = pcli
        else:
            raise Exception("wrong type for pcli parameter %s" %\
                            type(pcli))


    def _get_rcli(self, rcli):
        if rcli is None:
            rcli = {}

        if isinstance(rcli, dict):
            if 'account' not in rcli:
                rcli['account'] = SYNC_ACCOUNT_FMT % self.pnn.lower()
            self.rcli = Client(**rcli)
        elif isinstance(rcli, Client):
            #pylint: disable=redefined-variable-type
            self.rcli = rcli
        else:
            raise Exception("wrong type for rcli parameter %s" %\
                            type(rcli))

    def block_at_pnn(self):
        """
        Verify if the block is at pnn (using phedex datasvn)
        """
        metadata = self.pcli.list_data_items(
            pditem=self.dataset,
            pnn=self.pnn,
            locality=True,
            metadata=True
        )
        self.is_at_pnn = bool(len(metadata) == 1 and\
            'block' in metadata[0] and\
            'replica' in metadata[0]['block'][0] and\
            metadata[0]['block'][0]['replica'][0]['complete'] == 'y')


    def register_container(self, dry=False):
        """
        Register container of the dataset
        (only if there is a dataset replica on the pnn)
        :dry: Dry run. Default false.
        """

        try:
            self.rcli.get_did(scope=self.scope, name=self.container)
            return 'exists'
        except DataIdentifierNotFound:
            pass

        if self.is_at_pnn and dry:
            logging.dry('Create container %s in scope %s.',
                        self.container, self.scope)
            return 'created'
        elif self.is_at_pnn:
            logging.verbose('Create container %s in scope %s.',
                            self.container, self.scope)
            try:
                self.rcli.add_container(scope=self.scope, name=self.container,
                                        lifetime=self.lifetime)

            except DataIdentifierAlreadyExists:
                logging.warning('Container was created in the meanwhile')
                return 'exists'

            return 'created'

        return 'skipped'


    def register_dataset(self, dry=False):
        """
        Register the dataset (if there is a replica at the pnn)
        :dry: Dry run. Default false.
        """

        try:
            self.rcli.get_did(scope=self.scope, name=self.dataset)
            return 'exists'
        except DataIdentifierNotFound:
            pass

        if self.is_at_pnn and dry:
            logging.dry('Create dataset %s in scope %s.',
                        self.dataset, self.scope)
            return 'created'

        elif self.is_at_pnn:
            logging.verbose('Create dataset %s in scope %s.',
                            self.dataset, self.scope)
            self.rcli.add_dataset(scope=self.scope, name=self.dataset,
                                  lifetime=self.lifetime)
            self.rcli.attach_dids(scope=self.scope, name=self.container,
                                  dids=[{'scope': self.scope, 'name': self.dataset}])
            return 'created'

        return 'skipped'


    def update_replicas(self, dry=False):
        """
        Add or removes replicas for the dataset at rse.
        :dry:  Drydrun. default false
        """

        rrepl = [repl['name'] for repl in self.rcli.list_replicas([{
            'scope': self.scope,
            'name': self.dataset
        }], rse_expression='rse=%s' % self.rse)]

        prepl = [repl for repl in self.replicas.keys()]

        missing = list(set(prepl) - set(rrepl))

        to_remove = list(set(rrepl) - set(prepl))

        if missing and dry:
            logging.dry('Adding replicas %s to rse %s.',
                        str(missing), self.rse)

        elif missing:
            logging.verbose('Adding replicas %s to rse %s.',
                            str(missing), self.rse)

            self.rcli.add_replicas(
                rse=self.rse,
                files=[{
                    'scope': self.scope,
                    'name': self.replicas[lfn]['name'],
                    'adler32': self.replicas[lfn]['checksum'],
                    'bytes': self.replicas[lfn]['size'],
                } for lfn in missing])

            # missing files that are not in the list of dataset files
            # are to be attached.
            lfns = [item['name'] for item in self.rcli.list_files(
                scope=self.scope,
                name=self.dataset
            )]

            missing_lfns = list(set(missing) - set(lfns))
            if missing_lfns:
                logging.verbose('Attaching lfns %s to dataset %s.',
                                str(missing_lfns), self.dataset)


                try:
                    self.rcli.attach_dids(
                        scope=self.scope,
                        name=self.dataset,
                        dids=[{
                            'scope': self.scope,
                            'name': lfn
                        } for lfn in list(set(missing) - set(lfns))]
                    )

                except FileAlreadyExists:
                    logging.warning('Trying to attach already existing files.')

        if to_remove and dry:
            logging.dry('Removing replicas %s from rse %s.',
                        str(to_remove), self.rse)

        elif to_remove:
            logging.verbose('Removing replicas %s from rse %s.',
                            str(to_remove), self.rse)
            attempt = 0
            while True:
                attempt += 1
                try:
                    self.rcli.delete_replicas(rse=self.rse, files=[{
                        'scope': self.scope,
                        'name': lfn,
                    } for lfn in to_remove])
                    break
                except DatabaseException:
                    logging.warning('DatabaseException raised, retrying...')
                    if attempt > 3:
                        raise
                    time.sleep(randint(1, 5))

        return {'added': missing, 'removed': to_remove}


    def update_rule(self, dry=False):
        """
        Adds or removes the rule for the dataset.
        :dry:  Drydrun. default false

        returns the action performed: None, added, removed
        """
        rules = self.rcli.list_did_rules(scope=self.scope, name=self.dataset)
        rrule = None
        account = self.rcli.__dict__['account']
        action = None
        rse_exp = 'rse=' + self.rse

        rrule = next((
            rule for rule in rules
            if rule['account'] == account and\
                rule['rse_expression'] == rse_exp
        ), None)

        if rrule is None and self.is_at_pnn:

            if dry:
                logging.dry("Adding rule for dataset %s at rse %s.",
                            self.dataset, self.rse)
            else:
                self.rcli.add_replication_rule(
                    dids=[{'scope': self.scope, 'name': self.dataset}],
                    copies=1,
                    rse_expression=rse_exp,
                )
            action = 'added'

        elif rrule is not None and not self.is_at_pnn:
            # removing rule
            if dry:
                logging.dry("Removing rule for dataset %s at rse %s.",
                            self.dataset, self.rse)
            else:
                self.rcli.delete_replication_rule(rrule['id'], purge_replicas=False)
            action = 'removed'

        return action

    def update(self, dry=False):
        """
        syncronize the dataset replica info.
        :dry:  Drydrun. default false
        """
        ret = {'at_node': self.is_at_pnn}

        #datasets and containers are only added
        ret['container'] = self.register_container(dry)
        ret['dataset'] = self.register_dataset(dry)

        ret['replicas'] = self.update_replicas(dry)
        ret['rule'] = self.update_rule(dry)

        return ret

#pylint: disable=too-many-arguments
def dataset_replica_update(dataset, pnn, rse, pcli, account, dry):
    """
    Just wrapping the update method.
    """

    try:
        rcli = Client(account=account)
    except CannotAuthenticate:
        logging.warning("cannot authenticate with account %s, skipping pnn %s",
                        account, pnn)
        return None


    logging.my_fmt(label='update:rse=%s:rds=%s' % (pnn, dataset))

    logging.notice('Starting.')

    try:
        ret = _replica_update(dataset, pnn, rse, pcli, rcli, dry)

    #pylint: disable=broad-except
    except Exception as exc:
        logging.error('Exception %s raised: %s',
                      type(exc).__name__,
                      traceback.format_exc().replace('\n', '~~'))
        return None

    logging.notice('Finished %s.', ret)


@timer
def _replica_update(dataset, pnn, rse, pcli, rcli, dry):
    ret = CMSRucioDatasetReplica(
        rds=dataset,
        pnn=pnn,
        rse=rse,
        pcli=pcli,
        rcli=rcli
    ).update(
        dry=dry
    )

    ret['replicas']['added'] = len(ret['replicas']['added'])
    ret['replicas']['removed'] = len(ret['replicas']['removed'])
    return ret

@timer
def _get_dset_list(pcli, datasets):

    logging.verbose("Getting datasets list for: %s",
                    datasets)
    ret = []

    wildcard = re.compile(r'\S*[*]\S*')

    for dset in datasets:
        ret.extend([
            item for
            item in pcli.list_data_items(pditem=dset, metadata=False, locality=False)
            if not wildcard.match(item)
        ])

    ret = list(set(ret))

    logging.verbose("Got %d datasets", len(ret))

    return ret

@timer
def _launch_workers(pnns, datasets, pool, options, pcli):

    procs = []

    rcli = Client()

    for pnn in pnns:

        account = options.account or SYNC_ACCOUNT_FMT % pnn.lower()
#        try:
#            rcli = Client(account=account)
#        except CannotAuthenticate:
#            logging.warning("cannot authenticate with account %s, skipping pnn %s",
#                            account, pnn)
#            continue

        rse = list(rcli.list_rses('pnn=%s&cms_type=real' % pnn))

        if not rse:
            logging.warning("cannot find real rse for pnn %s, skipping", pnn)
            continue

        rse = rse[0]['rse']

        for dataset in datasets:
            procs.append(pool.apply_async(
                dataset_replica_update,
                (dataset, pnn, rse, pcli, account, options.dry)
            ))

    return procs

@timer
def _get_workers(pool, procs):
    pool.close()

    for proc in procs:
        proc.get()


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='''CLI for updating a CMS RSE''',
    )
    PARSER.add_argument('-v', '--verbosity', dest='verbosity',
                        choices=[lev['name'] for lev in get_levels()],
                        help='Define verbosity level. Default DRY', default='DRY')
    PARSER.add_argument('-t', '--dry', dest='dry', action='store_true',
                        help='only printout what would have been done')
    PARSER.add_argument('--pnn', dest='pnn', help='PhEDEx node name regex. Can be multiple.',
                        action='append', default=[])
    PARSER.add_argument('--account', dest='account', default=None,
                        help='Rucio account. default the sync account')
    PARSER.add_argument('--dataset', dest='dataset', action='append', default=[],
                        help='dataset to be updates. Can have wildcard and can be multiple')
    PARSER.add_argument('--pool', dest='pool', default=1,
                        help='number of parallel threads. Default 1.')

    OPTIONS = PARSER.parse_args()

    logging.my_lvl(OPTIONS.verbosity)

#    logging.summary('DBP1')

    install_mp_handler()
    POOL = multiprocessing.Pool(int(OPTIONS.pool))

    PCLI = PhEDEx()

    PNNS = PCLI.pnns(select=OPTIONS.pnn)

    TIMING = {}

    WILDCARD = re.compile(r'\S*[*]\S*')

    DATASETS = get_timing(
        _get_dset_list(PCLI, OPTIONS.dataset),
        TIMING
    )

    PROCS = get_timing(
        _launch_workers(PNNS, DATASETS, POOL, OPTIONS, PCLI),
        TIMING
    )

    get_timing(
        _get_workers(POOL, PROCS),
        TIMING
    )

    logging.summary('Final Stats: n.pnns: %d, n.datasets: %d, poolsize: %d, timing: %s',
                    len(PNNS), len(DATASETS), int(OPTIONS.pool), TIMING)
