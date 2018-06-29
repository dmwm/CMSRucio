#! /bin/env python
"""
Command line tool for registering a CMS dataset into rucio
"""

from __future__ import absolute_import, division, print_function

import sys
import re
import multiprocessing
from argparse import ArgumentParser
import json
import datetime

from CMSRucio import (CMSRucio, DEFAULT_DASGOCLIENT, das_go_client,
                      datasvc_client, get_subscriptions, get_phedex_tfc)

BLOCKREPLICAS_URL = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas"
DEBUG_FLAG = False
DEFAULT_SCOPE = 'cms'
DEFAULT_LIMIT = 10
DEFAULT_POOL_SIZE = 5
DEFAULT_TFC_PROTOCOL = 'srmv2'

class NodeSync(CMSRucio):
    """
    Synchronize PhEDEx Site attributes with RSE attributes.
    Currently dealing only with the tfc
    """

    def __init__(self, pnn, rse=None, dry_run=False):
        """
        """

        super(NodeSync, self).__init__(account=None, auth_type=None,
                                       scope=None, dry_run=dry_run,
                                       das_go_path=DEFAULT_DASGOCLIENT)

        self.pnn = pnn
        self.rse = rse

        self.get_phedex_tfc()

        # for the moment assuming we have only 1 protocol
        self.protocol = self.cli.get_protocols(rse=self.rse)[0]
        self.get_rucio_tfc()

    def get_rucio_tfc(self):
        """
        Get the tfc rules from the extended_attributes of the rse protocol.
        """
        if 'extended_attributes' in self.protocol:
            if 'tfc' in self.protocol['extended_attributes']:
                self.rucio_tfc = self.protocol['extended_attributes']['tfc']
                return
        self.rucio_tfc = {}

    def get_phedex_tfc(self):
        """
        Get the relevant lines of the pnn tfc.
        """
        full = get_phedex_tfc(self.pnn)

        protos = [DEFAULT_TFC_PROTOCOL]
        num_protos = 0

        while len(protos) != num_protos:
            num_protos = len(protos)
            for rule in full:
                if rule['element_name'] == 'lfn-to-pfn':
                    if ('chain' in rule) and (rule['chain'] not in protos):
                        protos.append(rule['chain'])

        self.phedex_tfc = []

        for rule in full:
            if rule['element_name'] == 'lfn-to-pfn' and rule['protocol'] in protos:
                self.phedex_tfc.append(rule)

    def sync(self):
        """
        Syncronize the tfc rules in rucio.
        """
        if json.dumps(self.rucio_tfc) != json.dumps(self.phedex_tfc):
            print('Rucio and PhEDEx TFC differ')
            if self.dry_run:
                print('Dry run, not modyfing the protocol')
                return
            ext = {}
            if 'extended_attributes' in self.protocol:
                ext = self.protocol['extended_attributes']
            ext['tfc'] = self.phedex_tfc
            ext['tfc_proto'] = DEFAULT_TFC_PROTOCOL

            self.protocol['extended_attributes'] = ext

            self.cli.delete_protocols(rse=self.rse, scheme=self.protocol['scheme'])

            self.cli.add_protocol(rse=self.rse, params=self.protocol)

            #self.cli.update_protocols(
            #    rse=self.rse,
            #    scheme=self.protocol['scheme'],
            #    data=self.protocol
            #)

class DatasetSync(CMSRucio):
    """
    Synchronize the replica of a given dataset at a PhEDEx site to
    the corresponding Rucio site.
    """

    def __init__(self, dataset, pnn, rse=None, scope=DEFAULT_SCOPE,
                 check=True, lifetime=None, dry_run=False, syncrules=False,
                 das_go_path=DEFAULT_DASGOCLIENT):
        """
           :param dataset: Name of the PhEDEx dataset to synchronize with Rucio.
           :param pnn: PhEDEx node name to filter on for replica information.
        """

        super(DatasetSync, self).__init__(account=None, auth_type=None,
                                          scope=scope, dry_run=dry_run,
                                          das_go_path=das_go_path, check=check)

        self.dataset = dataset
        self.phedex_dataset = dataset
        self.pnn = pnn
        if rse is None:
            rse = pnn
        self.rse = rse
        self.check = check
        self.syncrules = syncrules

        self.lifetime = lifetime
        self.rucio_datasets = {}
        self.blocks = self.get_phedex_metadata(dataset=self.phedex_dataset, pnn=self.pnn)

        if self.syncrules:
            if len(self.blocks) > 0:
                self.subscriptions = get_subscriptions(dataset=self.phedex_dataset, pnn=self.pnn)
            else:
                self.subscriptions = {}

        self.get_rucio_metadata()
        if self.syncrules:
            self.get_sync_rules()

    def get_rucio_metadata(self):
        """
        Gets the list of datasets at the Rucio RSE, the files, and the metadata.
        """
        print("Initializing Rucio... getting the list of blocks and files at %s for dataset %s"
              % (self.rse, self.phedex_dataset))
        replica_info = self.cli.list_replicas([{"scope": self.scope,
                                                "name": self.phedex_dataset}],
                                              rse_expression="rse=%s" % self.rse)
        replica_files = set()
        for file_info in replica_info:
            name = file_info['name']
            if self.rse in file_info['rses']:
                replica_files.add(name)
        for dataset in self.cli.list_content(self.scope, self.phedex_dataset):
            dataset_summary = {}
            for file_info in self.cli.list_content(self.scope, dataset['name']):
                file_summary = {'name': file_info['name'],
                                'adler32': file_info['adler32'],
                                'size': file_info['bytes']
                               }
                if file_info['name'] in replica_files:
                    dataset_summary[file_info['name']] = file_summary
            self.rucio_datasets[dataset['name']] = dataset_summary
        print("Rucio initialization done.")

    def register(self):
        """
        Create the container, the datasets and attach them to the container.
        """
        print("Registering %s ..." % self.dataset)
        if len(self.blocks) > 0:
            self.register_container(dataset=self.dataset, lifetime=self.lifetime)
        # First, iterate through all the known PhEDEx blocks.
        for block_name, block_info in self.blocks.items():
            if block_name not in self.rucio_datasets:
                self.register_dataset(block_name, dataset=self.dataset, lifetime=self.lifetime)
            rucio_dataset_info = self.rucio_datasets.setdefault(block_name, {})
            files_to_attach = []
            for filename, filemd in block_info.items():
                if filename not in rucio_dataset_info:
                    files_to_attach.append(filemd)
            self.register_replicas(rse=self.rse, replicas=files_to_attach)
            self.attach_files([i['name'] for i in files_to_attach], block_name)
        # We do not detach files, but perhaps remove the replicas.
        for dataset_name, dataset_info in self.rucio_datasets.items():
            block_info = self.blocks.setdefault(dataset_name, {})
            replicas_to_delete = []
            for filename, filemd in dataset_info.items():
                if filename not in block_info:
                    replicas_to_delete.append(filemd)
            self.delete_replicas(rse=self.rse, replicas=replicas_to_delete)
        print("All datasets, blocks and files registered")
        if self.syncrules:
            self.update_rules()

    def get_sync_rules(self):
        """
        Get the rules generated by the syncScript for the datasets and rse
        It only takes into account the rules with comment in the form
        "{'type': 'phedex_sync', ....}"
        so that the sync scripts only manages its own rules.
        """
        self.rules = {}
        for dsname in self.rucio_datasets.keys():
            print("Getting rucio rule for dataset %s" % dsname)
            rules = self.cli.list_did_rules(scope=self.scope, name=dsname)
            for rule in rules:
                try:
                    rulemd = json.loads(rule['comments'])
                except (TypeError, ValueError):
                    print("Missing Comments, skipping")
                    continue

                rse_exp = 'rse=' + self.rse
                if rule['rse_expression'] != rse_exp:
                    continue

                if rulemd['type'] != 'phedex_sync':
                    continue
                self.rules[dsname] = rule

    def update_rules(self):
        """
        Synchronize rules and subscriptions
        """
        print("Checking existing rules and subscription for (phedex) dataset %s " %
              self.phedex_dataset)

        for block, sub in self.subscriptions.items():
            if 'request' not in sub:
                print("subscription for block %s missing 'request' field" % block)
                sub['request'] = 'unknown'

            if 'group' not in sub:
                print("subscription for block %s missing 'group' field" % block)
                sub['group'] = 'unknown'

            submd = json.dumps({'type': 'phedex_sync', 'rid': sub['request'],
                                'group': sub['group']})

            if block not in self.rules:
                print("No rule found for %s, creating one" % block)
                self.add_rule(names=[block], rse_exp='rse='+self.rse,
                              comment=submd)
            # For the moment ignoring this: ISSUE ..
            #elif submd != self.rules[block]['comments']:
            #    print("Rule for %s has wrong comment, re-creating" % block)
            #    self.del_rule(self.rules[block]['id'])
            #    self.add_rule(names=[block], rse_exp='rse='+self.rse,
            #                  comment=submd)
            elif self.cli.account != self.rules[block]['account']:
                print("Rule for %s belongs to the wrong account, modifying" % block)
                self.update_rule(self.rules[block]['id'], {'account': self.account})

        for block, rule in self.rules.items():
            if block not in self.subscriptions:
                print("Rule for %s correspond to no subscription, deleting" % block)
                self.del_rule(rule['id'])


def get_node_datasets(node, dasgoclient):
    """
    Given a PhEDEx Node Name, return a list of datasets which
    have some fileblocks at the site according to DAS.
    """
    print("Get datasets at node %s" % node)
    das_datasets = das_go_client("dataset site=%s system=phedex" % node, dasgoclient)
    datasets = []
    for das_dataset in das_datasets:
        datasets.append(das_dataset['dataset'][0]['name'])

    return datasets

def get_transferred_datasets(node, subdays):
    """
    Given a PhEDEx Node Name, return the list of datasets transfered
    by subscriptions created during the last subdays days
    """

    print("Gets datasets transferred at %s in the last %d days" % (node, int(subdays)))

    date = (datetime.datetime.now() - datetime.timedelta(days=int(subdays))).strftime("%Y-%m-%d")
    blocks = get_subscriptions(node, since=date).keys()

    return list(set([bl.split('#')[0] for bl in blocks]))


def get_deleted_datasets(node, delreqdays):
    """
    Given a PhEDEx Node Name, return the list of datasets concerned
    by deletion requests in the last delreqdays days
    """
    print("Gets datasets deleted at %s in the last %d days" % (node, int(delreqdays)))

    date = (datetime.datetime.now() - datetime.timedelta(days=int(delreqdays))).strftime("%Y-%m-%d")
    delreq_datasets = datasvc_client('deleterequests', {'node': node,
                                                        'approval': 'approved',
                                                        'create_since': date})
    datasets = []
    for req in delreq_datasets['phedex']['request']:
        for block in req['data']['dbs']['block']:
            datasets.append(block['name'].split('#')[0])

    return list(set(datasets))

def sync_one_dataset(dataset, site, rse, scope, check, dry_run, syncrules, dasgoclient):
    """
    Helper function for DatasetSync
    """
    instance = DatasetSync(
        dataset=dataset,
        pnn=site,
        rse=rse,
        scope=scope,
        check=check,
        dry_run=dry_run,
        syncrules=syncrules,
        das_go_path=dasgoclient,
    )
    instance.register()


def main():
    """
    Main function.
    """
    parser = ArgumentParser(description="Given a PhEDEx node name, "
                            "synchronize the locality information between PhEDEx and Rucio.")
    parser.add_argument('--scope', dest='scope', help='scope of the dataset (default %s).'
                        % DEFAULT_SCOPE, default=DEFAULT_SCOPE)
    parser.add_argument('--site', dest='site', help='CMS site name.', required=True)
    parser.add_argument('--rse', dest='rse', help='RSE name, default is CMS Site name.')
    parser.add_argument('--nocheck', dest='check', action='store_false',
                        help='do not check size and checksum of files replicas on storage.')
    parser.add_argument('--dryrun', dest='dry_run', action='store_true',
                        help='do not change anything in rucio, checking only')
    parser.add_argument('--syncrules', dest='syncrules', action='store_true',
                        help='syncronize rules and subscriptions.')
    parser.add_argument('--synctfc', dest='synctfc', action='store_true',
                        help='syncronize TFC into RSE attributes.')
    parser.add_argument('--fullsync', dest='fullsync', action='store_true',
                        help='synchronize all the phedex datasets at node')
    parser.add_argument('--limit', dest='limit', default=DEFAULT_LIMIT, type=int,
                        help="limit on the number of datasets to attempt sync. \
                             Default %s. -1 for unlimited" % DEFAULT_LIMIT)
    parser.add_argument('--pool', dest='pool', default=DEFAULT_POOL_SIZE, type=int,
                        help="number of helper processes to use (default %s)" % DEFAULT_POOL_SIZE)
    parser.add_argument('--dataset', dest='dataset', action='append',
                        help='specific datasets to sync')
    parser.add_argument('--dasgoclient', dest='dasgoclient', default=DEFAULT_DASGOCLIENT,
                        help='full path to the dasgoclient (default %s).' % DEFAULT_DASGOCLIENT)
    parser.add_argument('--delreqdays', dest='delreqdays', default=0,
                        help='looking at the deletion requests since DELREQDAYS days\
                             (default 0, don\'t use deletion requests to check files).')
    parser.add_argument('--subdays', dest='subdays', default=0,
                        help='looking at the subscriptions since SUBDAYS days\
                             (default 0).')
    parser.add_argument('--debug', dest='debug', action='store_true',
                        help='run in debug mode')
    parser.add_argument('--filter', dest='filter',
                        help='regex filtering the datasets name')

    options = parser.parse_args()

    datasets = options.dataset
    limit = options.limit
    futures = []

    if options.synctfc:
        NodeSync(options.site, options.rse, options.dry_run).sync()


    if not datasets:
        if options.fullsync:
            # Get all datasets at the site
            datasets = get_node_datasets(options.site, options.dasgoclient)
        elif int(options.subdays) > 0:
            # ... or only those requested in the last n days
            datasets = get_transferred_datasets(options.site, options.subdays)
        else:
            datasets = []

    sys.stdout.flush()

    # checking the datasets which are in deletion requests
    if int(options.delreqdays) > 0:
        datasets.extend(get_deleted_datasets(options.site, options.delreqdays))

    # when running in debug mode
    # order datasets.
    if options.debug:
        print('Sorting datasets list...')
        datasets.sort(key=lambda s: s.lower())

    # apply the filter, if any
    if options.filter:
        regex = re.compile(options.filter)
        datasets = [d for d in datasets if regex.match(d)]

    pool = multiprocessing.Pool(options.pool)

    done = []

    for dataset in datasets:
        if dataset in done:
            continue
        done.append(dataset)
        if limit >= 0 and len(done) > limit:
            break

        # in debug mode run single-threaded
        if options.debug:
            sync_one_dataset(dataset, options.site, options.rse, options.scope,
                             options.check, options.dry_run, options.syncrules, options.dasgoclient)
            print("Finished processing dataset %s" % dataset)
        else:
            future = pool.apply_async(sync_one_dataset,
                                      (dataset, options.site, options.rse, options.scope,
                                       options.check, options.dry_run, options.syncrules,
                                       options.dasgoclient))
            futures.append((dataset, future))

    if not options.debug:
        pool.close()

        for dataset, future in futures:
            future.get()
            print("Finished processing dataset %s" % dataset)

if __name__ == '__main__':
    main()
