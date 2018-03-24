#! /bin/env python
"""
Command line tool for registering a CMS dataset into rucio
"""

from __future__ import absolute_import, division, print_function

import multiprocessing
import re
from argparse import ArgumentParser

import datetime

import rucio.rse.rsemanager as rsemgr
from CMSRucio import CMSRucio, DEFAULT_DASGOCLIENT, das_go_client, datasvc_client
from gfal2 import GError, Gfal2Context

BLOCKREPLICAS_URL = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas"
DEBUG_FLAG = False
DEFAULT_SCOPE = 'cms'
DEFAULT_LIMIT = 10


class DatasetSync(CMSRucio):
    """
    Synchronize the replica of a given dataset at a PhEDEx site to
    the corresponding Rucio site.
    """

    def __init__(self, dataset, pnn, rse=None, scope=DEFAULT_SCOPE,
                 check=True, lifetime=None, dry_run=False, dasgoclient=DEFAULT_DASGOCLIENT):
        """
           :param dataset: Name of the PhEDEx dataset to synchronize with Rucio.
           :param pnn: PhEDEx node name to filter on for replica information.
        """

        super(DatasetSync, self).__init__(account=None, auth_type=None, scope=scope, dry_run=dry_run)

	self.dataset = dataset
        self.phedex_dataset = dataset
        self.pnn = pnn
        if rse is None:
            rse = pnn
        self.rse = rse
        self.check = check
        self.lifetime = lifetime
        self.rucio_datasets = {}
        self.url = ''
        self.blocks = self.get_phedex_metadata(dataset=self.phedex_dataset, pnn=self.pnn)
        self.get_rucio_metadata()
        self.get_global_url()

        self.gfal = Gfal2Context()

    def get_file_url(self, lfn):
        """
        Return the rucio url of a file.
        """
        return self.url + '/' + lfn

    def get_global_url(self):
        """
        Return the base path of the rucio url
        """
        print("Getting parameters for rse %s" % self.rse)
        rse = rsemgr.get_rse_info(self.rse)
        proto = rse['protocols'][0]

        schema = proto['scheme']
        prefix = proto['prefix'] + '/' + self.scope.replace('.', '/')
        if schema == 'srm':
            prefix = proto['extended_attributes']['web_service_path'] + prefix
        url = schema + '://' + proto['hostname']
        if proto['port'] != 0:
            url = url + ':' + str(proto['port'])
        self.url = url + prefix
        print("Determined base url %s" % self.url)

    def get_rucio_metadata(self):
        """
        Gets the list of datasets at the Rucio RSE, the files, and the metadata.
        """
        print("Initializing Rucio... getting the list of blocks and files at %s"
              % self.rse)
        replica_info = self.rc.list_replicas([{"scope": self.scope,
                                                 "name": self.phedex_dataset}],
                                               rse_expression="rse=%s" % self.rse)
        replica_files = set()
        for file_info in replica_info:
            name = file_info['name']
            if self.rse in file_info['rses']:
                replica_files.add(name)
        for dataset in self.didc.list_content(self.scope, self.phedex_dataset):
            dataset_summary = {}
            for file_info in self.didc.list_content(self.scope, dataset['name']):
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

    def check_storage(self, filemd):
        """
        Check size and checksum of a file on storage
        """
        url = self.get_file_url(filemd['name'])
        print("checking url %s" % url)
        try:
            size = self.gfal.stat(str(url)).st_size
            checksum = self.gfal.checksum(str(url), 'adler32')
            print("got size and checksum of file: pfn=%s size=%s checksum=%s"
                  % (url, size, checksum))
        except GError:
            print("no file found at %s" % url)
            return False
        if str(size) != str(filemd['size']):
            print("wrong size for file %s. Expected %s got %s"
                  % (filemd['name'], filemd['size'], size))
            return False
        if str(checksum) != str(filemd['checksum']):
            print("wrong checksum for file %s. Expected %s git %s"
                  % (filemd['name'], filemd['checksum'], checksum))
            return False
        print("size and checksum are ok")
        return True


def get_node_datasets(node, dasgoclient):
    """
    Given a PhEDEx Node Name, return a list of datasets which
    have some fileblocks at the site according to DAS.
    """
    das_datasets = das_go_client("dataset site=%s system=phedex" % node, dasgoclient)
    for das_dataset in das_datasets:
        yield das_dataset['dataset'][0]['name']

def get_deleted_datasets(node, delreqdays):
    """
    Given a PhEDEx Node Name, return the list of datasets concerned
    by deletion requests in the last delreqdays days
    """
    date = (datetime.datetime.now() - datetime.timedelta(days=int(delreqdays))).strftime("%Y-%m-%d")
    delreq_datasets = datasvc_client('deleterequests', {'node': node, 'approval': 'approved', 'create_since': date})
    datasets = []
    for req in delreq_datasets['phedex']['request']:
        for block in req['data']['dbs']['block']:
            datasets.append(block['name'].split('#')[0])

    for dataset in set(datasets):
        yield dataset

def sync_one_dataset(dataset, site, rse, scope, check, dry_run, dasgoclient):
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
        dasgoclient=dasgoclient,
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
    parser.add_argument('--limit', dest='limit', default=DEFAULT_LIMIT, type=int,
                        help="limit on the number of datasets to attempt sync. default %s. -1 for unlimited" % DEFAULT_LIMIT)
    parser.add_argument('--pool', dest='pool', default=5, type=int,
                        help="number of helper processes to use.")
    parser.add_argument('--dataset', dest='dataset', action='append',
                        help='specific datasets to sync')
    parser.add_argument('--dasgoclient', dest='dasgoclient', default=DEFAULT_DASGOCLIENT,
                        help='full path to the dasgoclient (default %s).' % DEFAULT_DASGOCLIENT)
    parser.add_argument('--delreqdays', dest='delreqdays', default=0,
                        help='looking at the deletion requests since DELREQDAYS days\
                             (default 0, don\'t use deletion requests to check files).')



    options = parser.parse_args()

    pool = multiprocessing.Pool(options.pool)

    datasets = options.dataset
    limit = options.limit
    count = 0
    futures = []

    # checking the datasets which are in deletion requetss
    if options.delreqdays > 0:
        for dataset in get_deleted_datasets(options.site,  options.delreqdays):
           future = pool.apply_async(sync_one_dataset, (dataset, options.site, options.rse,
                                  options.scope, options.check, options.dry_run, options.dasgoclient))
           futures.append((dataset, future))

    # checking the datasets which are at the site
    if not datasets:
        datasets = get_node_datasets(options.site, options.dasgoclient)
    for dataset in datasets:
        count += 1
        if limit > 0 and count >= limit:
            break
        future = pool.apply_async(sync_one_dataset, (dataset, options.site, options.rse,
                                  options.scope, options.check, options.dry_run, options.dasgoclient))
        futures.append((dataset, future))
    pool.close()

    for dataset, future in futures:
        future.get()
        print("Finished processing dataset %s" % dataset)

if __name__ == '__main__':
    main()
