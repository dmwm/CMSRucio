#! /bin/env python
"""
Command line tool for registering a CMS dataset into rucio
"""

from __future__ import absolute_import, division, print_function

import json
import multiprocessing
import re
from argparse import ArgumentParser
from subprocess import Popen, PIPE

from gfal2 import Gfal2Context, GError
import rucio.rse.rsemanager as rsemgr
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient
from rucio.common.exception import DataIdentifierAlreadyExists
from rucio.common.exception import RucioException
from rucio.common.exception import FileAlreadyExists

BLOCKREPLICAS_URL = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas"
DEBUG_FLAG = False
DEFAULT_SCOPE = 'cms'


def das_go_client(query):
    """
    just wrapping the dasgoclient command line
    """
    proc = Popen(['/cvmfs/cms.cern.ch/common/dasgoclient', '-query=%s' % query, '-json'], stdout=PIPE)
    output = proc.communicate()[0]
    if DEBUG_FLAG:
        print('DEBUG:' + output)
    return json.loads(output)


class DatasetSync(object):
    """
    Synchronize the replica of a given dataset at a PhEDEx site to
    the corresponding Rucio site.
    """

    def __init__(self, dataset, pnn, rse=None, scope=DEFAULT_SCOPE,
                 check=True, lifetime=None, dry_run=False):
        """
           :param dataset: Name of the PhEDEx dataset to synchronize with Rucio.
           :param pnn: PhEDEx node name to filter on for replica information.
        """
        self.phedex_dataset = dataset
        self.pnn = pnn 
        if rse is None:
            rse = pnn 
        self.rse = rse
        self.scope = scope
        self.check = check
        self.lifetime = lifetime
        self.dry_run = dry_run

        self.rucio_datasets = {}
        self.blocks = {}
        self.url = ''

        self.get_phedex_metadata()
        self.didc = DIDClient()
        self.repc = ReplicaClient()
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

    def get_phedex_metadata(self):
        """
        Gets the list of blocks at a PhEDEx site, their files and their metadata
        """
        print("Initializing... getting the list of blocks and files")
        blocks = das_go_client("block dataset=%s site=%s system=phedex"
                               % (self.phedex_dataset, self.pnn))
        for item in blocks:
            block_summary = {}
            block_name = item['block'][0]['name']
            files = das_go_client("file block=%s site=%s system=phedex"
                                  % (block_name, self.pnn))
            for item2 in files:
                cksum = re.match(r"adler32:([^,]+)", item2['file'][0]['checksum'])
                cksum = cksum.group(0).split(':')[1]
                cksum = "{0:0{1}x}".format(int(cksum, 16), 8)
                block_summary[item2['file'][0]['name']] = {
                    'name': item2['file'][0]['name'],
                    'checksum': cksum,
                    'size': item2['file'][0]['size']
                }
            self.blocks[block_name] = block_summary
        print("PhEDEx initalization done.")

    def get_rucio_metadata(self):
        """
        Gets the list of datasets at the Rucio RSE, the files, and the metadata.
        """
        print("Initializing Rucio... getting the list of blocks and files at %s"
              % self.rse)
        replica_info = self.repc.list_replicas([{"scope": self.scope,
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
        print("Registering...")
        self.register_container()
        # First, iterate through all the known PhEDEx blocks.
        for block_name, block_info in self.blocks.items():
            if block_name not in self.rucio_datasets:
                self.register_dataset(block_name)
            rucio_dataset_info = self.rucio_datasets.setdefault(block_name, {})
            files_to_attach = []
            for filename, filemd in block_info.items():
                if filename not in rucio_dataset_info:
                    files_to_attach.append(filemd)
            self.register_replicas(files_to_attach)
            self.attach_files([i['name'] for i in files_to_attach], block_name)
        # We do not detach files, but perhaps remove the replicas.
        for dataset_name, dataset_info in self.rucio_datasets.items():
            block_info = self.blocks.setdefault(dataset_name, {})
            replicas_to_delete = []
            for filename, filemd in dataset_info.items():
                if filename not in block_info:
                    replicas_to_delete.append(filemd)
            self.delete_replicas(replicas_to_delete)
        print("All datasets, blocks and files registered")

    def register_container(self):
        """
        Create the container.
        """

        print("registering container %s:%s" % (self.scope, self.phedex_dataset))
        if self.dry_run:
            print(' Dry run only. Not creating container.')
            return

        try:
            self.didc.add_container(scope=self.scope, name=self.phedex_dataset, lifetime=self.lifetime)
        except DataIdentifierAlreadyExists:
            print("Container %s already exists" % self.phedex_dataset)

    def register_dataset(self, block):
        """
        Create the dataset and attach them to the container
        """
        print("registering dataset %s" % block)

        if self.dry_run:
            print(' Dry run only. Not creating dataset.')
            return

        try:
            self.didc.add_dataset(scope=self.scope, name=block, lifetime=self.lifetime)
        except DataIdentifierAlreadyExists:
            print(" Dataset %s already exists" % block)

        try:
            print("attaching dataset %s to container %s" % (block, self.phedex_dataset))
            self.didc.attach_dids(scope=self.scope, name=self.phedex_dataset,
                                  dids=[{'scope': self.scope, 'name': block}])
        except RucioException:
            print(" Dataset already attached")

    def attach_files(self, lfns, block):
        """
        Attach the file to the container
        """
        if not lfns:
            return

        if self.dry_run:
            print(' Dry run only. Not attaching files.')
            return

        try:
            print("attaching files: %s" % ", ".join(lfns))
            self.didc.attach_dids(scope=self.scope, name=block,
                                  dids=[{'scope': self.scope, 'name': lfn} for lfn in lfns])
        except FileAlreadyExists:
            print("File already attached")

    def register_replicas(self, replicas):
        """
        Register file replica.
        """
        if not replicas:
            return

        print("registering files in Rucio: %s" % ", ".join([filemd['name'] for filemd in replicas]))

        if self.dry_run:
            print(' Dry run only. Not registering files.')
            return

        if self.check:
            for filemd in replicas:
                self.check_storage(filemd)

        self.repc.add_replicas(rse=self.rse, files=[{
                'scope': self.scope,
                'name': filemd['name'],
                'adler32': filemd['checksum'],
                'bytes': filemd['size'],
               } for filemd in replicas])

    def delete_replicas(self, replicas):
        """
        Delete replicas from the current RSE.
        """
        if not replicas:
            return

        print("Deleting files from %s in Rucio: %s" % (self.rse,
              ", ".join([filemd['name'] for filemd in replicas])))

        if self.dry_run:
            print(" Dry run only.  Not deleting replicas.")
            return

        self.repc.delete_replicas(rse=self.rse, files=[{
            'scope': self.scope,
            'name': filemd['name'],
           } for filemd in replicas])

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


def get_node_datasets(node):
    """
    Given a PhEDEx Node Name, return a list of datasets with some
    data present.
    """
    das_datasets = das_go_client("dataset site=%s system=phedex" % node)
    for das_dataset in das_datasets:
        yield das_dataset['dataset'][0]['name']


def sync_one_dataset(dataset, site, rse, scope, check, dry_run):
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
    parser.add_argument('--limit', dest='limit', default=10, type=int,
                        help="limit on the number of datasets to attempt sync")
    parser.add_argument('--pool', dest='pool', default=5, type=int,
                        help="number of helper processes to use.")

    options = parser.parse_args()

    pool = multiprocessing.Pool(options.pool)

    limit = options.limit
    count = 0
    futures = []
    for dataset in get_node_datasets(options.site):
        count += 1
        if limit > 0 and count >= limit:
            break
        future = pool.apply_async(sync_one_dataset, (dataset, options.site, options.rse,
                                  options.scope, options.check, options.dry_run))
        futures.append((dataset, future))
    pool.close()

    for dataset, future in futures:
        future.get()
        print("Finished processing dataset %s" % dataset)

if __name__ == '__main__':
    main()
