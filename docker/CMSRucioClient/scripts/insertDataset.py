#! /bin/env python
"""
Command line tool for registering a CMS dataset into rucio
"""

from __future__ import absolute_import, division, print_function

import json
from subprocess import Popen, PIPE
import re
from argparse import ArgumentParser

from gfal2 import Gfal2Context, GError
import rucio.rse.rsemanager as rsemgr
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient
from rucio.common.exception import DataIdentifierAlreadyExists
from rucio.common.exception import RucioException
from rucio.common.exception import FileAlreadyExists

DEFAULT_SCOPE = 'cms'

DEBUG_FLAG = False


def das_go_client(query):
    """
    just wrapping the dasgoclient command line
    """
    proc = Popen(['dasgoclient', '-query=%s' % query, '-json'], stdout=PIPE)
    output = proc.communicate()[0]
    if DEBUG_FLAG:
        print('DEBUG:' + output)
    return json.loads(output)


class DatasetInjector(object):
    """
    General Class for injecting a cms dataset in rucio
    """

    def __init__(self, dataset, site, rse=None, scope=DEFAULT_SCOPE,
                 uuid=None, check=True, lifetime=None, dry_run=False):
        self.dataset = dataset
        self.site = site
        if rse is None:
            rse = site
        self.rse = rse
        self.scope = scope
        self.uuid = uuid
        self.check = check
        self.lifetime = lifetime
        self.dry_run = dry_run

        self.blocks = []
        self.url = ''

        self.getmetadata()
        self.get_global_url()
        self.didc = DIDClient()
        self.repc = ReplicaClient()

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

    def getmetadata(self):
        """
        Gets the list of blocks at a site, their files and their metadata
        """
        print("Initializing... getting the list of blocks and files")
        blocks = das_go_client("block dataset=%s site=%s system=phedex"
                               % (self.dataset, self.site))
        for item in blocks:
            uuid = item['block'][0]['name'].split('#')[1]
            if (self.uuid is None) or (uuid == self.uuid):
                block = {'name': item['block'][0]['name'], 'files': []}
                files = das_go_client("file block=%s site=%s system=phedex"
                                      % (block['name'], self.site))
                for item2 in files:
                    cksum = re.match(r"adler32:([^,]+)", item2['file'][0]['checksum'])
                    cksum = cksum.group(0).split(':')[1]
                    cksum = "{0:0{1}x}".format(int(cksum, 16), 8)
                    block['files'].append({
                        'name': item2['file'][0]['name'],
                        'checksum': cksum,
                        'size': item2['file'][0]['size']
                    })
                self.blocks.append(block)
        print("Initalization done.")

    def register(self):
        """
        Create the container, the  datasets and attach them to the container.
        """
        print("Registering...")
        self.register_container()
        for block in self.blocks:
            self.register_dataset(block['name'])

            self.register_replicas(block['files'])
            self.attach_files([filemd['name'] for filemd in block['files']], block['name'])
            # for filemd in block['files']:
            #     self.register_replica(filemd)
            #     self.attach_file(filemd['name'], block['name'])
        print("All datasets, blocks and files registered")

    def register_container(self):
        """
        Create the container.
        """

        print("registering container %s" % self.dataset)
        if self.dry_run:
            print(' Dry run only. Not creating container.')
            return

        try:
            self.didc.add_container(scope=self.scope, name=self.dataset, lifetime=self.lifetime)
        except DataIdentifierAlreadyExists:
            print(" Container %s already exists" % self.dataset)

    def register_dataset(self, block):
        """
        Create the dataset and attach them to teh container
        """

        if self.dry_run:
            print(' Dry run only. Not creating dataset.')
            return

        try:
            self.didc.add_dataset(scope=self.scope, name=block, lifetime=self.lifetime)
            print("registered dataset %s" % block)
        except DataIdentifierAlreadyExists:
            pass

        try:
            self.didc.attach_dids(scope=self.scope, name=self.dataset,
                                  dids=[{'scope': self.scope, 'name': block}])
            print("attaching dataset %s to container %s" % (block, self.dataset))
        except RucioException:
            pass

    def attach_file(self, lfn, block):
        """
        Attach the file to the container
        """

        if self.dry_run:
            print(' Dry run only. Not attaching files.')
            return

        try:
            print("attaching file %s" % lfn)
            self.didc.attach_dids(scope=self.scope, name=block,
                                  dids=[{'scope': self.scope, 'name': lfn}])
        except FileAlreadyExists:
            print("File already attached")

    def attach_files(self, lfns, block):
        """
        Attach the file to the container
        """
        if not lfns:
            print('lfns is empty for %s. Moving on.' % block)
            return
        if self.dry_run:
            print(' Dry run only. Not attaching files.')
            return

        try:
            self.didc.attach_dids(scope=self.scope, name=block,
                                  dids=[{'scope': self.scope, 'name': lfn} for lfn in lfns])
            print("attached %s files to %s" % (len(lfns), block))
        except FileAlreadyExists:
            pass

    def register_replica(self, filemd):
        """
        Register file replica.
        """
        print("registering file %s" % filemd['name'])

        if self.dry_run:
            print(' Dry run only. Not registering files.')
            return

        if self.check:
            self.check_storage(filemd)
        if not self.check_replica(filemd['name']):
            import pdb
            # pdb.set_trace()
            self.repc.add_replicas(rse=self.rse, files=[{
                'scope': self.scope,
                'name': filemd['name'],
                'adler32': filemd['checksum'],
                'bytes': filemd['size'],
                # 'pfn': self.get_file_url(filemd['name'])
            }])

    def register_replicas(self, replicas):
        """
        Register file replica.
        """
        if not replicas:
            return

        print("registering %s files in Rucio" % len([filemd['name'] for filemd in replicas]))

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

    def check_replica(self, lfn):
        """
        Check if a replica of the given file at the site already exists.
        """
        print("checking if file %s with scope %s has already a replica at %s"
              % (lfn, self.scope, self.rse))
        replicas = list(self.repc.list_replicas([{'scope': self.scope, 'name': lfn}]))
        if replicas:
            replicas = replicas[0]
            if 'rses' in replicas:
                if self.rse in replicas['rses']:
                    print("file %s with scope %s has already a replica at %s"
                          % (lfn, self.scope, self.rse))
                    return True
        print("no existing replicas")
        return False


def main():
    """
    Main function.
    """
    parser = ArgumentParser(description="insert a dataset, all \
                             its blocks and files and all the replicas at a give site")
    parser.add_argument('--scope', dest='scope', help='scope of the dataset (default %s).'
                        % DEFAULT_SCOPE, default=DEFAULT_SCOPE)
    parser.add_argument('--dataset', dest='dataset', help='dataset name.', required=True)
    parser.add_argument('--site', dest='site', help='CMS site name.', required=True)
    parser.add_argument('--rse', dest='rse', help='RSE name, default is CMS Site name.')
    parser.add_argument('--uuid', dest='uuid', help='block UUID (default none).')
    parser.add_argument('--nocheck', dest='check', action='store_false',
                        help='do not check size and checksum of files replicas on storage.')
    parser.add_argument('--dryrun', dest='dry_run', action='store_true',
                        help='do not change anything in rucio, checking only')

    options = parser.parse_args()

    i = DatasetInjector(
        dataset=options.dataset,
        site=options.site,
        rse=options.rse,
        scope=options.scope,
        uuid=options.uuid,
        check=options.check,
        dry_run=options.dry_run,
    )
    i.register()


if __name__ == '__main__':
    main()
