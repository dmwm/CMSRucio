#! /bin/env python
"""
Command line tool for registering a CMS dataset into rucio
"""

from __future__ import absolute_import, division, print_function

import re
from argparse import ArgumentParser

import rucio.rse.rsemanager as rsemgr
from CMSRucio import CMSRucio, das_go_client
from gfal2 import GError, Gfal2Context
from rucio.common.exception import FileAlreadyExists

DEFAULT_SCOPE = 'cms'

DEBUG_FLAG = False


class DatasetInjector(CMSRucio):
    """
    General Class for injecting a cms dataset in rucio
    """

    def __init__(self, dataset, site, rse=None, scope=DEFAULT_SCOPE,
                 uuid=None, check=True, lifetime=None, dry_run=False):

        super(DatasetInjector, self).__init__(account=None, auth_type=None, scope=scope, dry_run=dry_run)

        self.dataset = dataset
        self.site = site
        if rse is None:
            rse = site
        self.rse = rse
        self.uuid = uuid
        self.check = check
        self.lifetime = lifetime

        self.blocks = []
        self.url = ''

        self.getmetadata()
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
        self.register_container(dataset=self.dataset, lifetime=self.lifetime)
        for block in self.blocks:
            self.register_dataset(block['name'], dataset=self.dataset, lifetime=self.lifetime)

            self.register_replicas(rse=self.rse, replicas=block['files'])
            self.attach_files([filemd['name'] for filemd in block['files']], block['name'])
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

    def check_replica(self, lfn):
        """
        Check if a replica of the given file at the site already exists.
        """
        print("checking if file %s with scope %s has already a replica at %s"
              % (lfn, self.scope, self.rse))
        replicas = list(self.rc.list_replicas([{'scope': self.scope, 'name': lfn}]))
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
