#! /bin/env python
"""
Command line tool for registering a CMS dataset into rucio
"""

from __future__ import absolute_import, division, print_function

import os
import zlib
from argparse import ArgumentParser

import rucio.rse.rsemanager as rsemgr
from CMSRucio import CMSRucio


class CRABDatasetInjector(CMSRucio):
    """
    General Class for injecting a cms dataset in rucio
    """

    def __init__(self, replica, local_file, source_site, dest_site, pfn, rse=None, scope="",
                 uuid=None, check=True, lifetime=None, dry_run=None, account=None):

        super(CRABDatasetInjector, self).__init__(account=account, auth_type=None, scope=scope, dry_run=dry_run)
        self.replica = replica
        self.dest_site = dest_site
        self.source_site = source_site
        self.local_file = local_file
        self.pfn = pfn
        self.uuid = uuid
        self.check = check
        self.lifetime = lifetime

    def upload_file(self, size, checksum):
        """[summary]

        """
        print("Uploading {0} ({1})".format(self.replica, self.pfn))
        self.upload([self.local_file], self.source_site, pfns=[self.pfn])

        print("{0} uploaded".format(self.replica))

        # REPLICA = [{
        #     'scope': self.scope,
        #     'name' : self.replica,
        #     'adler32': checksum,
        #     'bytes': size
        #     'pfn': URL
        # }]

        self.register_temp_replicas(self.source_site,
                                    [self.replica],
                                    [self.pfn],
                                    [size],
                                    [checksum])

        self.add_rule([self.replica], self.dest_site, "")


if __name__ == "__main__":

    parser = ArgumentParser(description='Arguments for file Rucio upload')
    parser.add_argument('file', type=str, help='local file path')
    parser.add_argument('replica', type=str, help='Rucio replica name')

    parser.add_argument('temp', type=str, help='Rucio source temp RSE')
    parser.add_argument('dest', type=str, help='Rucio destination RSE')
    parser.add_argument('--pfn', type=str, help='Source rse pfn')

    parser.add_argument('--account', type=str, help='Rucio account')
    parser.add_argument('--scope', type=str, help='Rucio scope')

    args = parser.parse_args()

    # TODO: get pfn from CRIC

    crabInj = CRABDatasetInjector(args.replica,
				  args.file,
				  args.temp,
				  args.dest,
				  args.pfn,
				  account=args.account,
				  scope=args.scope
                                 )

    checksum = None

    with open(args.file) as f:
        data = f.read()
        checksum = zlib.adler32(data)

    file_dict = {
        "size": os.path.getsize(args.file),
        "checksum": "%08x" % checksum
    }

    crabInj.upload_file(file_dict["size"], file_dict["checksum"])
