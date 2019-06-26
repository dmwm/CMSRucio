#! /bin/env python
"""
Command line tool for registering a CMS dataset into rucio
"""

from __future__ import absolute_import, division, print_function

import re
from argparse import ArgumentParser

import rucio.rse.rsemanager as rsemgr
from CMSRucio import CMSRucio


class CRABDatasetInjector(CMSRucio):
    """
    General Class for injecting a cms dataset in rucio
    """

    def __init__(self, replica, source_site, dest_site, rse=None, scope="",
                 uuid=None, check=True, lifetime=None, dry_run=None, account=None):

        super(CRABDatasetInjector, self).__init__(account=account, auth_type=None, scope=scope, dry_run=dry_run)
        self.replica = replica
        self.dest_site = dest_site
        self.source = source_site
        self.uuid = uuid
        self.check = check
        self.lifetime = lifetime

    def upload_file(self, size, checksum):
        """[summary]

        """
        self.upload(self.replica, self.source_site)

        REPLICA = [{
            'scope': self.scope,
            'name' : self.replica,
            #'adler32': checksum,
            'bytes': size
            #'pfn': URL
        }]

        self.register_replicas(self.source_site, REPLICA) #register_temp_replicas(self.source_site, self.replica, pfn, sizes, checksums)

        self.add_rule([self.replica], self.dest_site, "")

if __name__ == "__main__":

    crabInj = CRABDatasetInjector("replica.txt", "T2_IT_Pisa_Temp", "T2_IT_Pisa", account="dciangot")


    file_dict = {
        "size": 0,
        "checksum": "asd235fs"
    }

    crabInj.upload_file(file_dict["size"], file_dict["checksums"])

