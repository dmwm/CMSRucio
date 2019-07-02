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
        self.source_site = source_site
        self.uuid = uuid
        self.check = check
        self.lifetime = lifetime

    def upload_file(self, size, checksum):
        """[summary]

        """
        print("Uploading {0}".format(self.replica))
        #self.upload([self.replica], self.source_site)

        print("{0} uploaded".format(self.replica))

        REPLICA = [{
            'scope': self.scope,
            'name' : self.replica,
            #'adler32': checksum,
            'bytes': size
            #'pfn': URL
        }]

        self.register_temp_replicas(self.source_site, ["/"+self.replica], ["srm://storm-se-01.ba.infn.it:8444/srm/managerv2?SFN=/cms/store/temp/dciangot/replica2.txt"+self.replica], [size], None) #, checksum)

        self.add_rule(["/"+self.replica], self.dest_site, "")

if __name__ == "__main__":

    crabInj = CRABDatasetInjector("replica4.txt", "T2_IT_Bari_Temp", "T2_IT_Pisa", account="dciangot", scope="user.dciangot")


    file_dict = {
        "size": 0,
        "checksum": "asd235fs"
    }

    crabInj.upload_file(file_dict["size"], file_dict["checksum"])

