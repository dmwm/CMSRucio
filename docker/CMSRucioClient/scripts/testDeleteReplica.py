#! /bin/env python

from __future__ import absolute_import, division, print_function

from CMSRucio import CMSRucio

files = [
    '/store/mc/RunIISummer16MiniAODv2/BlackHole_BH1_MD-6000_MBH-11000_n-6_TuneCUETP8M1_13TeV-blackmax/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/70000/4C669DD3-31C2-E611-9636-0025904AC2C6.root',
]

RSE = 'T2_US_Nebraska'

if __name__ == '__main__':
    cms_rucio = CMSRucio(account='root', auth_type='x509_proxy')

    rse = RSE
    replicas = [{'name': filename} for filename in files]

    cms_rucio.delete_replicas(rse, replicas)
