#!/usr/bin/env bash
set -e

source /cvmfs/cms.cern.ch/rucio/setup.sh
# this may need changing
export RUCIO_HOME=/data/rucio-int
export RUCIO_ACCOUNT=root

rucio-admin rse add T3_US_SDSC
rucio-admin rse set-attribute --rse T3_US_SDSC --key lfn2pfn_algorithm --value identity
rucio-admin rse add-protocol --hostname gordon-dm.sdsc.edu --scheme gsiftp --prefix /simons/scratch/nsmith1/rucio T3_US_SDSC --domain-json '{"wan": {"read": 1, "write": 1, "delete": 1, "third_party_copy": 1}}'
rucio-admin rse set-attribute --rse T3_US_SDSC --key fts --value https://cmsfts3.fnal.gov:8446
rucio-admin rse set-attribute --rse T3_US_SDSC --key reaper --value True
rucio-admin account set-limits transfer_ops T3_US_SDSC 100TB

rucio list-rses --expression "rse_type=DISK&cms_type=real&tier=1" | while read rse
do
  rucio-admin rse add-distance --distance 3 $rse T3_US_SDSC
done

rucio list-rses --expression "rse_type=DISK&cms_type=real&tier=2" | while read rse
do
  rucio-admin rse add-distance --distance 4 $rse T3_US_SDSC
done
