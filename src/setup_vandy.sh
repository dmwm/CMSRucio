#!/usr/bin/env bash
set -e

source /cvmfs/cms.cern.ch/rucio/setup.sh
# this may need changing
export RUCIO_HOME=/data/rucio-int
export RUCIO_ACCOUNT=root

RSE=T3_US_Vanderbilt_Spark

rucio-admin rse add $RSE
rucio-admin rse set-attribute --rse $RSE --key lfn2pfn_algorithm --value identity
rucio-admin rse add-protocol --hostname gridftp-vanderbilt.sites.opensciencegrid.org --port 2814 --scheme gsiftp --prefix /store $RSE --domain-json '{"wan": {"read": 1, "write": 1, "delete": 1, "third_party_copy": 1}}'
rucio-admin rse set-attribute --rse $RSE --key fts --value https://cmsfts3.fnal.gov:8446
rucio-admin rse set-attribute --rse $RSE --key reaper --value True
rucio-admin account set-limits transfer_ops $RSE 300TB

rucio list-rses --expression "rse_type=DISK&cms_type=real&tier=1" | while read rse
do
  rucio-admin rse add-distance --distance 3 $rse $RSE
done

rucio list-rses --expression "rse_type=DISK&cms_type=real&tier=2" | while read rse
do
  rucio-admin rse add-distance --distance 4 $rse $RSE
done
