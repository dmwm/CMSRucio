#!/usr/bin/env bash
set -e

source /cvmfs/cms.cern.ch/rucio/setup.sh
# this may need changing
export RUCIO_HOME=/data/rucio-int
export RUCIO_ACCOUNT=root

rucio-admin rse add T3_US_NERSC
rucio-admin rse set-attribute --rse T3_US_NERSC --key lfn2pfn_algorithm --value identity
rucio-admin rse add-protocol --hostname dtn02.nersc.gov --scheme gsiftp --prefix /global/cscratch1/sd/uscms/rucio T3_US_NERSC --domain-json '{"wan": {"read": 1, "write": 1, "delete": 1, "third_party_copy": 1}}'
rucio-admin rse add-protocol --hostname dtn03.nersc.gov --scheme gsiftp --prefix /global/cscratch1/sd/uscms/rucio T3_US_NERSC --domain-json '{"wan": {"read": 1, "write": 1, "delete": 1, "third_party_copy": 1}}'
rucio-admin rse add-protocol --hostname dtn04.nersc.gov --scheme gsiftp --prefix /global/cscratch1/sd/uscms/rucio T3_US_NERSC --domain-json '{"wan": {"read": 1, "write": 1, "delete": 1, "third_party_copy": 1}}'
rucio-admin rse add-protocol --hostname dtn05.nersc.gov --scheme gsiftp --prefix /global/cscratch1/sd/uscms/rucio T3_US_NERSC --domain-json '{"wan": {"read": 1, "write": 1, "delete": 1, "third_party_copy": 1}}'
rucio-admin rse add-protocol --hostname dtn06.nersc.gov --scheme gsiftp --prefix /global/cscratch1/sd/uscms/rucio T3_US_NERSC --domain-json '{"wan": {"read": 1, "write": 1, "delete": 1, "third_party_copy": 1}}'
rucio-admin rse add-protocol --hostname dtn07.nersc.gov --scheme gsiftp --prefix /global/cscratch1/sd/uscms/rucio T3_US_NERSC --domain-json '{"wan": {"read": 1, "write": 1, "delete": 1, "third_party_copy": 1}}'
rucio-admin rse add-protocol --hostname dtn08.nersc.gov --scheme gsiftp --prefix /global/cscratch1/sd/uscms/rucio T3_US_NERSC --domain-json '{"wan": {"read": 1, "write": 1, "delete": 1, "third_party_copy": 1}}'   
rucio-admin rse set-attribute --rse T3_US_NERSC --key fts --value https://cmsfts3.fnal.gov:8446
rucio-admin rse set-attribute --rse T3_US_NERSC --key reaper --value False
rucio-admin account set-limits transfer_ops T3_US_NERSC 1PB

rucio list-rses --expression "cms_type=real&tier=1" | while read rse
do
  rucio-admin rse update-distance --distance 2 $rse T3_US_NERSC
done

rucio list-rses --expression "cms_type=real&tier=2" | while read rse
do
  rucio-admin rse update-distance --distance 3 $rse T3_US_NERSC
done
