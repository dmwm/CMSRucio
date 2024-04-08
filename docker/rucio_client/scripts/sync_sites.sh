#!/usr/bin/env bash

./cmsrses.py --pnn all --select 'T2_\S+' --exclude '\S+RAL\S*' --exclude '\S+Nebraska\S*' --exclude 'T2_MY_\S+' --exclude '\S+CERN\S+' --type real --type temp --type test --fts https://fts3.cern.ch:8446
./cmsrses.py --pnn all --select 'T1_\S+' --exclude '.*MSS' --type real --type test --fts https://fts3.cern.ch:8446
./syncaccounts.py --identity "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cmsrucio/CN=430796/CN=Robot: CMS Rucio Data Transfer" --type x509
./cmslinks.py --phedex_link --overwrite --disable

