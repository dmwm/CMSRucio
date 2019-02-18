#!/usr/bin/env bash

# We have to copy the certificates because we cannot change permissions on them as mounted secrets and voms-proxy is particular about permissions

cp /opt/rucio/certs/usercert.pem /tmp/cert.pem
cp /opt/rucio/keys/new_userkey.pem /tmp/key.pem
chmod 400 /tmp/key.pem

# Generate a proxy with the voms extension if requested
voms-proxy-init2 --debug -rfc -valid 96:00 -cert /tmp/cert.pem -key /tmp/key.pem -out /tmp/x509up -voms cms -rfc -timeout 5

cd /root/CMSRucio
git remote add sartiran https://github.com/sartiran/CMSRucio.git
git fetch --all
git checkout raw
export RUCIO_ACCOUNT=root

echo Using config file in $RUCIO_HOME

cd docker/CMSRucioClient/scripts/

./cmsrses.py --pnn all --select 'T2_\S+' --exclude '\S+RAL\S*' --exclude '\S+Nebraska\S*' --exclude 'T2_MY_\S+' --exclude 'T2_US_Florida' --exclude '\S+CERN\S+' --type real --type temp --type test --fts https://fts3.cern.ch:8446
./syncaccounts.py
./cmslinks.py --phedex_link --overwrite --disable

