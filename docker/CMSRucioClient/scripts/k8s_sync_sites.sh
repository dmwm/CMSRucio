#!/usr/bin/env bash

# We have to copy the certificates because we cannot change permissions on them as mounted secrets and voms-proxy is particular about permissions

cp /opt/rucio/certs/usercert.pem /tmp/cert.pem
cp /opt/rucio/keys/new_userkey.pem /tmp/key.pem
chmod 400 /tmp/key.pem

# Generate a proxy with the voms extension if requested
voms-proxy-init -voms cms  -cert /tmp/cert.pem -key /tmp/key.pem

cd /root/CMSRucio
git pull origin master
export RUCIO_ACCOUNT=root

echo Using config file in $RUCIO_HOME

cd docker/CMSRucioClient/scripts/

./cmsrses.py --pnn all --select 'T2_\S+' --exclude 'T2_MY_\S+' --exclude '\S+CERN\S+' --type real --type temp --type test --fts https://fts3.cern.ch:8446
./cmsrses.py --pnn all --select 'T1_\S+' --exclude '.*MSS' --exclude 'T1_UK_RAL_Tape\S+' --type real --type test --fts https://fts3.cern.ch:8446
./syncaccounts.py --identity "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cmsrucio/CN=430796/CN=Robot: CMS Rucio Data Transfer" --type x509
./cmslinks.py --phedex_link --overwrite --disable

