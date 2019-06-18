#!/usr/bin/env bash

# set -x

# We have to copy the certificates because we cannot change permissions on them as mounted secrets and voms-proxy is particular about permissions

cp /opt/rucio/certs/usercert.pem /tmp/cert.pem
cp /opt/rucio/keys/new_userkey.pem /tmp/key.pem
chmod 400 /tmp/key.pem

# Generate a proxy with the voms extension if requested
voms-proxy-init2 --debug -rfc -valid 96:00 -cert /tmp/cert.pem -key /tmp/key.pem -out /tmp/x509up -voms cms -rfc -timeout 5

cd /root/CMSRucio
git pull origin master
export RUCIO_ACCOUNT=root

echo Using config file in $RUCIO_HOME

cd docker/CMSRucioClient/scripts/

echo "Site sync config file:"
cat  /etc/sync-config/site-sync.yaml

./synccmssites.py --nodaemon --config /etc/sync-config/site-sync.yaml --logs /dev/stdout
