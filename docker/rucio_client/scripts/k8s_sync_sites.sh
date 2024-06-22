#!/usr/bin/env bash

# We have to copy the certificates because we cannot change permissions on them as mounted secrets and voms-proxy is particular about permissions

cp /opt/rucio/certs/usercert.pem /tmp/cert.pem
cp /opt/rucio/keys/new_userkey.pem /tmp/key.pem
chmod 400 /tmp/key.pem

# Generate a proxy with the voms extension if requested
voms-proxy-init -voms cms  -cert /tmp/cert.pem -key /tmp/key.pem
voms-proxy-info

cd /root/CMSRucio
export RUCIO_ACCOUNT=root

echo Using config file in $RUCIO_HOME

cat $RUCIO_HOME/etc/rucio.cfg

set -x

cd docker/rucio_client/scripts/
if [ "$RUCIO_HOME" = "/opt/rucio-int" ]
  then
  echo "Syncing Integration sites from JSON"
  ./setRucioFromGitlab --type int-real 
  exit 0
fi

echo "Syncing sites from JSON"
if [ "$RUCIO_HOME" = "/opt/rucio-prod" ]
  then
  ./setRucioFromGitlab --type prod-real
fi

./setRucioFromGitlab --type test
./setRucioFromGitlab --type temp

echo "Set site capacity"
./setSiteCapacity

echo "Setting availability"
./setSiteAvailability
echo "Update DDM quota"
./updateDDMQuota
