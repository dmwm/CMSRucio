#!/usr/bin/env bash

# We have to copy the certificates because we cannot change permissions on them as mounted secrets and voms-proxy is particular about permissions

cp /opt/rucio/certs/usercert.pem /tmp/cert.pem
cp /opt/rucio/keys/new_userkey.pem /tmp/key.pem
chmod 400 /tmp/key.pem

# Generate a proxy with the voms extension if requested
voms-proxy-init -voms cms  -cert /tmp/cert.pem -key /tmp/key.pem
voms-proxy-info

cd /root/CMSRucio
git pull origin master
export RUCIO_ACCOUNT=root

echo Using config file in $RUCIO_HOME

cat $RUCIO_HOME/etc/rucio.cfg

set -x

cd docker/CMSRucioClient/scripts/

echo "Syncing account roles for managers and group accounts"
./syncRolesToAttributes.py
./syncRolesToGroupAccounts.py
./syncEgroupsToGroupAccounts.py

if [ "$RUCIO_HOME" = "/opt/rucio-int" ]
  then
  ./setRucioFromGitlab --type int-real
fi
if [ "$RUCIO_HOME" = "/opt/rucio-prod" ]
  then
  ./setRucioFromGitlab --type prod-real
fi

./setRucioFromGitlab --type test
./setRucioFromGitlab --type temp

echo "Setting quotas"
./setSiteQuotas
echo "Setting availability"
./setSiteAvailability

if [ "$RUCIO_HOME" = "/opt/rucio-prod" ]
  then
  echo "Creating user accounts and setting quotas"
  ./user_to_site_mapping.py
fi


echo "Creating links"
./cmslinks.py --phedex_link --overwrite # Remove the --disable flag for Katy's RSE
## ./cmslinks.py --phedex_link --overwrite --disable
