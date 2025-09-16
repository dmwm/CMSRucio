#!/usr/bin/env bash

# We have to copy the certificates because we cannot change permissions on them as mounted secrets and voms-proxy is particular about permissions

cp /opt/rucio/certs/$USERCERT_NAME /tmp/cert.pem
cp /opt/rucio/keys/$USERKEY_NAME /tmp/key.pem
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

cd docker/rucio_client/scripts/


if [ "$RUCIO_HOME" = "/opt/rucio-prod" ]
  then
  echo "Syncing User Roles to RSE Attributes and Group Accounts"
  ./syncUserRoles.py
  echo "Creating user accounts and setting quotas"
  ./user_to_site_mapping.py
  echo "Adding group account scopes"
  ./account_to_scope_mapping.py --only-include-accounts-with-group-suffix --include-local-users-accounts
  echo "Syncing custom RSE Roles"
  ./syncCustomRoles.py
fi

# Disable temporarily due to CMSTRANSF-1045 
echo "Creating links"
./cmslinks.py --overwrite --disable


echo "Syncing OIDC user identities"
./syncaccount_oidc.py
