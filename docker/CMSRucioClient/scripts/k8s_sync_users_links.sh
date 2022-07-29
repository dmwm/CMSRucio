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


if [ "$RUCIO_HOME" = "/opt/rucio-prod" ]
  then
  echo "Syncing Roles to RSE attributes"
  ./syncRolesToAttributesLocalUsers.py
  echo "Creating user accounts and setting quotas"
  ./user_to_site_mapping.py
fi

echo "Creating links"
./cmslinks.py --overwrite --disable

# Preparing oidc-agent to get/refresh token to retrieve OIDC user information
export OIDC_CONFIG_DIR=$HOME/.oidc-agent

eval $(oidc-agent)

oidc-gen cms --issuer "$IAM_SERVER" \
    --client-id "$IAM_CLIENT_ID" \
    --client-secret "$IAM_CLIENT_SECRET" \
    --rt "$REFRESH_TOKEN" \
    --confirm-yes \
    --scope "scim:read" \
    --redirect-uri http://localhost:8843 \
    --pw-cmd "echo \"DUMMY PWD\""

echo "Syncing OIDC user identities"
./syncaccount_oidc.py