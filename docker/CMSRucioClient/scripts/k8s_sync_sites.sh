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

#echo "Syncing RSEs"
#./cmsrses.py --pnn all --select 'T2_\S+' --exclude 'T2_MY_\S+' --exclude '\S+CERN\S+' --type real --type temp --type test --fts https://fts3.cern.ch:8446
#./cmsrses.py --pnn all --select 'T1_\S+' --exclude '.*MSS' --exclude '\S+_Tape_Test' --type real --type test --fts https://fts3.cern.ch:8446
#./cmsrses.py --pnn all --select 'T3_CH_PSI' --select 'T3_TW_NTU_HEP' --select  'T3_US_Brown' \
#                       --select 'T3_US_TAMU'  --type real --fts https://fts3.cern.ch:8446
#

#./cmsrses.py --pnn all --select 'T3_TW_NCU'  --type real --type temp  --fts https://fts3.cern.ch:8446

# OR ./setRucioFromGitlab with some new parameters

#echo "Creating sync accounts"
#./syncaccounts.py --identity "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cmsrucio/CN=430796/CN=Robot: CMS Rucio Data Transfer" --type x509

echo "Setting quotas"
./setSiteQuotas
echo "Setting availability"
./setSiteAvailability

echo "Creating user accounts and setting quotas"
./user_to_site_mapping.py

#echo "Creating links"
#./cmslinks.py --phedex_link --overwrite # Remove the --disable flag for Katy's RSE
## ./cmslinks.py --phedex_link --overwrite --disable
