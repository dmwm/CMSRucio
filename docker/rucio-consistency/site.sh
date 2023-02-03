#!/bin/bash

cd /consistency/cms_consistency/site_cmp3

cp /opt/proxy/x509up /tmp/x509up
chmod 600 /tmp/x509up
export X509_USER_PROXY=/tmp/x509up
export RUCIO_CONFIG=/consistency/rucio-client.cfg
export PYTHON=python3

cfg_src=/config/config.yaml
cfg_copy=/consistency/config.yaml

if [ ! -f $cfg_copy ]; then
    cp $cfg_src $cfg_copy    # to make it editable
    echo Config file $cfg_src copied to $cfg_copy
fi

./site_cmp3.sh \
  $cfg_copy \
  /opt/rucio/etc/rucio.cfg \
  $1 \
  /var/cache/consistency-temp \
  /var/cache/consistency-dump \
  /tmp/x509up



