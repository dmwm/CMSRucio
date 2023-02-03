#!/bin/bash

cd /consistency/cms_consistency/wm

cp /opt/proxy/x509up /tmp/x509up
chmod 600 /tmp/x509up
export X509_USER_PROXY=/tmp/x509up

export PYTHON=python3

cfg_src=/unmerged-config/config.yaml
cfg_copy=/consistency/unmerged_config.yaml

if [ ! -f $cfg_copy ]; then
    cp $cfg_src $cfg_copy    # to make it editable
    echo Config file $cfg_src copied to $cfg_copy
fi

./wm_scan.sh $cfg_copy $1  /var/cache/consistency-dump/unmerged
