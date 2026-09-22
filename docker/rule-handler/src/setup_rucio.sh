#!/bin/bash
# shellcheck disable=SC1090
source /cvmfs/cms.cern.ch/cmsset_default.sh
export X509_USER_PROXY=/tmp/x509up_dmtops.pem
voms-proxy-init -voms cms -valid 192:00 --cert /etc/secrets/dmtops.crt.pem --key /etc/secrets/dmtops.key.pem --out  "$X509_USER_PROXY"
source /cvmfs/cms.cern.ch/rucio/setup-py3.sh
export RUCIO_ACCOUNT=transfer_ops

export X509_USER_KEY=/etc/secrets/dmtops.key.pem
export X509_USER_CERT=/etc/secrets/dmtops.crt.pem