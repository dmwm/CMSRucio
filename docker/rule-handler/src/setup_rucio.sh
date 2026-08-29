#!/bin/bash
# shellcheck disable=SC1090
source /cvmfs/cms.cern.ch/cmsset_default.sh
voms-proxy-init -valid 192:00 --cert /etc/secrets/dmtops.crt.pem --key /etc/secrets/dmtops.key.pem
source /cvmfs/cms.cern.ch/rucio/setup-py3.sh
export RUCIO_ACCOUNT=transfer_ops

#kinit -kt /etc/secrets/dmtops.keytab dmtops
export X509_USER_KEY=/etc/secrets/dmtops.key.pem
export X509_USER_CERT=/etc/secrets/dmtops.crt.pem