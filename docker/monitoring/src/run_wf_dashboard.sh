#!/bin/bash
# shellcheck disable=SC1090
chmod 777 /src/config_rucio.sh
source /src/config_rucio.sh
kinit -kt /etc/secrets/dmtops.keytab dmtops

python3 /src/monit_pull.py