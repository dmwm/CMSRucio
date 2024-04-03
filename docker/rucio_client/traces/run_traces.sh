#! /bin/bash

#mkdir -p /opt/rucio/etc/
#
#if [ -f /opt/rucio/etc/rucio.cfg ]; then
#    echo "rucio.cfg already mounted."
#else
#    echo "rucio.cfg not found. will generate one."
#    j2 /tmp/rucio.cfg.j2 | sed '/^\s*$/d' > /opt/rucio/etc/rucio.cfg
#fi
#
#if [ ! -z "$RUCIO_PRINT_CFG" ]; then
#    echo "=================== /opt/rucio/etc/rucio.cfg ============================"
#    cat /opt/rucio/etc/rucio.cfg
#    echo ""
#fi

cp /traces/dot-jobber.yaml /root/.jobber

# Get docker environment into jobber
export >> /etc/profile.d/jobber.sh


echo "Starting Jobber"
/usr/local/libexec/jobbermaster