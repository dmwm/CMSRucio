#! /bin/bash

mkdir -p /opt/rucio/etc/mail_templates/
cp  /root/mail_templates/* /opt/rucio/etc/mail_templates/
ls /opt/rucio/etc/mail_templates/

/usr/sbin/fetch-crl & 

/usr/sbin/crond 

mkdir -p /var/log/rucio/
chown -R apache /var/log/rucio/

# Add the policy package directory to PYTHONPATH
if [ ! -z "$POLICY_PKG_PATH" ]; then
    export PYTHONPATH=${POLICY_PKG_PATH}:${PYTHONPATH:+:}${PYTHONPATH}
fi

/docker-entrypoint.sh

