#! /bin/bash

# To run loadtest script in docker container for testing, you first need a production proxy:
# $ voms-proxy-init --voms cms:/cms/Role=production --cert ruciocert.pem --key ruciokey.pem
# The loadtest_client container is built in the parent directory with:
# $ docker build -t loadtest_client -f Dockerfile.loadtest .
# The loadtest.py executable is the entrypoint

cp /tmp/x509up0/x509up /tmp/

cd /loadtest

./loadtest.py -v   --source_rse_expression "rse_type=disk&cms_type=real&tier<3" --dest_rse_expression "rse_type=disk&cms_type=real&tier<3"
