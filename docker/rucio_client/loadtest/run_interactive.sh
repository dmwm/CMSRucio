#!/bin/bash

# To run loadtest script in docker container for testing, you first need a production proxy:
# $ voms-proxy-init --voms cms:/cms/Role=production --cert ruciocert.pem --key ruciokey.pem
# The loadtest_client container is built in the parent directory with:
# $ docker build -t loadtest_client -f Dockerfile.loadtest .
# The loadtest.py executable is the entrypoint

docker run -it --rm \
  -v $(voms-proxy-info --path):/tmp/x509up \
  -e RUCIO_HOME=/opt/rucio-prod/ \
  loadtest_client \
  -v --source_rse_expression "tier=1&cms_type=real" --dest_rse_expression "tier=1&rse_type=TAPE&cms_type=real"

