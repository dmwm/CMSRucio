# voms-proxy-init --voms cms:/cms/Role=production --cert ruciocert.pem --key ruciokey.pem
docker run -it --rm \
  -v $(voms-proxy-info --path):/tmp/x509up \
  -v $PWD/rucio.cfg:/opt/rucio/etc/rucio.cfg \
  -v $PWD/loadtest.py:/root/loadtest.py \
  -e RUCIO_ACCOUNT=transfer_ops \
  --entrypoint /usr/bin/python \
  cmssw/rucio_client loadtest.py -v \
  --source_rse_expression "rse_type=disk&cms_type=real&tier<3" \
  --dest_rse_expression "rse_type=disk&cms_type=real&tier=1"
