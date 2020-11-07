# voms-proxy-init --voms cms:/cms/Role=production --cert ruciocert.pem --key ruciokey.pem
docker run -it --rm \
  -v $(voms-proxy-info --path):/tmp/x509up \
  -v $PWD/rucio.cfg:/opt/rucio/etc/rucio.cfg \
  -v $PWD/loadtest.py:/root/loadtest.py \
  -e ACCOUNT=transfer_ops \
  --entrypoint /usr/bin/python \
  cmssw/rucio_client loadtest.py
