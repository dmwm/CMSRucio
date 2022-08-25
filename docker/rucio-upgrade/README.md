Steps to create image and push to docker hub

* podman login registry.cern.ch
* export RUCIO_VERSION=1.29.2.post1
* podman build   --build-arg RUCIO_VERSION=$RUCIO_VERSION -t registry.cern.ch/cmsrucio/rucio-upgrade:release-$RUCIO_VERSION . 
* podman push registry.cern.ch/cmsrucio/rucio-upgrade:release-$RUCIO_VERSION
