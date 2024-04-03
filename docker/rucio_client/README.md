# CMS rucio_client

N.B. We are in the process of changing over from docker and dockerhub to podmand and CERN's Harbor registry. 
The documention below reflects this state of flux. For new builds, please switch the build and instructions to Harbor.

## Building
To build the image, the following environment variables need to be set:
```
export RUCIO_VERSION=32.3.1
export CMS_VERSION=${RUCIO_VERSION}.cms1
export HARBOR=registry.cern.ch/cmsrucio
```

Then while in the root `CMSRucio` directory, run:
```
podman build --build-arg RUCIO_VERSION=$RUCIO_VERSION -f docker/rucio_client/Dockerfile -t $HARBOR/rucio_client:release-$CMS_VERSION .
```

To use the `almalinux:9-minimal` base:
```
podman build --build-arg RUCIO_VERSION=$RUCIO_VERSION -f docker/rucio_client/Dockerfile.minimal -t $HARBOR/rucio_client:release-minimal-$CMS_VERSION .
```

### Old rucio-trace instructions
```
docker build . -f Dockerfile.trace -t ericvaandering/rucio-trace
docker push ericvaandering/rucio-trace
```
    

## Running 
To run (no need to build, Eric does this occassionally):

    docker pull cmssw/rucio_client
    docker kill client; docker rm client
    docker run -d --name client cmssw/rucio_client
    docker cp ~/.globus/usercert.pem client:/tmp/usercert.pem
    docker cp ~/.globus/userkey.pem client:/tmp/userkey.pem
    docker exec -it client /bin/bash

Grid certs will also need to be added if running locally.

Inside container generate a proxy and connect to the Rucio server

    chown root *.pem
    voms-proxy-init -voms cms -key userkey.pem -cert usercert.pem 
    export RUCIO_ACCOUNT=[username]
    rucio whoami

to verify it worked. (You should get feedback saying the account is correct.)

We ship the container with a number of configuration files in `/opt` which you can switch to with

    export RUCIO_HOME=/opt/rucio-[foo]/

If you want to develop the rucio client code inside the container, add ```-v /path/to/git/rucio/lib/rucio:/usr/lib/python2.7/site-packages/rucio```
to the ```docker run``` commands above. Then edit the code you checked out from GitHub in ```/path/to/git/rucio```
on your local machine and the changes will be seen inside the container.
