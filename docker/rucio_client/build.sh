#!/bin/sh

set -e

export RUCIO_VERSION=32.3.1
export CMS_VERSION=${RUCIO_VERSION}.cms1

export HARBOR=registry.cern.ch/cmsrucio

podman build --build-arg RUCIO_VERSION=$RUCIO_VERSION -f Dockerfile -t $HARBOR/rucio-client:release-$CMS_VERSION .