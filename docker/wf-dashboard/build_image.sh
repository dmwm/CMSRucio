#! /bin/sh

podman build . -t registry.cern.ch/cmsrucio/wf-dashboard:release-0.9
podman push registry.cern.ch/cmsrucio/wf-dashboard:release-0.9