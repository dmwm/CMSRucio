#!/bin/bash

helm repo add stable https://charts.helm.sh/stable
# OCI
echo "${HARBOR_TOKEN}" | helm registry login -u ${HARBOR_USERNAME} --password-stdin registry.cern.ch
cd helm
for chart in $(ls -d */Chart.yaml | xargs dirname); do
        LOCAL_VERSION=$(grep -R "version:" ${chart}/Chart.yaml | awk '{print $2}')
        helm dep update ${chart}
        helm package ${chart}
	      set +x
        set -x
	      helm push "${chart}-${LOCAL_VERSION}.tgz" oci://registry.cern.ch/cmsrucio/helm      
done
