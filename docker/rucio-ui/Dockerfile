# Copyright European Organization for Nuclear Research (CERN) 2017
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Eric Vaandering, <ewv@fnal.gov>, 2018

ARG RUCIO_VERSION
FROM rucio/rucio-ui:release-$RUCIO_VERSION

# Add CMS policy package
RUN mkdir -p /opt/rucio/policy/CMSRucioPolicy
COPY src/policy/CMSRucioPolicy /opt/rucio/policy/CMSRucioPolicy

# FIXME: Probably don't need this anymore
RUN yum -y install http://linuxsoft.cern.ch/wlcg/centos7/x86_64/wlcg-repo-1.0.0-1.el7.noarch.rpm \
    && yum clean all \
    && rm -rf /var/cache/yum

ADD http://repository.egi.eu/sw/production/cas/1/current/repo-files/EGI-trustanchors.repo /etc/yum.repos.d/egi.repo
RUN yum update  -y \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN mkdir -p /etc/grid-security && ln -s /cvmfs/grid.cern.ch/etc/grid-security/certificates /etc/grid-security/

# Might get recreated if apache was upgraded
RUN rm -f /etc/httpd/conf.d/welcome.conf /etc/httpd/conf.d/userdir.conf /etc/httpd/conf.d/ssl.conf

ENV RUCIO_CA_PATH="/cvmfs/grid.cern.ch/etc/grid-security/certificates"

# Cannot make patch directory unless there are patches
RUN mkdir -p /patch

# Eric's patch for FNAL URL problem
# Merged for Rucio 32
ADD https://patch-diff.githubusercontent.com/raw/rucio/rucio/pull/6097.patch /patch/6097.patch

# Patch for auto approve plugin rucio/pull/6215
ADD https://raw.githubusercontent.com/dynamic-entropy/rucio/b55a1d62e39565ac0a220b64f42530db96cb468f/0001-Policies-Adding-plugin-hook-for-auto-approval-flow.patch /patch/6203.patch 

ADD docker/rucio-ui/cms-entrypoint.sh /
ENTRYPOINT ["/cms-entrypoint.sh"]
