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

RUN mkdir -p /etc/grid-security && ln -s /cvmfs/grid.cern.ch/etc/grid-security/certificates /etc/grid-security/

# Might get recreated if apache was upgraded
RUN rm -f /etc/httpd/conf.d/welcome.conf /etc/httpd/conf.d/userdir.conf /etc/httpd/conf.d/ssl.conf

ENV RUCIO_CA_PATH="/cvmfs/grid.cern.ch/etc/grid-security/certificates"

# Cannot make patch directory unless there are patches
#RUN mkdir -p /patch

# Patch for auto approve plugin rucio/pull/6215
#ADD https://github.com/rucio/rucio/pull/6215.patch /patch/6215.patch

ADD docker/rucio-ui/cms-entrypoint.sh /
ENTRYPOINT ["/cms-entrypoint.sh"]
