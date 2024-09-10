# Copyright European Organization for Nuclear Research (CERN) 2017
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Eric Vaandering, <ewv@fnal.gov>, 2018

ARG RUCIO_VERSION

FROM rucio/rucio-server:release-$RUCIO_VERSION

# Add CMS policy package
RUN mkdir -p /opt/rucio/policy/CMSRucioPolicy
COPY src/policy/CMSRucioPolicy/ /opt/rucio/policy/CMSRucioPolicy/

RUN mkdir -p /etc/grid-security && ln -s /cvmfs/grid.cern.ch/etc/grid-security/certificates /etc/grid-security/
ENV RUCIO_CA_PATH="/cvmfs/grid.cern.ch/etc/grid-security/certificates"
ADD docker/rucio-server/cms-entrypoint.sh /

# Install what's needed out of dmwm/rucio/CMS branch
ADD docker/rucio-server/install_mail_templates.sh /tmp/
RUN /tmp/install_mail_templates.sh

# Cannot make patch directory unless there are patches
RUN mkdir -p /patch

# Patch for dst_file_report
ADD https://patch-diff.githubusercontent.com/raw/dynamic-entropy/rucio/pull/1.patch /patch/1.patch
ADD https://patch-diff.githubusercontent.com/raw/ericvaandering/rucio/pull/10.patch /patch/10.patch

ENTRYPOINT ["/cms-entrypoint.sh"]
