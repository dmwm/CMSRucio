# Copyright European Organization for Nuclear Research (CERN) 2017
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Eric Vaandering, <ewv@fnal.gov>, 2018

ARG RUCIO_VERSION
FROM rucio/rucio-daemons:release-$RUCIO_VERSION

# Add CMS policy package
RUN mkdir -p /opt/rucio/policy/CMSRucioPolicy
COPY src/policy/CMSRucioPolicy /opt/rucio/policy/CMSRucioPolicy

# Install what's needed out of dmwm/rucio/CMS branch
ADD docker/rucio-daemons/install_mail_templates.sh /tmp/
RUN /tmp/install_mail_templates.sh

# Install globus SDK
RUN python3 -m pip install --no-cache-dir globus-sdk pyyaml
ADD docker/rucio-daemons/globus-config.yml.j2 /tmp

RUN rmdir /etc/grid-security/certificates && ln -s /cvmfs/grid.cern.ch/etc/grid-security/certificates /etc/grid-security/certificates

ADD docker/rucio-daemons/cms-entrypoint.sh /

# Cannot make patch directory unless there are patches
RUN mkdir -p /patch

# Patch for dst_file_report: Remove when https://github.com/rucio/rucio/pull/7081 is available
ADD https://patch-diff.githubusercontent.com/raw/dynamic-entropy/rucio/pull/1.patch /patch/1.patch

# Patch for directory scoped token support
ADD https://patch-diff.githubusercontent.com/raw/dynamic-entropy/rucio/pull/4.patch /patch/token.patch

# To be removed once this PR is available in our rucio version
ADD https://patch-diff.githubusercontent.com/raw/rucio/rucio/pull/7106.patch /patch/7106.patch
ENTRYPOINT ["/cms-entrypoint.sh"]
