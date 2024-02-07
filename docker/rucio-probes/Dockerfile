# Copyright European Organization for Nuclear Research (CERN) 2017
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Eric Vaandering, <ewv@fnal.gov>, 2019-2020

ARG RUCIO_VERSION
FROM rucio/probes:$RUCIO_VERSION

# For now CMS versions use python3 explicitly. Can be removed when main container goes to python3
#RUN ln -s /usr/bin/python3 /usr/local/bin/python

# Add CMS policy package
RUN mkdir -p /opt/rucio/policy/CMSRucioPolicy
COPY src/policy/CMSRucioPolicy /opt/rucio/policy/CMSRucioPolicy
# Make it search the policy directory. Needs a change for python upgrades
RUN echo "/opt/rucio/policy" > /usr/local/lib/python3.9/site-packages/policy.pth

# Temporary additions

# Until Fernando ports to SQLAlchemy and/or merged

ADD https://raw.githubusercontent.com/ericvaandering/probes/hack_obsolete_replicas/common/check_obsolete_replicas /probes/common
ADD https://raw.githubusercontent.com/nsmith-/probes/hack_replicas/common/check_deletable_replicas /probes/common

# TODO: Merge Donata's probes somewhere
# This is removed by 122 or 123 from Panos
ADD https://raw.githubusercontent.com/dmielaikaite/probes/dmielaik_probes/common/check_used_space /probes/common

# Where from?
# Supplanted in PR115 ADD https://raw.githubusercontent.com/ericvaandering/probes/cms_check_expired_rules/common/check_expired_rules /probes/common
ADD https://raw.githubusercontent.com/ericvaandering/probes/cms_check_expired_locked/common/check_expired_locked_rules /probes/common

# PR 122 - Should be in 33.4
ADD https://raw.githubusercontent.com/ericvaandering/probes/675-dev-merge-and-update-rse-based-probes/cms/check_rse_files_rules /probes/cms/

# PR 123 - Should be in 33.4
ADD https://raw.githubusercontent.com/Panos512/probes/20240116_combines_space_probes/cms/check_used_space /probes/cms/

# PR 125 - Should be in 33.4
ADD https://raw.githubusercontent.com/haozturk/probes/CMSRucio-688-fix/cms/check_rule_counts /probes/cms/

# PR 126 - Should be in 33.5
ADD https://raw.githubusercontent.com/rucio/probes/cms_rules_by_account_to_prom/cms/check_rules_states_by_account /probes/cms/

RUN chmod +x /probes/common/check_*
RUN chmod +x /probes/cms/check_*

# Temporary while we are adding variables to the config. Push to rucio-containers
ADD https://raw.githubusercontent.com/ericvaandering/containers/probes_prom/probes/rucio.cfg.j2 /tmp/
