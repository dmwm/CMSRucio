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

ADD https://raw.githubusercontent.com/dmielaikaite/probes/dmielaik_probes/common/check_used_space /probes/common

# PR 71
# Supplanted in PR115 ADD https://raw.githubusercontent.com/ericvaandering/probes/expiring_rules_rse_127/cms/check_expiring_rules_per_rse /probes/cms

# PR 72
ADD https://raw.githubusercontent.com/ericvaandering/probes/update_check_expected_total_number_of_files_per_rse/cms/check_expected_total_number_of_files_per_rse /probes/cms

# Where from?
# Supplanted in PR115 ADD https://raw.githubusercontent.com/ericvaandering/probes/cms_check_expired_rules/common/check_expired_rules /probes/common
ADD https://raw.githubusercontent.com/ericvaandering/probes/cms_check_expired_locked/common/check_expired_locked_rules /probes/common

# PR 115

ADD https://raw.githubusercontent.com/ericvaandering/probes/test_new_pusher/cms/check_expected_total_number_of_files_per_rse /probes/cms/
ADD https://raw.githubusercontent.com/ericvaandering/probes/test_new_pusher/cms/check_expiring_rules_per_rse /probes/cms/
ADD https://raw.githubusercontent.com/ericvaandering/probes/test_new_pusher/cms/check_report_used_space /probes/cms/
ADD https://raw.githubusercontent.com/ericvaandering/probes/test_new_pusher/common/check_expired_rules /probes/common/
ADD https://raw.githubusercontent.com/ericvaandering/probes/test_new_pusher/common/utils/common.py /probes/common/utils/

# PR 122

ADD https://raw.githubusercontent.com/ericvaandering/probes/675-dev-merge-and-update-rse-based-probes/cms/check_rse_files_rules /probes/cms/

RUN chmod +x /probes/common/check_*
RUN chmod +x /probes/cms/check_*

# Temporary while we are adding variables to the config. Push to rucio-containers
ADD https://raw.githubusercontent.com/ericvaandering/containers/probes_prom/probes/rucio.cfg.j2 /tmp/
