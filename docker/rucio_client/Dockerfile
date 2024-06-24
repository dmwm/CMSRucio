FROM almalinux:9

ARG RUCIO_VERSION
ARG CMSRUCIO_GITTAG

RUN dnf upgrade -y && \
    dnf clean all && \
    rm -rf /var/cache/dnf

RUN dnf install -y epel-release.noarch && \
    dnf clean all && \
    rm -rf /var/cache/dnf

RUN dnf config-manager --set-enabled crb

# FIXME: What is all this? We can't need it all
RUN dnf install -y python3 python3-pip libaio gcc python3-devel \
    wget unzip openssl-devel bind-utils nano git && \
    dnf clean all && \
    rm -rf /var/cache/dnf

WORKDIR /tmp

# Get various repos for yum setup
RUN curl -o /etc/yum.repos.d/ca.repo https://raw.githubusercontent.com/rucio/rucio/1.20.3/etc/docker/dev/ca.repo

# Install WLCG repo for CMS VOMS
RUN dnf -y install http://linuxsoft.cern.ch/wlcg/el9/x86_64/wlcg-repo-1.0.0-1.el9.noarch.rpm && \
    dnf clean all && \
    rm -rf /var/cache/dnf

# Install sets of RPM
# FIXME: This overlaps with above

RUN dnf config-manager --save --setopt=wlcg.skip_if_unavailable=true \
    && dnf install -y which krb5-devel gridsite \
    globus-proxy-utils voms-clients-cpp wlcg-voms-cms \
    wlcg-iam-vomses-cms \
    gfal2-util gfal2-all gfal2-plugin-xrootd python3-gfal2 && \
    dnf clean all && \
    rm -rf /var/cache/dnf

COPY docker/rucio_client/scripts /scripts
ADD docker/rucio_client/rucio-int.cfg /opt/rucio-int/etc/rucio.cfg
ADD docker/rucio_client/rucio-prod.cfg /opt/rucio-prod/etc/rucio.cfg

# Install Rucio and other stuff
RUN pip3 install --no-cache-dir --upgrade pip setuptools && \
    pip3 -v install --no-cache-dir rucio-clients==$RUCIO_VERSION && \
    pip3 -v install --no-cache-dir -r /scripts/requirements.txt

# Install DAS client to pull info from PhEDEx/DBS
RUN curl -k -o  /bin/dasgoclient https://www.lns.cornell.edu/~vk/Work/dasgoclient && chmod +x /bin/dasgoclient
RUN curl -o /etc/ca.crt https://cafiles.cern.ch/cafiles/certificates/CERN%20Root%20Certification%20Authority%202.crt

WORKDIR /root
RUN git clone https://github.com/rucio/rucio.git

# Pull some useful stuff out of git
WORKDIR /root
RUN git clone https://github.com/dmwm/CMSRucio.git
RUN cd CMSRucio && git checkout $CMSRUCIO_GITTAG

RUN mkdir -p /etc/grid-security ; ln -s /cvmfs/grid.cern.ch/etc/grid-security/certificates /etc/grid-security/

# Overwrite what's there for testing
# COPY scripts /root/CMSRucio/docker/CMSRucioClient/scripts

# Our schema
ADD https://raw.githubusercontent.com/ericvaandering/rucio/cms_nano12/lib/rucio/common/schema/cms.py /usr/lib/python3.9/site-packages/rucio/common/schema/cms.py
RUN python3 -m compileall /usr/lib/python3.9/site-packages/rucio/common/schema

ENV X509_USER_PROXY=/tmp/x509up
ENTRYPOINT ["/bin/bash", "-c", "sleep infinity"]
