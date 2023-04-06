FROM centos:7

RUN yum upgrade -y && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN yum install -y epel-release.noarch && \
    yum clean all && \
    rm -rf /var/cache/yum

# FIXME: What is all this? We can't need it all
RUN yum install -y python3 python3-pip pylint \
                   httpd mod_wsgi libaio gcc python-devel.x86_64 \
                   install wget unzip \
                   mod_ssl openssl-devel.x86_64 bind-utils nano git && \
    yum clean all && \
    rm -rf /var/cache/yum

WORKDIR /tmp

# Get various repos for yum setup
RUN curl -o /etc/yum.repos.d/ca.repo https://raw.githubusercontent.com/rucio/rucio/1.20.3/etc/docker/dev/ca.repo

# Install WLCG repo for CMS VOMS

RUN yum -y install http://linuxsoft.cern.ch/wlcg/centos7/x86_64/wlcg-repo-1.0.0-1.el7.noarch.rpm && \
    yum clean all && \
    rm -rf /var/cache/yum

# Install sets of RPM
# FIXME: This overlaps with above

RUN yum-config-manager --save --setopt=wlcg.skip_if_unavailable=true \
        && yum install -y which krb5-devel gridsite \
                   lcg-CA globus-proxy-utils voms-clients-java voms-clients-cpp wlcg-voms-cms \
                   gfal2-python gfal2-all gfal2-util gfal2-python3 \
                   nordugrid-arc-client nordugrid-arc-plugins-gfal \
                   nordugrid-arc-plugins-globus nordugrid-arc-plugins-s3 \
                   nordugrid-arc-plugins-xrootd \
                   xrootd-client && \
    yum clean all && \
    rm -rf /var/cache/yum


# Install Rucio and other stuff

RUN pip3 install --upgrade pip
RUN pip3 install --upgrade setuptools
RUN pip3 install rucio-clients
RUN pip3 install python-gitlab
RUN pip3 install liboidcagent

# Install DAS client to pull info from PhEDEx/DBS
RUN curl -k -o  /bin/dasgoclient https://www.lns.cornell.edu/~vk/Work/dasgoclient && chmod +x /bin/dasgoclient
RUN curl -o /etc/ca.crt https://cafiles.cern.ch/cafiles/certificates/CERN%20Root%20Certification%20Authority%202.crt

WORKDIR /root
RUN git clone https://github.com/rucio/rucio.git

COPY scripts /scripts
COPY loadtest /loadtest

COPY rucio-testbed.cfg /opt/rucio-testbed/etc/rucio.cfg
COPY rucio-dev.cfg /opt/rucio-dev/etc/rucio.cfg
COPY rucio-int.cfg /opt/rucio-int/etc/rucio.cfg
COPY rucio-prod.cfg /opt/rucio-prod/etc/rucio.cfg

# Pull some useful stuff out of git
WORKDIR /root

#RUN git clone https://github.com/dmwm/CMSRucio.git
RUN git clone -b fix_oidc https://github.com/dciangot/CMSRucio.git

RUN mkdir -p /etc/grid-security ; ln -s /cvmfs/grid.cern.ch/etc/grid-security/certificates /etc/grid-security/

# Overwrite what's there for testing
# COPY scripts /root/CMSRucio/docker/CMSRucioClient/scripts

# Our schema
ADD https://raw.githubusercontent.com/ericvaandering/rucio/cms_nano12/lib/rucio/common/schema/cms.py /usr/lib/python2.7/site-packages/rucio/common/schema/cms.py
RUN python -m compileall /usr/lib/python2.7/site-packages/rucio/common/schema

ENV X509_USER_PROXY=/tmp/x509up
ENTRYPOINT ["/bin/bash", "-c", "sleep infinity"]
