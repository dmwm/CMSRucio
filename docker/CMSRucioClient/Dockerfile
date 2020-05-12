FROM centos:7

WORKDIR /tmp

RUN yum install -y epel-release.noarch && \
    yum clean all && \
    rm -rf /var/cache/yum
RUN yum upgrade -y && \
    yum clean all && \
    rm -rf /var/cache/yum
RUN yum install -y httpd mod_wsgi python-pip libaio gcc python-devel.x86_64 \
                   install wget unzip \
                   mod_ssl openssl-devel.x86_64 bind-utils nano git && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN pip install --upgrade pip setuptools
RUN rm -rf /usr/lib/python2.7/site-packages/ipaddress*

# Get various repos for yum setup

RUN curl -o /etc/yum.repos.d/ca.repo https://raw.githubusercontent.com/rucio/rucio/1.20.3/etc/docker/dev/ca.repo

# Install WLCG repo for CMS VOMS

RUN yum -y install http://linuxsoft.cern.ch/wlcg/centos7/x86_64/wlcg-repo-1.0.0-1.el7.noarch.rpm && \
    yum clean all && \
    rm -rf /var/cache/yum

# Install sets of RPM
RUN yum install -y which krb5-devel gridsite \
                   lcg-CA globus-proxy-utils voms-clients-java voms-clients-cpp wlcg-voms-cms \
                   gfal2-python gfal2-all gfal2-util \
                   xrootd-client pylint && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN pip install statsd python-gitlab

# Install Rucio and other stuff
RUN pip install rucio-clients multiprocessing-logging jsonschema PyYAML pystatsd

# Install DAS client to pull info from PhEDEx/DBS
RUN curl -k -o  /bin/dasgoclient https://www.lns.cornell.edu/~vk/Work/dasgoclient && chmod +x /bin/dasgoclient
RUN curl -o /etc/ca.crt https://cafiles.cern.ch/cafiles/certificates/CERN%20Root%20Certification%20Authority%202.crt

WORKDIR /root
RUN git clone https://github.com/rucio/rucio.git

COPY scripts /scripts

COPY rucio-testbed.cfg /opt/rucio-testbed/etc/rucio.cfg
COPY rucio-dev.cfg /opt/rucio-dev/etc/rucio.cfg
COPY rucio-int.cfg /opt/rucio-int/etc/rucio.cfg
COPY rucio-prod.cfg /opt/rucio-prod/etc/rucio.cfg
COPY rucio-futureprod.cfg /opt/rucio-futureprod/etc/rucio.cfg

# Pull some useful stuff out of git
WORKDIR /root
RUN git clone https://github.com/dmwm/CMSRucio.git

#ADD baseclient.py /usr/lib/python2.7/site-packages/rucio/client/baseclient.py
#ADD sessions.py /usr/lib/python2.7/site-packages/requests/sessions.py
#ADD adapters.py /usr/lib/python2.7/site-packages/requests/adapters.py

ENV X509_USER_PROXY=/tmp/x509up
ENTRYPOINT ["/bin/bash", "-c", "sleep infinity"]
