FROM centos:7

WORKDIR /tmp

RUN yum install -y epel-release.noarch && \
    yum clean all && \
    rm -rf /var/cache/yum
RUN yum upgrade -y && \
    yum clean all && \
    rm -rf /var/cache/yum
RUN yum install -y httpd mod_wsgi python-pip libaio gcc python-devel.x86_64 mod_ssl openssl-devel.x86_64 && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN pip install --upgrade pip setuptools
RUN rm -rf /usr/lib/python2.7/site-packages/ipaddress*

RUN pip install rucio-clients

# Get various repos for yum setup

RUN curl -o /etc/yum.repos.d/ca.repo https://raw.githubusercontent.com/rucio/rucio/master/etc/docker/dev/ca.repo

# Install WLCG repo for CMS VOMS

RUN curl -o wlcg-repo-1.0.0-1.el7.noarch.rpm http://linuxsoft.cern.ch/wlcg/centos7/x86_64/wlcg-repo-1.0.0-1.el7.noarch.rpm && \
    yum -y install wlcg-repo-1.0.0-1.el7.noarch.rpm && \
    rm wlcg-repo-1.0.0-1.el7.noarch.rpm && \
    yum clean all && \
    rm -rf /var/cache/yum

# Install sets of RPM
RUN yum install -y which krb5-devel gridsite \
                   lcg-CA globus-proxy-utils voms-clients-java wlcg-voms-cms \
                   gfal2-python gfal2-all gfal2-util \
                   xrootd-client bind-utils nano && \
    yum clean all && \
    rm -rf /var/cache/yum

# Install DAS client to pull info from PhEDEx/DBS
RUN curl -k -o  /bin/dasgoclient https://www.lns.cornell.edu/~vk/Work/dasgoclient && chmod +x /bin/dasgoclient
RUN curl -o /etc/ca.crt https://cafiles.cern.ch/cafiles/certificates/CERN%20Root%20Certification%20Authority%202.crt

COPY scripts /scripts
COPY rucio-openstack.cfg /opt/rucio/etc/rucio.cfg
COPY rucio-openstack.cfg /opt/rucio-openstack/etc/rucio.cfg
COPY rucio-chicago.cfg /opt/rucio-chicago/etc/rucio.cfg

ENV X509_USER_PROXY=/tmp/x509up
ENTRYPOINT ["/bin/bash", "-c", "sleep infinity"]
