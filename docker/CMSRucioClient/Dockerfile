FROM centos:7

# Get various repos for yum setup
RUN yum -y install https://ecsft.cern.ch/dist/cvmfs/cvmfs-release/cvmfs-release-latest.noarch.rpm && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN curl -o /etc/yum.repos.d/ca.repo https://raw.githubusercontent.com/rucio/rucio/master/etc/docker/dev/ca.repo

RUN yum -y update && \
    yum install -y ca-certificates.noarch epel-release.noarch && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN curl -o wlcg-repo-1.0.0-1.el7.noarch.rpm http://linuxsoft.cern.ch/wlcg/centos7/x86_64/wlcg-repo-1.0.0-1.el7.noarch.rpm && \
    yum -y install wlcg-repo-1.0.0-1.el7.noarch.rpm && \
    rm wlcg-repo-1.0.0-1.el7.noarch.rpm && \
    yum clean all && \
    rm -rf /var/cache/yum

# Install sets of RPM
RUN yum install -y which python-pip gcc krb5-devel python-devel.x86_64 gridsite \
                   lcg-CA globus-proxy-utils voms-clients-cpp wlcg-voms-cms \
                   gfal2-python gfal2-all gfal2-util \
                   xrootd-client && \
    yum clean all && \
    rm -rf /var/cache/yum
#RUN yum install -y  && yum clean all


# Get the latest setuptools version
# to fix the setup.py error:
# install fails with: `install_requires` must be a string or list of strings

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools

RUN pip install flask
RUN pip install rucio-clients-cms

# Install DAS client to pull info from PhEDEx/DBS
RUN curl -k -o  /bin/dasgoclient https://www.lns.cornell.edu/~vk/Work/dasgoclient && chmod +x /bin/dasgoclient
RUN curl -o /etc/ca.crt https://cafiles.cern.ch/cafiles/certificates/CERN%20Root%20Certification%20Authority%202.crt

COPY scripts /scripts
COPY rucio-chicago.cfg /opt/rucio/etc/rucio.cfg
COPY rucio-chicago.cfg /opt/rucio-chicago/etc/rucio.cfg
COPY rucio-fermicloud.cfg /opt/rucio-fermicloud/etc/rucio.cfg

ENV X509_USER_PROXY=/tmp/x509up
