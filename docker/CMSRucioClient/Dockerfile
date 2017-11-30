FROM centos:7

RUN curl -o /etc/yum.repos.d/ca.repo https://raw.githubusercontent.com/rucio/rucio/master/etc/docker/dev/ca.repo

RUN yum -y update && yum install -y ca-certificates.noarch epel-release.noarch && yum clean all
RUN yum install -y python-pip gcc krb5-devel python-devel.x86_64 gridsite lcg-CA globus-proxy-utils voms-clients-cpp && yum clean all

RUN pip install rucio-clients-cms

COPY rucio.cfg /opt/rucio/etc/rucio.cfg
RUN curl -o /etc/ca.crt https://cafiles.cern.ch/cafiles/certificates/CERN%20Root%20Certification%20Authority%202.crt

ENV X509_USER_PROXY=/tmp/x509up
