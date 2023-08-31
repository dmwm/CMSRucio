FROM centos:7
USER root

RUN yum install -y epel-release.noarch\
    && yum clean all \
    && rm -rf /var/cache/yum

# PKI stuff
RUN yum install -y https://repo.opensciencegrid.org/osg/3.5/osg-3.5-el7-release-latest.rpm\
    && yum clean all \
    && rm -rf /var/cache/yum

RUN yum install -y osg-pki-tools\
    && yum clean all \
    && rm -rf /var/cache/yum

#RUN rpm -i http://mirror.grid.uchicago.edu/pub/osg/3.3/el7/testing/x86_64/voms-2.0.14-1.3.osg33.el7.x86_64.rpm
RUN rpm -i http://mirror.grid.uchicago.edu/pub/osg/3.5/el7/release/x86_64/voms-2.0.14-1.6.osg35.el7.x86_64.rpm\
    && yum clean all \
    && rm -rf /var/cache/yum

RUN rpm -i http://mirror.grid.uchicago.edu/pub/osg/3.5/el7/release/x86_64/voms-clients-cpp-2.0.14-1.6.osg35.el7.x86_64.rpm\
    && yum clean all \
    && rm -rf /var/cache/yum


RUN echo 1
    
# this will install /etc/grid-security/certificates

RUN yum install -y osg-ca-certs\
    && yum clean all \
    && rm -rf /var/cache/yum

# Oracle client
RUN yum install -y libaio\
    && yum clean all \
    && rm -rf /var/cache/yum

RUN rpm -i https://download.oracle.com/otn_software/linux/instantclient/19600/oracle-instantclient19.6-basic-19.6.0.0.0-1.x86_64.rpm

# xrootd client
RUN curl -o /etc/yum.repos.d/xrootd-stable-slc7.repo https://xrootd.slac.stanford.edu/binaries/xrootd-stable-slc7.repo
RUN yum install -y xrootd-libs xrootd-client\
    && yum clean all \
    && rm -rf /var/cache/yum

# jobber
RUN rpm -i https://github.com/dshearer/jobber/releases/download/v1.4.0/jobber-1.4.0-1.el7.x86_64.rpm

# Python and libs
WORKDIR /root
RUN curl -o Miniconda3-latest-Linux-x86_64.sh \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh \
    && echo PATH="/root/miniconda3/bin":$PATH >> .bashrc 

ENV PATH=/root/miniconda3/bin:${PATH}

RUN pip3 install --upgrade pip
RUN pip3 install --upgrade setuptools
RUN conda install -c anaconda cx_oracle
RUN pip3 --no-cache-dir install SQLAlchemy pyyaml pythreader j2cli 
RUN pip3 install rucio-clients rucio-consistency

RUN mkdir -p /consistency
RUN mkdir /root/RAL
COPY vomses /etc
COPY cleanup.sh run.sh site.sh unmerged_site.sh RAL_Disk_pre.sh RAL_Disk_post.sh  RAL_Tape_pre.sh RAL_Tape_post.sh /consistency/

ADD rucio.cfg.j2 /tmp

ADD rucio-client.cfg /consistency/rucio-client.cfg

WORKDIR /consistency
RUN chmod +x *.sh

RUN git clone https://github.com/ivmfnal/cms_consistency.git

CMD /bin/bash


