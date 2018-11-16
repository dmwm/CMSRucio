FROM rucio/rucio-server

RUN yum install -y mlocate wget bind-utils nano git && \
    yum clean all && \
    rm -rf /var/cache/yum

# Pull some usefule stuff out of git

WORKDIR /root

RUN git clone https://github.com/rucio/rucio.git
RUN git clone https://github.com/dmwm/CMSRucio.git

ADD https://raw.githubusercontent.com/dmwm/CMSRucio/master/docker/CMSRucioClient/rucio-openstack.cfg /opt/rucio/etc/rucio.cfg

ENTRYPOINT ["/bin/bash", "-c", "sleep infinity"]
