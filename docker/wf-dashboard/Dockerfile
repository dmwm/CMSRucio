# syntax=docker/dockerfile:1
FROM almalinux:9
ENV WDIR=/data
RUN dnf -y install python3 python3-pip
RUN pip install opensearch-py pandas pycurl rucio-clients tqdm
RUN mkdir $WDIR 
WORKDIR $WDIR
ENV X509_USER_PROXY=/opt/proxy/x509up
RUN dnf -y install git
RUN git clone https://github.com/emcgrady/WFDashboard.git
RUN git clone https://github.com/dmwm/CMSSpark.git
RUN pip install -r CMSSpark/requirements.txt
ENV PYTHONPATH="${PYTHONPATH}:CMSSpark/src/python/CMSSpark"
RUN cp WFDashboard/monit_pull.py $WDIR
ADD rucio.cfg /opt/rucio/etc/
ADD entrypoint.sh /
RUN chmod +x /entrypoint.sh
RUN chmod +x monit_pull.py
ENTRYPOINT ["/entrypoint.sh"]
