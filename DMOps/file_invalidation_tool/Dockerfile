FROM registry.cern.ch/cmsmonitoring/cmsmon-spark:latest

# Set environment variables
ENV PYCURL_SSL_LIBRARY=nss \
    X509_USER_CERT=/certs/usercert.pem \
    X509_USER_KEY=/certs/userkey.pem \
    USER=dmtops \
    RUCIO_CONFIG=/cvmfs/cms.cern.ch/rucio/rucio.cfg \
    RUCIO_ACCOUNT=transfer_ops \
    DRIVERPORT=5001 \
    BMPORT=5002 \
    UIPORT=5003

ADD http://repository.egi.eu/sw/production/cas/1/current/repo-files/egi-trustanchors.repo /etc/yum.repos.d/egi.repo


# Install dependencies for Rucio, DBS and Gfal
RUN dnf install -y libcurl-devel openssl-devel libffi-devel ca-policy-egi-core\
    && dnf install -y cmake gfal2-devel libcurl-devel \
    && dnf install -y gfal2-all python3-gfal2 python3-gfal2-util \
    && dnf -y groupinstall "Development Tools" || true \
    && pip3 install cx-Oracle SQLAlchemy==1.4.49 dbs3-client rucio-clients \
    && pip3 install --compile --global-option="--with-nss" --no-cache-dir pycurl

# Expose ports
EXPOSE 5001
EXPOSE 5002
EXPOSE 5003

# Copy code
COPY ./src /src/

# Set working directory
WORKDIR /src

# Set entrypoint
ENTRYPOINT ["python3", "run_invalidations.py"]