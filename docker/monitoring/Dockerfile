# Base image with spark-submit capabilities, CMSMonitoring, CMSSpark and CERN central tools
FROM registry.cern.ch/cmsmonitoring/cmsmon-spark

# Update hadoop version
RUN mkdir -p /var/log/hbase /var/run/hbase && chmod 777 /var/log/hbase /var/run/hbase
RUN dnf install -y cern-hadoop-config spark-bin-3.5 hadoop-bin-3.3
RUN dnf install -y hbase-bin-2.5

# Set environment variables
ENV PYCURL_SSL_LIBRARY=nss \
    X509_USER_CERT=/etc/secrets/dmtops.crt.pem \
    X509_USER_KEY=/etc/secrets/dmtops.key.pem \
    USER=dmtops \
    RUCIO_ACCOUNT=transfer_ops

# Install dependencies for Rucio and DBS
RUN dnf install -y python-pycurl \
    && dnf install -y libcurl-devel openssl-devel libffi-devel \
    && dnf -y groupinstall "Development Tools" || true \
    && pip3 install --compile --global-option="--with-nss" --no-cache-dir pycurl \
    && pip3 install oracledb SQLAlchemy dbs3-client rucio-clients

# Copy source code to the image
COPY ./src /src/

# Give execution permissions
RUN chmod +x /src/sync_systems.sh
RUN chmod +x /src/submit_rucio_account_usage.sh
RUN chmod +x /src/submit_rucio_account_usage_locks.sh
RUN chmod +x /src/submit_rucio_locks_count_volume.sh
RUN chmod +x /src/submit_rucio_rules_count_volume.sh