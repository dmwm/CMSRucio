# Base image with spark-submit capabilities, CMSMonitoring, CMSSpark and CERN central tools
FROM registry.cern.ch/cmsmonitoring/cmsmon-spark

# Update hadoop version
RUN mkdir -p /var/log/hbase /var/run/hbase && chmod 777 /var/log/hbase /var/run/hbase

# Set environment variables
ENV PYCURL_SSL_LIBRARY=nss \
    X509_USER_CERT=/etc/secrets/dmtops.crt.pem \
    X509_USER_KEY=/etc/secrets/dmtops.key.pem \
    USER=dmtops \
    RUCIO_ACCOUNT=transfer_ops \
    PYTHONPATH="${PYTHONPATH}:CMSSpark/src/python/CMSSpark"

ADD http://repository.egi.eu/sw/production/cas/1/current/repo-files/egi-trustanchors.repo /etc/yum.repos.d/egi.repo

# Install dependencies for Rucio and DBS
RUN dnf --disablerepo=hdp9al-stable install -y python-pycurl \
    && dnf --disablerepo=hdp9al-stable install -y libcurl-devel openssl-devel libffi-devel \
    && dnf -y groupinstall "Development Tools" || true \
    && pip3 install --compile --global-option="--with-nss" --no-cache-dir pycurl \
    && pip3 install oracledb SQLAlchemy dbs3-client rucio-clients opensearch-py tqdm

# Copy source code to the image
COPY ./src /src/

# Give execution permissions
RUN chmod +x /src/submit_rucio_account_usage.sh
RUN chmod +x /src/submit_rucio_activity_usage.sh
RUN chmod +x /src/submit_rucio_account_usage_locks.sh
RUN chmod +x /src/submit_rucio_activity_account_usage.sh
RUN chmod +x /src/submit_rucio_locks_count_volume.sh
RUN chmod +x /src/submit_rucio_rules_count_volume.sh
RUN chmod +x /src/submit_rucio_errors_volume.sh
RUN chmod +x /src/submit_rucio_subscriptions.sh
RUN chmod +x /src/run_rucio_summary.sh
RUN chmod +x /src/run_wf_dashboard.sh