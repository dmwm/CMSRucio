#!/bin/bash
# shellcheck disable=SC1090

# Load common utilities
set -e
script_dir="$(cd "$(dirname "$0")" && pwd)"
. /data/CMSSpark/bin/utils/common_utils.sh

# Initiate Kerberos Credentials
kinit -kt /etc/secrets/dmtops.keytab dmtops

# Setup Spark for a k8s cluster
util_setup_spark_k8s

spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.shuffle.useOldFetchProtocol=true --conf "spark.driver.bindAddress=0.0.0.0"
    --conf spark.shuffle.service.enabled=true --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.host=${K8SHOST}"
    --conf "spark.driver.port=${DRIVERPORT}" --conf "spark.driver.blockManager.port=${BMPORT}"
    --driver-memory=32g --num-executors 30 --executor-memory=32g --packages org.apache.spark:spark-avro_2.12:3.5.1
)

py_input_args=(--creds /etc/secrets/amq.json)
# log all to stdout
spark-submit "${spark_submit_args[@]}" "$script_dir/rucio_activity_usage.py" "${py_input_args[@]}" 2>&1
