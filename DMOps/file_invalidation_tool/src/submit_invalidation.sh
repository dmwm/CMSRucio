#!/bin/bash
# Display usage instructions
display_usage() {
    echo "Usage: $0 <submit_script> <filename> [--rse <RSE>]"
    echo "  submit_script: Specify the execution level ('container', 'dataset', or 'file')."
    echo "  filename:      Name of the file to process."
    echo "  --rse <RSE>:   (Optional) Specify the RSE."
}

# Ensure minimum number of arguments
if [ $# -lt 2 ]; then
    display_usage
    exit 1
fi

# Initialize variables
exec_script=""
filename=""
rse=""

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --rse)
            rse="$2"
            shift 2
            ;;
        *)
            if [ -z "$exec_script" ]; then
                exec_script="$1"
            elif [ -z "$filename" ]; then
                filename="$1"
            else
                display_usage
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate required arguments
if [ -z "$exec_script" ] || [ -z "$filename" ]; then
    display_usage
    exit 1
fi

# Validate execution level
valid_options=("container" "dataset" "file")
if [[ ! " ${valid_options[@]} " =~ " ${exec_script} " ]]; then
    echo "Error: 'submit_script' must be one of: container, dataset, or file." >&2
    exit 1
fi

# Set the Python script to execute
exec_script="${exec_script}_invalidation_spark.py"

# Load utilities and set environment
set -e
script_dir="$(cd "$(dirname "$0")" && pwd)"
. /data/CMSSpark/bin/utils/common_utils.sh

# Authenticate using Kerberos
kinit -kt "/secrets/${USER}.keytab" "${USER}"

# Configure Hadoop
source hadoop-setconf.sh analytix 3.2

# Spark submit arguments
spark_submit_args=(
    --master yarn
    --conf spark.ui.showConsoleProgress=false
    --conf spark.shuffle.useOldFetchProtocol=true
    --conf spark.shuffle.service.enabled=true
    --conf "spark.driver.bindAddress=0.0.0.0"
    --conf spark.driver.host="$(hostname)"
    --conf "spark.driver.port=${DRIVERPORT}"
    --conf "spark.driver.blockManager.port=${BMPORT}"
    --conf "spark.ui.port=${UIPORT}"
    --driver-memory 32g
    --num-executors 30
    --executor-memory 32g
    --packages org.apache.spark:spark-avro_2.12:3.4.0
)

# Prepare input arguments for Python script
py_input_args=(--filename "$filename")
if [ -n "$rse" ]; then
    py_input_args+=(--rse "$rse")
fi

# Ensure HDFS is prepared
hdfs_file="hdfs://analytix/user/${USER}/${filename}"
if /usr/hdp/hadoop/bin/hdfs dfs -test -e "$hdfs_file"; then
    /usr/hdp/hadoop/bin/hdfs dfs -rm "$hdfs_file"
fi
/usr/hdp/hadoop/bin/hdfs dfs -cp "file:///input/${filename}" "$hdfs_file"

# Submit Spark job
spark-submit "${spark_submit_args[@]}" "$script_dir/$exec_script" "${py_input_args[@]}" 2>&1

# Clean up HDFS
/usr/hdp/hadoop/bin/hdfs dfs -rm "$hdfs_file"