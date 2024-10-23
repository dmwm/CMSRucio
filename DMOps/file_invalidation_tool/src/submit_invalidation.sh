#!/bin/bash
# Function to display script usage

display_usage() {
    echo "Usage: $0 <submit_script> <filename> [--rse <RSE>]"
}

# Check if number of arguments is less than required
if [ $# -lt 2 ]; then
    display_usage
    exit 1
fi

# Initialize variables
exec_script=""
filename=""
rse=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --rse)
            shift
            rse="$1"
            shift
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

# Check if required arguments are provided
if [ -z "$exec_script" ] || [ -z "$filename" ]; then
    display_usage
    exit 1
fi

# Check if execution script is DID level is correct
valid_options=("container" "dataset" "file")

if [[ ! " ${valid_options[@]} " =~ " ${exec_script} " ]]; then
    echo "Error: param1 must be either 'container' or 'file' to define invalidation level" >&2
    exit 1
fi

exec_script="${exec_script}_invalidation_spark.py"

# Load common utilities
set -e
script_dir="$(cd "$(dirname "$0")" && pwd)"
. /data/CMSSpark/bin/utils/common_utils.sh

# Initiate Kerberos Credentials
kinit -kt /secrets/dmtops.keytab dmtops

source hadoop-setconf.sh analytix 3.2

spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.shuffle.useOldFetchProtocol=true
    --conf spark.shuffle.service.enabled=true --conf "spark.driver.bindAddress=0.0.0.0"
    --conf spark.driver.host=$(hostname)
    --conf "spark.driver.port=${DRIVERPORT}" --conf "spark.driver.blockManager.port=${BMPORT}"
    --conf "spark.ui.port=${UIPORT}"
    --driver-memory=32g --num-executors 30 --executor-memory=32g --packages org.apache.spark:spark-avro_2.12:3.4.0
)

if [ -n "$rse" ]; then
    py_input_args=(--filename "$filename" --rse "$rse")
else
    py_input_args=(--filename "$filename")
fi

#Delete file in case of error last time running
/usr/hdp/hadoop/bin/hdfs dfs -test -e "hdfs://analytix/user/dmtops/$filename" && /usr/hdp/hadoop/bin/hdfs dfs -rm "hdfs://analytix/user/dmtops/$filename"

#Add file to HDFS (required for file reading)
/usr/hdp/hadoop/bin/hdfs dfs -cp "file:///input/$filename" "hdfs://analytix/user/dmtops/$filename"

# log all to stdout
spark-submit "${spark_submit_args[@]}" "$script_dir/$exec_script" "${py_input_args[@]}" 2>&1
#Remove file from HDFS
/usr/hdp/hadoop/bin/hdfs dfs -rm "hdfs://analytix/user/dmtops/$filename"
