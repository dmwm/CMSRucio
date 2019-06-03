#! /bin/bash
# 
# Created by N. Ratnikova on June 3rd, 2019
#
# get_triggers.sh 
#    - extracts source code of all triggers from the Oracle DB instance to STDOUT
#    with modifications to create/update these triggers
#    - requires a file containing database connection parameters in format suitable for 
#    sqlplus query, e.g.:
# cms_rucio_dev_admin/passwordline@int2r
# 
# Usage:  get_triggers.sh <db-connection-file>
#  

usage() { 
  echo 'Usage:  get_triggers.sh <db-connection-file>';
}

# if no arguments supplied, display usage 
if [  $# -le 0 ]
  then
    usage
    exit 2
fi

if [ -r "$1" ]
then
  secret=$1
else
  echo "ERROR: can't read the file $1"
  exit 1
fi

# check whether user had supplied -h or --help . If yes display usage 
if [[ ( $# == "--help") ||  $# == "-h" ]] 
then 
  usage
  exit 0
fi 

conn='sqlplus64 -S '`cat $secret`

$conn @names.sql |awk '$NF>0'| while read t
do 
  $conn @source.sql $t | \
    awk '$NF>0'| \
    sed 's|^TRIGGER|CREATE OR REPLACE TRIGGER|'
  echo "/"
done
