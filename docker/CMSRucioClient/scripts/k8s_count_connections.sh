#! /bin/bash

cd /root/CMSRucio/docker/CMSRucioClient/scripts/

sql $LOGON @./${INSTANCE}_connection_count.sql

./report_db_connections.py
