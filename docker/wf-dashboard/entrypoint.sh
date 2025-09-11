#! /bin/sh
ls -l /opt
ls -l /opt/proxy
python3 monit_pull.py
sleep 3600