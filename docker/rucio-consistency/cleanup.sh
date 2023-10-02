#! /bin/sh

find /var/cache/consistency-dump -mtime +45 -exec rm {} \;
find /var/cache/consistency-temp -mtime +30 -exec rm {} \;

