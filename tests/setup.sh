#!/bin/bash

memcached -u root -d

echo 'Clearing memcache'
echo 'flush_all' | nc localhost 11211

echo 'Graceful restart of Apache'
httpd -k graceful

echo 'Setting up database'
python /tests/setup.py
