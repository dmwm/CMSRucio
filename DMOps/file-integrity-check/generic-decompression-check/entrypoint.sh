#!/bin/bash
set -e

# Initialize proxy with the provided certificates
chmod 400 /certs/usercert.pem 2>/dev/null || true
chmod 400 /certs/userkey.pem 2>/dev/null || true

voms-proxy-init -voms cms -rfc -valid 192:00 \
  --cert /certs/usercert.pem \
  --key /certs/userkey.pem

# Run the main application
python3 run_check.py "$@"