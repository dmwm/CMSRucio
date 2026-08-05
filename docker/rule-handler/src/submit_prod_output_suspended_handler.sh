#!/bin/bash
# shellcheck disable=SC1090
echo "=== Setting up rucio ==="
source /src/setup_rucio.sh

echo "=== Running suspended overview ==="
python3 /src/run_handler.py overview --suspended 
mkdir -p /shared
cp ./locks_suspended_rules.csv /shared/locks_suspended_rules.csv
echo "=== Running suspended output ==="
exec python3 /src/run_prod_output_suspended.py