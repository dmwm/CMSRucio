#!/bin/bash
# shellcheck disable=SC1090
set -eo pipefail
cd /src
echo "=== Setting up rucio ==="
source /src/setup_rucio.sh

echo "=== Running stuck overview ==="
python3 /src/run_handler.py overview --account wmcore_output

python3 -c "import send_os; send_os.post_overview(state='stuck',account='wmcore_output')"

echo "=== Running suspended overview ==="
python3 /src/run_handler.py overview --suspended --account wmcore_output

python3 -c "import send_os; send_os.post_overview(state='suspended',account='wmcore_output')"

#mkdir -p /shared
#cp ./locks_suspended_rules.csv /shared/locks_suspended_rules.csv
#echo "=== Running suspended output ==="
#exec python3 /src/run_prod_output_suspended.py