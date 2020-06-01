#!/bin/bash

function abacus {
  # run all accounting daemons
  rucio-abacus-rse --run-once
  rucio-abacus-account --run-once
  rucio-abacus-collection-replica --run-once
}

function transfer {
  # run transfer daemon sequence
  rucio-conveyor-submitter --run-once
  rucio-conveyor-poller --run-once --older-than 0
  rucio-conveyor-finisher --run-once
}

python subscriptions.py
python wmagent_injection.py
rucio-judge-evaluator --run-once
rucio-transmogrifier --run-once
abacus
transfer

if [[ ! $(rucio list-rules --account transfer_ops --csv |wc -l) == 4 ]];
then
  echo "Did not find the expected number of subscription-generated rules"
  exit 1
fi
