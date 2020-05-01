#!/bin/bash

function abacus {
  rucio-abacus-rse --run-once
  rucio-abacus-account --run-once
  rucio-abacus-collection-replica --run-once
}

python wmagent_injection.py
rucio-judge-evaluator --run-once
abacus
rucio-conveyor-submitter --run-once
rucio-conveyor-poller --run-once --older-than 0
rucio-conveyor-finisher --run-once
