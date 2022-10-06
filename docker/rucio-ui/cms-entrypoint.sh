#! /bin/bash

# Add the policy package directory to PYTHONPATH
if [ ! -z "$POLICY_PKG_PATH" ]; then
    export PYTHONPATH=${POLICY_PKG_PATH}:${PYTHONPATH:+:}${PYTHONPATH}
fi

/docker-entrypoint.sh

