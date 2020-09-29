#!/usr/bin/env bash

set -e

if [ -n "$NEURO_CLUSTER" ]
then
    neuro config switch-cluster "$NEURO_CLUSTER" 1>/dev/null
fi

exec "$@"
