#!/usr/bin/env bash

set -e

if [ -n "$NEURO_CLUSTER" ]
then
    neuro config switch-cluster "$NEURO_CLUSTER" 1>/dev/null
fi

if [[ -n "$GOOGLE_APPLICATION_CREDENTIALS" ]];
then
    gcloud auth activate-service-account --key-file "$GOOGLE_APPLICATION_CREDENTIALS"
fi

exec "$@"
