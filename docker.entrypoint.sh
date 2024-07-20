#!/usr/bin/env bash

set -e

if [ -n "$APOLO_CLUSTER" ]
then
    apolo config switch-cluster "$APOLO_CLUSTER" 1>/dev/null
fi

if [[ -n "$GOOGLE_APPLICATION_CREDENTIALS" ]];
then
    gcloud auth activate-service-account --key-file "$GOOGLE_APPLICATION_CREDENTIALS"
fi

exec "$@"
