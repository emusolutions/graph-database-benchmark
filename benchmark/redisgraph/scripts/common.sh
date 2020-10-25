#!/usr/bin/env bash

# Database credentials
DATABASE_HOST=${DATABASE_HOST:-"127.0.0.1"}
DATABASE_PORT=${DATABASE_PORT:-6379}

# Data folder
BULK_DATA_DIR=${BULK_DATA_DIR:-"/tmp/bulk_data"}

# Full path to data file
DATASET=${DATASET:-${BULK_DATA_DIR}/${DATASET_DIR}}

# Ensure data file is in place
if [[ ! -d ${DATASET} ]]; then
   echo "Cannot find dataset directory ${DATASET}"
   exit -1
fi

set -x
