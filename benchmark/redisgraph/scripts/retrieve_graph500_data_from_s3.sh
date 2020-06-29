#!/usr/bin/env bash

INNERPATH=${INNERPATH:-"data"}
INSTALLPATH=${INSTALLPATH:-"/data"}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/common.sh


BUCKET=performance-cto-group-public
BUCKETPATH=benchmarks/redisgraph/graph-database-benchmark/graph500/data

for datafile in graph500_22 graph500_22_unique_node; do
  echo "Getting data: $datafile"
  wget https://$BUCKET.s3.amazonaws.com/$BUCKETPATH/$datafile
  chmod 644 $datafile
  mv $datafile "${BULK_DATA_DIR}/${datafile}"
done

