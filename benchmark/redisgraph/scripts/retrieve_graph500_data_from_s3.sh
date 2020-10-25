#!/usr/bin/env bash

INNERPATH=${INNERPATH:-"data"}
INSTALLPATH=${INSTALLPATH:-"/data"}

# Ensure DATA DIR available
mkdir -p ${INSTALLPATH}
chmod a+rwx ${INSTALLPATH}

BUCKET=performance-cto-group-public
BUCKETPATH=benchmarks/redisgraph/graph-database-benchmark/graph500/data

for datafile in graph500_22 graph500_22_seed graph500_22_unique_node; do
  echo "Getting data: $datafile"
  wget https://$BUCKET.s3.amazonaws.com/$BUCKETPATH/$datafile
  chmod 644 $datafile
  mv $datafile "${INSTALLPATH}/${datafile}"
done

