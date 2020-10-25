#!/usr/bin/env bash

INNERPATH=${INNERPATH:-"data"}

# upload to public s3 bucket the desired data
BUCKETPATH=performance-cto-group-public/benchmarks/redisgraph/graph-database-benchmark/graph500/rdb
for datafile in graph500.rdb; do
  echo "uploading $datafile to $BUCKETPATH/"
  FULLPATH="${INNERPATH}/${datafile}"
  echo "Uploading Binary: $FULLPATH to s3://$BUCKETPATH/${datafile}"
  aws s3 cp $FULLPATH s3://$BUCKETPATH/${datafile} --acl public-read
done
