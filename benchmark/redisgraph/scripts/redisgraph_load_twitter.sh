#!/usr/bin/env bash
DATASET_DIR=${DATASET_DIR:-"twitter_rv_net"}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/common.sh

## Prepare input files in RedisGraph bulk import format
#python generate_twitter_inputs.py ${DATASET} || exit 1

# Run RedisGraph bulk import script
python bulk_insert.py twitter_rv_net -n data/twitter_rv_net_unique_node -r data/twitter_rv_net \
  --host ${DATABASE_HOST} --port ${DATABASE_PORT} || exit 1

# Create index on node ID property
python graph_query.py --name "twitter_rv_net" --query "create index on :twitter_rv_net_unique_node(id)" \
  --host ${DATABASE_HOST} --port ${DATABASE_PORT}
