#!/usr/bin/env bash

# Data folder
DATASET_NAME=${DATASET_NAME:-"twitter_rv"}
EDGE_FILE=${EDGE_FILE:-"http://service.tigergraph.com/download/benchmark/dataset/twitter/twitter_rv.tar.gz"}
NODE_FILE=${NODE_FILE:-"http://service.tigergraph.com/download/benchmark/dataset/twitter/twitter_rv.net_unique_node"}
BULK_DATA_DIR=${BULK_DATA_DIR:-"/tmp/bulk_data"}

# Ensure DATA DIR available
mkdir -p ${BULK_DATA_DIR}
chmod a+rwx ${BULK_DATA_DIR}

# Ensure DATASET DIR available
mkdir -p ${BULK_DATA_DIR}/${DATASET_NAME}
chmod a+rwx ${BULK_DATA_DIR}/${DATASET_NAME}

DATA_EDGE_FILE_NAME=${BULK_DATA_DIR}/${DATASET_NAME}/twitter_rv
DATA_NODE_FILE_NAME=${BULK_DATA_DIR}/${DATASET_NAME}/twitter_rv.net_unique_node
DATA_SEED_FILE_NAME=${BULK_DATA_DIR}/${DATASET_NAME}/twitter_rv.net_seed

cp twitter_rv.net_seed ${DATA_SEED_FILE_NAME}

echo ""
echo "---------------------------------------------------------------------------------"
echo "Retrieving Edge File ${EDGE_FILE}"
echo "---------------------------------------------------------------------------------"

if [ ! -f ${DATA_EDGE_FILE_NAME} ]; then
  echo "${EDGE_FILE} not found locally at ${DATA_EDGE_FILE_NAME}. Retrieving..."
  curl -O ${EDGE_FILE} | tar zxf -  >  ${DATA_EDGE_FILE_NAME}
else
  echo "Dataset found locally at ${DATA_EDGE_FILE_NAME}. No need to retrieve again."
fi

echo ""
echo "---------------------------------------------------------------------------------"
echo "Retrieving Node File ${NODE_FILE}"
echo "---------------------------------------------------------------------------------"

if [ ! -f ${DATA_NODE_FILE_NAME} ]; then
  echo "${NODE_FILE} not found locally at ${DATA_NODE_FILE_NAME}. Retrieving..."
  curl ${NODE_FILE} > ${DATA_NODE_FILE_NAME}
else
  echo "Dataset found locally at ${DATA_NODE_FILE_NAME}. No need to retrieve again."
fi
