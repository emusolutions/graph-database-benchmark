
This article documents the details on how to reproduce the graph database benchmark result on RedisGraph.

# Installing redisgraph-database-benchmark

```
git clone https://github.com/RedisGraph/graph-database-benchmark.git
cd graph-database-benchmark/benchmark/redisgraph
make
```

# Benchmark
## One liner benchmark 

Assuming you have the graph previously populated ( see dataset section ) and  redisgraph-database-benchmark installed, for a default set of 100K queries,  80% 1 hop, 15% 2 hops, 5 % 3 hops queries, run:
```
redisgraph-database-benchmark --input k-hop.redisgraph.commands.BENCH.csv --json-out-file=bench.output.json
```

# Current use case: k-hop

Currently, RedisGraph benchmark supports one use case:

Name | Description and Source | Vertices | Edges
-- | -- | -- | --
k-hop | Synthetic Kronecker graph http://graph500.org | 2.4 M | 64 M

The K-hop neighbourhood count benchmark, is focused on READ performance, and contains queries that count the number of nodes a single start node (seed) is connected to at a certain depth. Specifically for this use case we’ll vary 1, 2, and 3-hops matches, each hop with a specific query percentage.  

The hops query percentage can be adjusted.  For a sample of each benchmark, query see the Sample benchmark queries section.


## Dataset
To be able to properly run this test suite you need the dataset previously loaded, which can be achieved either by loading a previously populated graph ( RDB loading ) or by bulk loading the data.  

### Option 1: Load a prepopulated RDB

In order to load a prepopulated RDB, you should download the  RDB file containing the graph and move it towards redis-server working directory.

#### Standalone OSS Redis and RedisGraph
```
wget https://s3.amazonaws.com/benchmarks.redislabs/redisgraph/datasets/k-hop/graph500_s22_dump.rdb 
mv graph500_s22_dump.rdb dump.rdb
redis-server --loadmodule <redisgraph.so>
```
#### Standalone containerized OSS RedisGraph
```
wget https://s3.amazonaws.com/benchmarks.redislabs/redisgraph/datasets/k-hop/graph500_s22_dump.rdb
mv graph500_s22_dump.rdb dump.rdb
docker run --volume="$PWD/":/data/:rw -p 6379:6379 redislabs/redisgraph:edge  redis-server --loadmodule /usr/lib/redis/modules/redisgraph.so
```

### Option 2: Bulk load the data

#### Install redisgraph-bulk-loader
```
python3 -m pip install redisgraph-bulk-loader==0.9.0
```

#### Clone  graph-database-benchmark repo and retrieve the nodes and relations files 
```
# retrieve the nodes csv file
wget https://s3.amazonaws.com/benchmarks.redislabs/redisgraph/datasets/k-hop/graph500_s22_nodes.csv

# retrieve the relations csv file
wget https://s3.amazonaws.com/benchmarks.redislabs/redisgraph/datasets/k-hop/graph500_s22_relations.csv
```

#### Load the data 
```
redisgraph-bulk-loader graph500_22 --nodes-with-label node graph500_s22_nodes.csv \
                                   --relations graph500_s22_relations.csv \
                                   --max-buffer-size 512 --max-token-size 32
```

#### Create index on label node
```
redis-cli graph.query graph500_22 "create index on :node(id)"
```

## Query set

The query set file contains queries that count the number of nodes a single start node (seed) is connected to at a certain depth. Specifically for CI test suite, we’ll vary 1, 2, and 3-hops matches, each hop with a specific query percentage, in the following manner:

- 1-hop query: 80% of the total issued queries
- 2-hops query: 15% of the total issued queries
- 3-hops query: 5% of the total issued queries

The hops query percentage can be adjusted.  For a sample of each query see the Sample benchmark queries subscection bellow:

### Sample benchmark queries

#### 1-hop match by internal autonumeric node id
```
CYPHER param1=<SEED> MATCH (n)-[*1]->(m) WHERE id(n) =$param1 RETURN count(distinct m);
```

#### 2-hops match by internal autonumeric node id
```
CYPHER param1=<SEED> MATCH (n)-[*2]->(m) WHERE id(n) =$param1 RETURN count(distinct m);
```

#### 3-hops match by internal autonumeric node id
```
CYPHER param1=<SEED> MATCH (n)-[*3]->(m) WHERE id(n) =$param1 RETURN count(distinct m);
```

### Generating the query set file

```
git clone https://github.com/RedisGraph/graph-database-benchmark.git
cd graph-database-benchmark/benchmark/redisgraph/scripts
python3 -m pip install -r requirements.txt

# retrieve the nodes csv file if you don't have it already
wget https://s3.amazonaws.com/benchmarks.redislabs/redisgraph/datasets/k-hop/graph500_s22_nodes.csv

python3 generate_graph500_queries.py --nodes-file graph500_s22_nodes.csv
```

#### Expected output:
```
/go/src/github.com/RedisGraph/graph-database-benchmark/benchmark/redisgraph/scripts$ python3 generate_graph500_queries.py --nodes-file graph500_s22_nodes.csv
Using 3 K-hops with the following probabilities 1-hop 80.0% 2-hop 15.0% 3-hop 5.0%
Using random seed 12345
Reading unique node ids
100%|████████████████████████████████████████████████████████| 2396020/2396020 [00:01<00:00, 2040058.92it/s]
Generating k-hop queries
100%|███████████████████████████████████████████████████████████| 100000/100000 [00:00<00:00, 145764.26it/s]
100%|███████████████████████████████████████████████████████████| 100000/100000 [00:00<00:00, 275907.83it/s]
```
