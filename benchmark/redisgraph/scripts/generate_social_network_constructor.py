#!/usr/bin/python
# Python 3.X
# Version 0.1

import argparse
import csv
import json
import random
import string
import boto3
from common import generate_setup_json, compress_files, generate_inputs_dict_item
from tqdm import tqdm
import sys

# Read the node input file and translate the input IDs into a contiguous range.
# Then, read the relation input file and translate all source and destination node IDs
# to their updated contiguous values.



# create node query
create_node_query = "CYPHER id={id} coins=891807823 firstName={id} lastName={id} image={id} xp=164 villageXp=859 villageLevel=83 fbId={id} attackableStructures=5 isFtueDone=false boardTotalUpgrades=84 name={id} availableAttacks=10 MERGE (u :User {{id: $id}}) SET u.id = $id, u.coins = $coins, u.firstName = $firstName, u.lastName = $lastName, u.image = $image, u.xp = $xp, u.villageXp = $villageXp, u.villageLevel = $villageLevel, u.fbId = $fbId, u.attackableStructures = $attackableStructures, u.isFtueDone = $isFtueDone, u.boardTotalUpgrades = $boardTotalUpgrades, u.name = $name, u.availableAttacks = $availableAttacks"
create_relation_query = "CYPHER userId={id} friends=[{ids}] MATCH (u: User {{ id: $userId}}) MATCH (uu :User) WHERE uu.fbId IN $friends WITH u, uu MERGE (uu)-[:fbFriend]->(u)-[:fbFriend]->(uu) RETURN uu.id"


# Main Function
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate social network facebook games queries.")
    parser.add_argument('--batch-size', type=int, default=80,
                            help='batch size')
    parser.add_argument('--prefix', type=str, default="abc",
                                help='prefix')
    parser.add_argument('--workers-count', type=int, default=100,
                                help='workers count')
    parser.add_argument('--graph-key-name', type=str, default="userFriends",
                        help='the name of the key containing the graph')
    parser.add_argument('--total-benchmark-commands', type=int, default=10000,
                        help='the total commands to generate to be issued in the benchmark stage')
    parser.add_argument('--test-name', type=str, default="social-network-builder", help='the name of the test')
    parser.add_argument('--test-description', type=str,
                        default="Benchmark focused on update performance.",
                        help='the full description of the test')
    parser.add_argument('--benchmark-output-file-prefix', type=str, default="social-network-builder.redisgraph.commands",
                        help='prefix to be used when generating the artifacts')
    parser.add_argument('--benchmark-config-file', type=str, default="social-network-builder.redisgraph.cfg.json",
                        help='name of the output config file used to store the full benchmark suite steps and description')
    parser.add_argument('--upload-artifacts-s3', default=False, action='store_true',
                        help="uploads the generated dataset files and configuration file to public benchmarks.redislabs bucket. Proper credentials are required")
    args = parser.parse_args()
    use_case_specific_arguments = dict(args.__dict__)
    del use_case_specific_arguments["upload_artifacts_s3"]
    del use_case_specific_arguments["test_name"]
    del use_case_specific_arguments["test_description"]
    del use_case_specific_arguments["benchmark_config_file"]
    del use_case_specific_arguments["benchmark_output_file_prefix"]

    test_name = args.test_name
    description = args.test_description
    benchmark_output_file = args.benchmark_output_file_prefix
    benchmark_config_file = args.benchmark_config_file
    graph_key_name = args.graph_key_name
    used_keys = [graph_key_name]
    setup_commands = []
    teardown_commands = []
    total_benchmark_commands = args.total_benchmark_commands
    key_metrics = [
        {
            "step": "benchmark",
            "metric-family": "throughput",
            "metric-json-path": "OverallRates.overallOpsRate",
            "metric-name": "Overall Read query rate",
            "unit": "queries/sec",
            "metric-type": "numeric",
            "comparison": "higher-better",
            "per-step-comparison-metric-priority": 1,
        }, {
            "step": "benchmark",
            "metric-family": "latency",
            "metric-json-path": "OverallQuantiles.allCommands.q50",
            "metric-name": "Overall Commands query q50 latency",
            "unit": "ms",
            "metric-type": "numeric",
            "comparison": "lower-better",
            "per-step-comparison-metric-priority": 2,
        },
    ]

    s3_bucket_name = "benchmarks.redislabs"
    s3_bucket_path = "redisgraph/datasets/{}/".format(test_name)
    s3_uri = "https://s3.amazonaws.com/{bucket_name}/{bucket_path}".format(bucket_name=s3_bucket_name,
                                                                           bucket_path=s3_bucket_path)
    bench_fname = "{}.BENCH.csv".format(benchmark_output_file)
    bench_fname_compressed = "{}.BENCH.tar.gz".format(benchmark_output_file)
    remote_url_bench = "{}{}".format(s3_uri, bench_fname_compressed)
    json_version = "0.1"
    benchmark_repetitions_require_teardown_and_resetup = False

    all_nodes = {}
    relations = []
    setup_queries = []
    print("Generating setup queries")
    print("Reading relations file")


    seed_count     =  args.workers_count  *  args.batch_size
    seeds = ["'" + args.prefix + str(s) + "'" for s in range(0, seed_count)]

#     with open(args.relations_file) as f:
    idx = 0
    batch_size = args.batch_size
    workers_count = args.workers_count

    for i in range(0, workers_count):
        ## node creation
        for j in range(0,batch_size):
            seed = seeds[idx]
            q = create_node_query.format(id=seed)
            cmd = ["WRITE", "NODE", "GRAPH.QUERY", graph_key_name, q]
            setup_queries.append(cmd)

            idx = idx + 1

        ## relations creation
        idx = idx - batch_size
        batch_number = i
        sub_seeds_start = batch_number * batch_size
        sub_seeds_end = sub_seeds_start + batch_size
        for j in range(0,batch_size):
            seed = seeds[idx]
            q = create_relation_query.format(id=seed, ids=','.join(seeds[sub_seeds_start:sub_seeds_end]))
            cmd = ["WRITE", "RELATIONS", "GRAPH.QUERY", graph_key_name, q]
            setup_queries.append(cmd)
            idx = idx + 1
#             batch_number = int(i/batch_size)


#
# #             q = create_relation_query.format(id=seed, ids=','.join(seeds[sub_seeds_start:sub_seeds_end]))
# #             print q

#            setup_queries.append(cmd)
#         for line in tqdm(f.readlines()):
#             src_dest_nodes = line.strip().split(" ")
#             src_node_id = src_dest_nodes[0]
#             dest_node_id = src_dest_nodes[1]
#             relations.append({"src_node":src_node_id,"dest_node":dest_node_id})
#
#             if src_node_id not in all_nodes:
#                 cypher = generate_node(src_node_id)
#                 all_nodes[src_node_id]=cypher
#
#                 setup_queries.append(cmd)
#             if dest_node_id not in all_nodes:
#                 cypher = generate_node(dest_node_id)
#                 all_nodes[dest_node_id]=cypher
#                 all_nodes[src_node_id]=cypher
#                 cmd = ["SETUP_WRITE", "NODE", "GRAPH.QUERY", graph_key_name, cypher]


#
    all_csvfile = open(bench_fname, 'w', newline='')
    all_csv_writer = csv.writer(all_csvfile, delimiter=',')
    for query in tqdm(setup_queries):
        all_csv_writer.writerow(query)
    all_csvfile.close()

