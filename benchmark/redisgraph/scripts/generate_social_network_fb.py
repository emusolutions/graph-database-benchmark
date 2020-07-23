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

# Read the node input file and translate the input IDs into a contiguous range.
# Then, read the relation input file and translate all source and destination node IDs
# to their updated contiguous values.

def get_random_string(length):
    # Random string with the combination of lower and upper case
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def generate_node(node_id):
    name = get_random_string(20)
    firstName = get_random_string(10)
    lastName = get_random_string(10)
    on_state_updated = 1
    coins = 1
    image = get_random_string(256)
    xp = 1
    villageXp = 1
    villageLevel = 1
    fbId = 1
    isFtueDone = 1

    properties = "name: '{0}', id: '{1}', firstName: '{2}', lastName: '{3}', on_state_updated: '{4}', coins: '{5}', ".format(name,node_id,firstName,lastName,on_state_updated,coins)
    properties += "image: '{0}', xp: '{1}', villageXp: '{2}', villageLevel: '{3}', fbId: '{4}', isFtueDone: '{5}'".format(image,xp,villageXp,villageLevel,fbId,isFtueDone)
    cypher = "merge (u:User { " + properties + " })"
    return cypher

# Main Function
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate social network facebook games queries.")
    parser.add_argument(
        "--relations-file", "-n", type=str, default='facebook_combined.txt', help="relations file"
    )
    parser.add_argument('--seed', type=int, default=12345,
                        help='the random seed used to generate random deterministic outputs')
    parser.add_argument('--graph-key-name', type=str, default="graph_social_network",
                        help='the name of the key containing the graph')
    parser.add_argument('--total-benchmark-commands', type=int, default=10000,
                        help='the total commands to generate to be issued in the benchmark stage')
    parser.add_argument('--k-hop-depths', type=str, default="1,2,3",
                        help='comma separated full list of K-hop depths to simulate queries for. Needs to have the same number of elements as --k-hop-depths-probability')
    parser.add_argument('--k-hop-depths-probability', type=str, default="0.80,0.15,0.05",
                        help='comma separated probability of the list of  K-hop depths passed via --k-hop-depths. Needs to have the same number of elements as --k-hop-depths')
    parser.add_argument('--test-name', type=str, default="social-network-games", help='the name of the test')
    parser.add_argument('--test-description', type=str,
                        default="Benchmark focused on update performance.",
                        help='the full description of the test')
    parser.add_argument('--benchmark-output-file-prefix', type=str, default="social-network-games.redisgraph.commands",
                        help='prefix to be used when generating the artifacts')
    parser.add_argument('--benchmark-config-file', type=str, default="social-network-games.redisgraph.cfg.json",
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

    k_hop_depths = args.k_hop_depths.split(",")
    k_hop_depths_p = [float(x) for x in args.k_hop_depths_probability.split(",")]

    k_hop_depths_p_str = []
    for idx, country in enumerate(k_hop_depths):
        k_hop_depths_p_str.append("{}-hop {}%".format(country, k_hop_depths_p[idx] * 100.0))
    print("Using {0} K-hops with the following probabilities {1}".format(len(k_hop_depths),
                                                                         " ".join(k_hop_depths_p_str)))
    print("Using random seed {0}".format(args.seed))
    random.seed(args.seed)

    all_nodes = {}
    relations = []
    setup_queries = []
    print("Generating setup queries")
    print("Reading relations file")
    with open(args.relations_file) as f:
        for line in tqdm(f.readlines()):
            src_dest_nodes = line.strip().split(" ")
            src_node_id = src_dest_nodes[0]
            dest_node_id = src_dest_nodes[1]
            relations.append({"src_node":src_node_id,"dest_node":dest_node_id})

            if src_node_id not in all_nodes:
                cypher = generate_node(src_node_id)
                all_nodes[src_node_id]=cypher
                cmd = ["SETUP_WRITE", "NODE", "GRAPH.QUERY", graph_key_name, cypher]
                setup_queries.append(cmd)
            if dest_node_id not in all_nodes:
                cypher = generate_node(dest_node_id)
                all_nodes[dest_node_id]=cypher
                all_nodes[src_node_id]=cypher
                cmd = ["SETUP_WRITE", "NODE", "GRAPH.QUERY", graph_key_name, cypher]
                setup_queries.append(cmd)

#     queries = []
#     print("Generating merge queries")
#     for query_number in tqdm(range(1, args.total_benchmark_commands + 1)):
#         seed = random.choices(unique_nodes_list, k=1)[0]
#         k_hop = random.choices(k_hop_depths, weights=k_hop_depths_p, k=1)[0]
#         cypher = "CYPHER param1={seed} MATCH (n)-[*{k_hop}]->(m) WHERE id(n) =$param1 RETURN count(distinct m);".format(
#             seed=seed, k_hop=k_hop)
#         cmd = ["READ", "{}-HOP".format(k_hop), "GRAPH.QUERY", graph_key_name, cypher]
#         queries.append(cmd)
#
    all_csvfile = open(bench_fname, 'w', newline='')
    all_csv_writer = csv.writer(all_csvfile, delimiter=',')
    for query in tqdm(setup_queries):
        all_csv_writer.writerow(query)
    all_csvfile.close()
#
#     total_commands = total_benchmark_commands
#     total_setup_commands = 0
#     total_setup_writes = 0
#     total_writes = 0
#     total_updates = 0
#     total_reads = total_benchmark_commands
#     total_deletes = 0
#
#     cmd_category_benchmark = {
#         "setup-writes": 0,
#         "writes": 0,
#         "updates": 0,
#         "reads": total_benchmark_commands,
#         "deletes": 0,
#     }
#
#     status, uncompressed_size, compressed_size = compress_files([bench_fname], bench_fname_compressed)
#     inputs_entry_benchmark = generate_inputs_dict_item("benchmark", bench_fname,
#                                                        "contains only the benchmark commands (required the dataset to have been previously populated)",
#                                                        remote_url_bench, uncompressed_size, bench_fname_compressed,
#                                                        compressed_size, total_benchmark_commands,
#                                                        cmd_category_benchmark)
#
#     inputs = {"benchmark": inputs_entry_benchmark}
#     deployment_requirements = {"utilities": {"redisgraph-database-benchmark": {}},
#                                "benchmark-tool": "redisgraph-database-benchmark",
#                                "redis-server": {"modules": {"graph": {}}}}
#
#     run_stages = ["benchmark"]
#     with open(benchmark_config_file, "w") as setupf:
#         setup_json = generate_setup_json(json_version, use_case_specific_arguments, test_name, description,
#                                          run_stages,
#                                          deployment_requirements,
#                                          key_metrics, inputs,
#                                          setup_commands,
#                                          teardown_commands,
#                                          used_keys,
#                                          total_commands,
#                                          total_setup_commands,
#                                          total_benchmark_commands, total_setup_writes, total_writes, total_updates,
#                                          total_reads,
#                                          total_deletes,
#                                          benchmark_repetitions_require_teardown_and_resetup,
#                                          None,
#                                          ["benchmark"]
#                                          )
#         json.dump(setup_json, setupf, indent=2)
#
#     if args.upload_artifacts_s3:
#         print("-- uploading dataset to s3 -- ")
#         s3 = boto3.resource('s3')
#         bucket = s3.Bucket(s3_bucket_name)
#         artifacts = [benchmark_config_file, bench_fname_compressed]
#         progress = tqdm(unit="files", total=len(artifacts))
#         for input in artifacts:
#             object_key = '{bucket_path}{filename}'.format(bucket_path=s3_bucket_path, filename=input)
#             bucket.upload_file(input, object_key)
#             object_acl = s3.ObjectAcl(s3_bucket_name, object_key)
#             response = object_acl.put(ACL='public-read')
#             progress.update()
#         progress.close()
