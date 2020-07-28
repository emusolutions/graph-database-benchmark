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

# IS3 query
IS3_query = "MATCH (n:Person {{id: {personId} }})-[r:KNOWS]-(friend) RETURN friend.id AS personId, friend.firstName AS firstName, friend.lastName AS lastName, r.creationDate AS friendshipCreationDate ORDER BY friendshipCreationDate DESC, toInteger(friend.id) ASC"

# Main Function
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate social network facebook games queries.")
    parser.add_argument('--graph-key-name', type=str, default="graph",
                        help='the name of the key containing the graph')
    parser.add_argument(
            "--person-file", type=str, default='person_0_0.csv', help="ldbc person file"
        )
    parser.add_argument('--total-benchmark-commands', type=int, default=1000000,
                        help='the total commands to generate to be issued in the benchmark stage')
    parser.add_argument('--test-name', type=str, default="ldbc-is3-no-params", help='the name of the test')
    parser.add_argument('--test-description', type=str,
                        default="Benchmark focused on read performance.",
                        help='the full description of the test')
    parser.add_argument('--benchmark-output-file-prefix', type=str, default="ldbc-is3-no-params.redisgraph.commands",
                        help='prefix to be used when generating the artifacts')
    parser.add_argument('--benchmark-config-file', type=str, default="lsbc-is3-no-params.redisgraph.cfg.json",
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
    ids = []
    setup_queries = []
    print("Reading person file")

    with open(args.person_file) as csvfile:
        csvreader = csv.reader(csvfile, delimiter='|')
        next(csvreader)
        for row in csvreader:
            ids.append(row[0])

    print("Generating read queries")
    total_ids = len(ids)
    for cmd_number in tqdm(range(1,args.total_benchmark_commands+1)):
        person_id = ids[total_ids%cmd_number-1]
        q = IS3_query.format(personId=person_id)
        cmd = ["READ", "IS3", "GRAPH.QUERY", graph_key_name, q]
        setup_queries.append(cmd)

    all_csvfile = open(bench_fname, 'w', newline='')
    all_csv_writer = csv.writer(all_csvfile, delimiter=',')
    for query in tqdm(setup_queries):
        all_csv_writer.writerow(query)
    all_csvfile.close()
