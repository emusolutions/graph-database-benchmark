#!/usr/bin/env python

############################################################
# Copyright (c)  2015-now, TigerGraph Inc.
# All rights reserved
# It is provided as it is for benchmark reproducible purpose.
# anyone can use it for benchmark purpose with the
# acknowledgement to TigerGraph.
# Author: Mingxi Wu mingxi.wu@tigergraph.com
############################################################
import argparse
import multiprocessing
import os
import sys
from timeit import default_timer as timer

from hdrh.histogram import HdrHistogram

from query_runner import *

# Global, map of reports.
seedReports = {}


#####################################################################
# Initialize seed reporting,
# seedReports[seed][iterations] contains the number of iterations required for seed
# seedReports[seed][iterationsSummaryRuns] array of iteration matrices
#######################################################################
def InitSeedReports(seeds, iterations):
    global seedReports
    for s in seeds:
        seedReports[s] = []


#####################################################################
# Generate a report summary.
#######################################################################
def FinalizeReport(seedReports, unique_node_file, depth, threads, debug):
    # seed=19284, k=1, runId=0, avgNeighbor=91.0, execTime=0.197093009949
    # AVG Seed iterations.

    output = ''
    avgKNSize = 0
    threadsTotalRuntime = [0] * threads
    runs = 0
    histogram = HdrHistogram(1, 1 * 1000 * 1000, 4)

    for seed in seedReports:
        report = seedReports[seed]
        for iterationReport in report:
            avgNeighbor = iterationReport['avgN']
            execTime = iterationReport['totalTime']
            threadId = iterationReport['threadId']
            threadsTotalRuntime[threadId] += execTime
            histogram.record_value(execTime * 1000)
            if debug is True:
                output += "seed=%s, k=%d, avgNeighbor=%d, execTime=%f[ms]\r\n" % (seed, depth, avgNeighbor, execTime)
                output += "**************************************************************\r\n"

            avgKNSize += avgNeighbor
            runs += 1

    avgKNSize /= runs

    # We're interested in how much time did it took us to compute a single query on average
    # Our total run time equals max(threadsTotalRuntime), and we've completed running
    # N queries.
    totalRuntime = max(threadsTotalRuntime)

    output += "**************************************************************\r\n"
    output += "Summary : avgKNSize=%f, avgQueryTime=%f[ms], totalRuntime=%f[ms]\r\n" % (
        avgKNSize, histogram.get_mean_value() / 1000.0,
        totalRuntime)
    output += "Latency by percentile : q50=%f[ms], q99=%f[ms], q99.99=%f[ms], q99.9999=%f[ms], \r\n" % (
        histogram.get_value_at_percentile(50.0) / 1000.0,
        histogram.get_value_at_percentile(90.0) / 1000.0,
        histogram.get_value_at_percentile(99.99) / 1000.0,
        histogram.get_value_at_percentile(99.9999) / 1000.0)

    output += "**************************************************************\r\n"

    return output, histogram


#####################################################################
# K-hop-path-neighbor-count benchmark workload.
# (1) read prepared random nodes from a seed file under seed folder.
#######################################################################
def GetSeeds(seed_file_path, count):
    if not os.path.exists(seed_file_path):
        print("Seed file does not exists: " + seed_file_path)
        sys.exit()

    # Open seed file
    with open(seed_file_path, 'r') as f:
        pre_nodes = f.read().split()
        if len(pre_nodes) >= count:
            return pre_nodes[0:count]
        else:
            print("Seed file does not contain enough seeds.")
            sys.exit()


###############################################################
# function: thread worker, pull work item from pool
# and execute query via runner
################################################################
def RunKNLatencyThread(datadir, graphid, threadId, depth, provider, label, seedPool, reportQueue, iterations, url,
                       seed):
    if provider == "redisgraph":
        runner = RedisGraphQueryRunner(graphid, label, url)
    elif provider == "tigergraph":
        runner = TigerGraphQueryRunner()
    else:
        print("Unknown runner %s, quiting" % provider)
        sys.exit()

    # As long as there's work to do...
    while not seedPool.empty():
        # Try to get a seed from pool
        seed = None
        try:
            seed = seedPool.get(True, 2)
        except Exception as inst:
            break

        # Make sure we've got a seed.
        if not seed:
            # Failed to get seed from pool, pool probably empty
            break

        iterationSummary = {}
        start = timer()
        # the k-hop distinct neighbor size.
        knsize = runner.KN(seed, depth)
        # for timeout query, we return -1
        end = timer()
        if knsize == -1:
            iterationTime = -1
        else:
            iterationTime = end - start
            iterationTime *= 1000  # convert from seconds to ms

        iterationSummary['threadId'] = threadId
        iterationSummary['seed'] = seed
        iterationSummary['avgN'] = knsize
        iterationSummary['totalTime'] = iterationTime
        reportQueue.put(iterationSummary, False)


###############################################################
# function: check the total latency for k-hop-path neighbor count
# query for a given set of seeds.
################################################################


def RunKNLatency(data_dir, graphid, count, depth, provider, label, threads, iterations, url, seed, stdout, rules):
    # create result folder
    global seedReports
    seedfile = os.path.join(data_dir, seed)
    seeds = GetSeeds(seedfile, count)

    # Create a pool of seeds.
    seedPool = multiprocessing.Queue(len(seeds) * iterations)

    # Thread will report their output using this queue.
    reportQueue = multiprocessing.Queue(len(seeds) * iterations)

    # Add each seed to pool.
    for s in seeds:
        for iter in range(iterations):
            seedPool.put(s)

    # Initialize seed reports
    InitSeedReports(seeds, iterations)
    threadsProc = []
    for tid in range(threads):
        p = multiprocessing.Process(target=RunKNLatencyThread,
                                    args=(
                                        data_dir, graphid, tid, depth, provider, label, seedPool, reportQueue,
                                        iterations,
                                        url, seed))
        threadsProc.append(p)

    # Launch threads
    for t in threadsProc:
        t.start()

    # Wait for all threads to join
    for t in threadsProc:
        t.join()

    # Aggregate reports
    print("Aggregate reports")
    while not reportQueue.empty():
        report = reportQueue.get(False)
        seed = report['seed']
        avgN = report['avgN']
        threadId = report['threadId']
        totalTime = report['totalTime']
        seedReports[seed].append({'avgN': avgN, 'totalTime': totalTime, 'threadId': threadId})

    print("Finalizing report")
    unique_node_file = os.path.join(data_dir, label)
    output, hdrhist = FinalizeReport(seedReports, unique_node_file, depth, threads, False)

    if stdout is False:
        dirName = "./result_" + provider + "/"
        fileName = "KN-latency-k%d-threads%d-iter%d" % (depth, threads, iterations)
        outputPath = os.path.join(dirName, fileName)

        if not os.path.exists(dirName):
            os.makedirs(dirName)

        with open(outputPath, 'wt') as ofile:
            ofile.write(output)

        HDR_LOG_NAME = "latency.hdrhist"
        hdrPath = os.path.join(dirName, HDR_LOG_NAME)
        with open(hdrPath, 'wt') as hdr_log:
            hdrhist.output_percentile_distribution(hdr_log, 1000.0, 10)

    else:
        hdrhist.output_percentile_distribution(sys.stdout, 1000.0, 10)
        print output

    for rule_name, rule_value in rules.items():
        quantile = float(rule_name)
        quantile_value = hdrhist.get_value_at_percentile(quantile) / 1000
        if quantile_value > rule_value:
            print "Failing due to quantile {} latency value {}ms being larger than allowed ({})".format(quantile,
                                                                                                        quantile_value,
                                                                                                        rule_value)
            sys.exit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="check the total latency for k-hop-path neighbor count.")
    parser.add_argument(
        "--graphid", "-g", type=str, default='graph500_22', help="graph id"
    )
    parser.add_argument(
        "--count", "-c", type=int, default=1, help="number of seeds"
    )
    parser.add_argument(
        "--depth", "-d", type=int, default=1, help="number of hops to perform"
    )
    parser.add_argument(
        "--provider", "-p", type=str, default="redisgraph", help="graph identifier"
    )
    parser.add_argument(
        "--url", "-u", type=str, default="127.0.0.1:6379", help="DB url"
    )
    parser.add_argument(
        "--label", "-l", type=str, default="graph500_22_unique_node", help="node label"
    )
    parser.add_argument(
        "--seed", "-s", type=str, default="seed", help="seed file"
    )
    parser.add_argument(
        "--data_dir", type=str, default="data", help="data dir"
    )
    parser.add_argument(
        "--threads", "-t", type=int, default=1, help="number of querying threads"
    )
    parser.add_argument(
        "--iterations", "-i", type=int, default=1, help="number of iterations per query"
    )
    parser.add_argument('--stdout', dest='stdout', action='store_true', help="print report to stdout")
    parser.add_argument(
        "--fail_q50", type=float, default=sys.float_info.max, help="Fail if overall latency q50 above threshold"
    )

    args = parser.parse_args()
    rules = {'50.0': args.fail_q50}
    RunKNLatency(args.data_dir, args.graphid, args.count, args.depth, args.provider, args.label, args.threads,
                 args.iterations, args.url, args.seed, args.stdout, rules)
