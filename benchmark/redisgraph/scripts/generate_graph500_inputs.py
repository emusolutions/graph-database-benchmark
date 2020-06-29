#!/usr/bin/python
# Python 2.7.X
# Version 0.1

import argparse
import os
from tqdm import tqdm
import boto3

# Read the node input file and translate the input IDs into a contiguous range.
# Then, read the relation input file and translate all source and destination node IDs
# to their updated contiguous values.

# Main Function
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate graph 500 inputs.")
    parser.add_argument(
        "--nodefile", type=str, default='graph500_22_unique_node', help="nodefile"
    )
    parser.add_argument(
        "--relfile", type=str, default='graph500_22', help="relfile"
    )
    parser.add_argument("--inputdir", type=str, default='.', help="input dir")
    parser.add_argument("--outputdir", type=str, default='.', help="output dir")
    parser.add_argument("--output_prefix", type=str, default='_out', help="datadir")

    args = parser.parse_args()

    # Create updated data directory if it doesn't exist
    try:
        os.mkdir(args.outputdir)
    except OSError:
        pass

    updated_id = 0

    relations_file = os.path.join(args.inputdir, args.relfile)
    updated_node_filename = os.path.join(args.outputdir, args.nodefile+args.output_prefix)
    updated_relation_filename = os.path.join(args.outputdir, args.relfile+args.output_prefix)

    updated_node_file = open(updated_node_filename, 'w')
    updated_node_file.write('id\n')  # Output a header row
    updated_relation_file = open(updated_relation_filename, 'w')
    updated_relation_file.write('src,dest\n')  # Output a header row
    # Map every node ID to its line number
    # and generate an updated node file.
    placement = {}
    num_lines = sum(1 for line in open(os.path.join(args.inputdir, args.nodefile)))
    with open(os.path.join(args.inputdir, args.nodefile)) as f:
        for line in tqdm(f, total=num_lines):
            placement[int(line)] = updated_id
            updated_node_file.write('%d\n' % (updated_id))
            updated_id += 1
    updated_node_file.close()
    num_lines = sum(1 for line in open(relations_file))
    with open(relations_file) as f:
        for line in tqdm(f, total=num_lines):
            # Tokenize every line and convert the data to ints
            src, dst = map(int, line.split())

            # Retrieve the updated ID of each source and destination
            a = placement[src]
            b = placement[dst]

            # Output the updated edge description
            updated_relation_file.write("%d,%d\n" % (a, b))

    updated_relation_file.close()
