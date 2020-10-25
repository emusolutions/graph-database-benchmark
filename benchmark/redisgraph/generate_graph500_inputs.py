#!/usr/bin/python
# Python 2.7.X
# Version 0.1

import argparse
import os

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
    parser.add_argument("--seedfile", type=str, default='graph500_22_seed', help="seed file")
    parser.add_argument("--inputdir", type=str, default='.', help="input dir")
    parser.add_argument("--datadir", type=str, default='data', help="datadir")
    args = parser.parse_args()

    # Create updated data directory if it doesn't exist
    try:
        os.mkdir(args.datadir)
    except OSError:
        pass

    updated_id = 0

    updated_node_file = open(os.path.join(args.datadir, args.nodefile), 'w')
    updated_node_file.write('id\n')  # Output a header row
    updated_relation_file = open(os.path.join(args.datadir, args.relfile), 'w')
    updated_seed_file = open(os.path.join(args.datadir, args.seedfile), 'w')

    # Map every node ID to its line number
    # and generate an updated node file.
    placement = {}
    with open(os.path.join(args.inputdir, args.nodefile)) as f:
        for line in f:
            placement[int(line)] = updated_id
            updated_node_file.write('%d\n' % (updated_id))
            updated_id += 1

    with open(os.path.join(args.inputdir, args.relfile)) as f:
        for line in f:
            # Tokenize every line and convert the data to ints
            src, dst = map(int, line.split())

            # Retrieve the updated ID of each source and destination
            a = placement[src]
            b = placement[dst]

            # Output the updated edge description
            updated_relation_file.write("%d,%d\n" % (a, b))

    with open(os.path.join(args.inputdir, args.seedfile)) as f:
        updated_seed_file.write(' '.join(str(placement[int(i)]) for i in f.read().split()))

    updated_node_file.close()
    updated_relation_file.close()
    updated_seed_file.close()
