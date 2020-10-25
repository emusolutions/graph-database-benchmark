import os
import sys

# Read the node input file and translate the input IDs into a contiguous range.
# Then, read the relation input file and translate all source and destination node IDs
# to their updated contiguous values.

# User-provided input data directory
if len(sys.argv) < 2 or os.path.exists(sys.argv[1]) is False:
    print("Usage: generate_inputs.py [path_to_inputs]")
    exit(1)

inputdir = sys.argv[1]

# Input filenames
nodefile = 'twitter_rv.net_unique_node'
relfile = 'twitter_rv.net'
seedfile = 'twitter_rv.net-seed'

# Output data directory
datadir = 'data'

# Create updated data directory if it doesn't exist
try:
    os.mkdir(datadir)
except OSError:
    pass

updated_id = 0

updated_node_file = open(os.path.join(datadir, nodefile.replace('.', '_')), 'w')
updated_node_file.write('id\n')  # Output a header row
updated_relation_file = open(os.path.join(datadir, relfile.replace('.', '_')), 'w')
updated_seed_file = open(os.path.join(datadir, seedfile.replace('.', '_')), 'w')

# Map every node ID to its line number
# and generate an updated node file.
placement = {}
with open(os.path.join(inputdir, nodefile)) as f:
    for line in f:
        placement[int(line)] = updated_id
        updated_node_file.write('%d\n' % (updated_id))
        updated_id += 1

with open(os.path.join(inputdir, relfile)) as f:
    for line in f:
        # Tokenize every line and convert the data to ints
        src, dst = map(int, line.split())

        # Retrieve the updated ID of each source and destination
        a = placement[src]
        b = placement[dst]

        # Output the updated edge description
        updated_relation_file.write("%d,%d\n" % (a, b))

with open(os.path.join(inputdir, seedfile)) as f:
    updated_seed_file.write(' '.join(str(placement[int(i)]) for i in f.read().split()))

updated_node_file.close()
updated_relation_file.close()
updated_seed_file.close()
