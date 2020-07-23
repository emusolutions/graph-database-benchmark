
import argparse
import csv
import json
import random
import string
from tqdm import tqdm
import redis
import redis
from redisgraph import Node, Edge, Graph, Path

def get_random_string(length):
    # Random string with the combination of lower and upper case
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

 # "CYPHER id='ckbyiunu30dbe0iuw9yca99dkR0c' fbId='10218476531848057' isFtueDone=true image='f:10218476531848057' country='NL' cardsXp=240 boardXp=120 villageXp=677 xp=1037 spins=170 shields=3 coins=3055720 dice=0 diceRefills=1594020222169 spinsRefills=1594019954282 villageLevel=27 counter=12867 userId='ck8tvg9a102nw0p1hbq7q9wukf9f' firstName='Reina' lastName='Vd Molen' name='Reina Vd Molen' on_state_updated=2848 MATCH (u :User {id: \$id}) WHERE u.on_state_updated < 12867 OR u.on_state_updated IS NULL SET u.id = \$id, u.coins = \$coins, u.firstName = \$firstName, u.lastName = \$lastName, u.image = \$image, u.xp = \$xp, u.villageXp = \$villageXp, u.villageLevel = \$villageLevel, u.fbId = \$fbId, u.isFtueDone = \$isFtueDone, u.name = \$name, u.on_state_updated = \$on_state_updated"

def generate_node(node_id):
    firstName = get_random_string(10)
    lastName = get_random_string(10)
    name = firstName + " " + lastName
    on_state_updated = 1
    coins = 1
    image = 'f:'+str(node_id)
    xp = 1
    villageXp = 1
    villageLevel = 1
    fbId = node_id
    isFtueDone = 'true'
    properties = { 'name':name, 'id':str(node_id), 'firstName':firstName,'lastName':lastName,'on_state_updated':on_state_updated,'coins':coins,'image':image,'xp':xp,'villageXp':villageXp,'villageLevel':villageLevel,'fbId':fbId,'isFtueDone':isFtueDone }

    properties_s = "name: '{0}', id: '{1}', firstName: '{2}', lastName: '{3}', on_state_updated: '{4}', coins: '{5}', ".format(name,node_id,firstName,lastName,on_state_updated,coins)
    properties_s += "image: '{0}', xp: '{1}', villageXp: '{2}', villageLevel: '{3}', fbId: '{4}', isFtueDone: '{5}'".format(image,xp,villageXp,villageLevel,fbId,isFtueDone)
    cypher = "(u:User { " + properties_s + " })"

    return properties, cypher

# Main Function
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate social network facebook games queries.")
    parser.add_argument(
        "--relations-file", "-n", type=str, default='facebook_combined.txt', help="relations file"
    )
    parser.add_argument('--seed', type=int, default=12345,
                        help='the random seed used to generate random deterministic outputs')
    parser.add_argument('--graph-key-name', type=str, default="social",
                        help='the name of the key containing the graph')
    parser.add_argument('--host', type=str, default="127.0.0.1",
                        help='the Redis Host')
    parser.add_argument('--port', type=int, default=6379,
                        help='the Redis Port')
    args = parser.parse_args()
    graph_key_name = args.graph_key_name

    print("Using random seed {0}".format(args.seed))
    random.seed(args.seed)

    all_nodes = {}
    all_nodes_cypher = {}
    print("Generating graph")
    print("Reading relations file")

    r = redis.Redis(host='localhost', port=6379)
    redis_graph = Graph(args.graph_key_name, r)

    with open(args.relations_file) as f:
        for line in tqdm(f.readlines()):
            src_dest_nodes = line.strip().split(" ")
            src_node_id = src_dest_nodes[0]
            dest_node_id = src_dest_nodes[1]

            if src_node_id not in all_nodes:
                properties, src_cypher = generate_node(src_node_id)
                src = Node(node_id=src_node_id, label='User', properties=properties)
                all_nodes[src_node_id] = src
                all_nodes_cypher[src_node_id] = src_cypher
                redis_graph.add_node(src)
                redis_graph.flush()

            else:
                src = all_nodes[src_node_id]
                src_cypher = all_nodes_cypher[src_node_id]

            if dest_node_id not in all_nodes:
                properties, dst_cypher = generate_node(dest_node_id)
                dst = Node(node_id=dest_node_id, label='User', properties=properties)
                all_nodes[dest_node_id] = dst
                all_nodes_cypher[dest_node_id] = dst_cypher
                redis_graph.add_node(dst)
                redis_graph.flush()

            else:
                dst = all_nodes[dest_node_id]
                dst_cypher = all_nodes_cypher[dest_node_id]

            query = "MATCH (a:User),(b:User) WHERE a.id = '{}' AND b.id = '{}' CREATE (a)-[:fbFriend]->(b)".format(src_node_id,dest_node_id)
            result = redis_graph.query(query)



