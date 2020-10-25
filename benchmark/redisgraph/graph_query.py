#!/usr/bin/python
# Python 2.7.X
# Version 0.1

import argparse

import redis

# Main Function
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RedisGraph Query simple script.")
    parser.add_argument(
        "--host", type=str, default="127.0.0.1", help="Redis server host"
    )
    parser.add_argument("--port", type=int, default=6379, help="Redis server port")
    parser.add_argument(
        "--password", type=str, default=None, help="Redis server password"
    )
    parser.add_argument("--name", type=str, required=True, help="Graph name")
    parser.add_argument("--query", type=str, required=True, help="Query")
    args = parser.parse_args()

    # Attempt to connect to Redis server
    try:
        client = redis.StrictRedis(
            host=args.host, port=args.port, password=args.password
        )
    except redis.exceptions.ConnectionError as e:
        print("Could not connect to Redis server.")
        raise e

    # Attempt to verify that RedisGraph module is loaded
    try:
        module_list = client.execute_command("GRAPH.QUERY", args.name, args.query)
    except redis.exceptions.ResponseError as e:
        raise e
