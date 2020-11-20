import zmq
import time
import sys
from itertools import cycle
from consistent_hashing import ConsistentHashing
from hrw import HrwHashing


def create_clients(servers):
    producers = {}
    context = zmq.Context()
    for server in servers:
        print(f"Creating a server connection to {server}...")
        producer_conn = context.socket(zmq.PUSH)
        producer_conn.bind(server)
        producers[server] = producer_conn
    return producers


def generate_data_round_robin(servers):
    print("\n***Starting...Round Robin***\n")
    producers = create_clients(servers)
    pool = cycle(producers.values())
    print("\n")
    for num in range(10):
        data = {"key": f"key-{num}", "value": f"value-{num}"}
        print(f"Sending data:{data}")
        next(pool).send_json(data)
        time.sleep(1)
    print("\nDone\n")


def generate_data_consistent_hashing(servers):
    print("\n***Starting...Consistent Hashing***\n")
    ## TODO
    producers = create_clients(servers)
    print("\nSetting up the HashRing...")
    ch.add_servers(servers)
    print("\n")
    for num in range(10):
        data = {"key": f"key-{num}", "value": f"value-{num}"}
        print(f"Sending data:{data}")
        server = ch.get_server(str(num))  # Getting server based on consistent hashing
        producers[server].send_json(data)
        time.sleep(1)
    print("\nDone\n")


def generate_data_hrw_hashing(servers):
    print("\n***Starting...HRW hashing***\n")
    ## TODO
    producers = create_clients(servers)
    print("\nSetting up server pool...")
    hrw.add_servers(servers)
    print("\n")
    for num in range(10):
        data = {"key": f"key-{num}", "value": f"value-{num}"}
        print(f"Sending data:{data}")
        server = hrw.get_server(num)  # Getting appropriate server based on HRW hashing
        producers[server].send_json(data)
        time.sleep(1)
    print("\nDone\n")


if __name__ == "__main__":
    servers = []
    num_server = 1
    if len(sys.argv) > 1:
        num_server = int(sys.argv[1])
        print(f"num_server={num_server}")

    for each_server in range(num_server):
        server_port = "200{}".format(each_server)
        servers.append(f"tcp://127.0.0.1:{server_port}")

    print("Servers:", servers)
    generate_data_round_robin(servers)
    ch = ConsistentHashing()
    generate_data_consistent_hashing(servers)
    hrw = HrwHashing()
    generate_data_hrw_hashing(servers)
