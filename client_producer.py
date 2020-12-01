import zmq
import time
import sys
from itertools import cycle
from consistent_hashing import ConsistentHashing
from hrw import HrwHashing
import json


def create_clients(servers):
    producers = {}
    context = zmq.Context()
    for server in servers:
        print(f"Creating a server connection to {server}...")
        producer_conn = context.socket(zmq.DEALER)
        producer_conn.bind(server)
        producers[server] = producer_conn
    return producers


def generate_data_round_robin(servers):
    print("\n***Starting...Round Robin***\n")
    producers = create_clients(servers)
    pool = cycle(producers.values())
    print("\n")
    for num in range(10):
        data = {"op": "PUT", "key": f"key-{num}", "value": f"value-{num}"}
        print(f"Sending data:{data}")
        server = next(pool)  #  Getting server by round-robin
        server.send_multipart([b"", json.dumps(data).encode("utf-8")])
        server.recv_multipart()
        time.sleep(1)
    clear_data(producers)  # Clears data on the server for future tests
    print("\nDone\n")


### Start of Consistent hashing Implementation


def generate_data_consistent_hashing(servers):
    print("\n***Starting...Consistent Hashing***\n")
    ## TODO
    producers = create_clients(servers)
    print("\nSetting up the HashRing...")
    ch.add_servers(servers)
    print("\n")
    for num in range(10):
        data = {"op": "PUT", "key": f"key-{num}", "value": f"value-{num}"}
        print(f"Sending data:{data}")
        server = ch.get_server(str(num))  # Getting server based on consistent hashing
        print(
            "This key will be stored by server ", server, " based on consistent hashing"
        )
        producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
        producers[server].recv_multipart()

    time.sleep(1)

    consistent_hashing_tests(producers)  # Tests GET_ONE, GET_ALL for consistent hashing
    clear_data(producers)  # Clears data on the server for future tests

    print("\nDone\n")


def get_by_key_consistent_hashing(num, producers):
    print("Get by key ", num)
    data = {"op": "GET_ONE", "key": f"key-{num}"}
    server = ch.get_server(str(num))  # Getting server based on consistent hashing
    print(
        "This key will be retreived by server ", server, " based on consistent hashing"
    )
    producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
    resp = producers[server].recv_multipart()
    print(json.dumps(json.loads(resp[1].decode("utf-8")), indent=2))


def get_all_consistent_hashing(producers):
    data = {"op": "GET_ALL"}
    servers = ch.get_server_list()  # Getting all servers in the consistent hashing ring
    for server in servers:
        print("Getting all from server " + server)
        producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
        resp = producers[server].recv_multipart()
        print(json.dumps(json.loads(resp[1].decode("utf-8")), indent=2))


def consistent_hashing_tests(producers):
    print("\nGetting data by key")
    get_by_key_consistent_hashing(3, producers)
    get_by_key_consistent_hashing(6, producers)
    get_by_key_consistent_hashing(8, producers)

    print("\nGetting All keys")
    get_all_consistent_hashing(producers)


### End of Consistent hashing Implementation


### Start of HRW Hashing Implementation


def generate_data_hrw_hashing(servers):
    print("\n***Starting...HRW hashing***\n")
    ## TODO
    producers = create_clients(servers)
    print("\nSetting up server pool...")
    hrw.add_servers(servers)
    print("\n")
    for num in range(10):
        data = {"op": "PUT", "key": f"key-{num}", "value": f"value-{num}"}
        print(f"Sending data:{data}")
        server = hrw.get_server(num)  # Getting appropriate server based on HRW hashing
        print("This key will be stored by server " + server + " based on HRW hashing")
        producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
        producers[server].recv_multipart()

    hrw_hashing_tests(producers)  # Tests GET_ONE and GET_ALL for HRW hashing
    clear_data(producers)  # Clears data on the server for future tests

    print("\nDone\n")


def get_by_key_hrw_hashing(num, producers):
    print("Get by key ", num)
    data = {"op": "GET_ONE", "key": f"key-{num}"}
    server = hrw.get_server(num)  # Getting appropriate server based on HRW hashing
    print("This key will be retreived by server " + server + " based on HRW hashing")
    producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
    resp = producers[server].recv_multipart()
    print(json.dumps(json.loads(resp[1].decode("utf-8")), indent=2))


def get_all_hrw_hashing(producers):
    data = {"op": "GET_ALL"}
    servers = hrw.get_server_list()  # Getting all servers in the HRW hash pool
    for server in servers:
        print("Getting all from server " + server)
        producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
        resp = producers[server].recv_multipart()
        print(json.dumps(json.loads(resp[1].decode("utf-8")), indent=2))


def hrw_hashing_tests(producers):

    time.sleep(1)
    print("\nGetting data by key")
    get_by_key_hrw_hashing(2, producers)
    get_by_key_hrw_hashing(5, producers)
    get_by_key_hrw_hashing(9, producers)

    print("\nGetting All keys")
    get_all_hrw_hashing(producers)


### End of HRW Hashing Implementation


def clear_data(producers):
    data = {"op": "RESET"}
    for server in servers:
        producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
        producers[server].recv_multipart()


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