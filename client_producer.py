import zmq
import time
import sys
from itertools import cycle
import consistent_hashing
from hrw import HrwHashing
import json
import consul
import requests


def create_clients(servers):
    context = zmq.Context()
    for server in servers:
        print(f"Creating a server connection to {server}...")
        producer_conn = context.socket(zmq.DEALER)
        producer_conn.bind(server)
        producers[server] = producer_conn
    return producers


def generate_data_round_robin(servers):
    pool = cycle(producers.values())
    print("\n")
    for num in range(10):
        data = {"op": "PUT", "key": f"key-{num}", "value": f"value-{num}"}
        print(f"Sending data:{data}")
        server = next(pool)  #  Getting server by round-robin
        server.send_multipart([b"", json.dumps(data).encode("utf-8")])
        server.recv_multipart()
        time.sleep(1)
    clear_data()  # Clears data on the server for future tests
    print("\nDone\n")


### Start of Consistent hashing Implementation


def generate_data_consistent_hashing(servers):
    ## TODO
    print("\nSetting up the HashRing...")
    ch.add_servers(servers)
    print("\n")
    for num in range(100):
        data = {"op": "PUT", "key": f"key-{num}", "value": f"value-{num}"}
        print(f"Sending data:{data}")
        assign_data_consistent_hashing(data, num)

    time.sleep(1)

    consistent_hashing_tests()  # Tests GET_ONE, GET_ALL for consistent hashing
    clear_data()  # Clears data on the server for future tests

    print("\nDone\n")


def assign_data_consistent_hashing(data, num):
    server = ch.get_server(str(num))  # Getting server based on consistent hashing
    print("This key will be stored by server ", server, " based on consistent hashing")
    producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
    producers[server].recv_multipart()


def get_by_key_consistent_hashing(num):
    print("\nGet by key ", num)
    data = {"op": "GET_ONE", "key": f"key-{num}"}
    server = ch.get_server(str(num))  # Getting server based on consistent hashing
    print(
        "This key will be retreived by server ", server, " based on consistent hashing"
    )
    producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
    resp = producers[server].recv_multipart()
    print(json.dumps(json.loads(resp[1].decode("utf-8")), indent=2))


def get_all_consistent_hashing():
    data = {"op": "GET_ALL"}
    servers = ch.get_server_list()  # Getting all servers in the consistent hashing ring
    for server in servers:
        print("\nGetting all from server " + server)
        producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
        resp = producers[server].recv_multipart()
        print(json.dumps(json.loads(resp[1].decode("utf-8")), indent=2))


def copy_data(server):
    data = {"op": "GET_ALL"}
    producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
    resp = producers[server].recv_multipart()
    return json.loads(resp[1].decode("utf-8"))["collection"]


def reBalance(copyOfData):
    for item in copyOfData:
        key = ""
        for each in item:
            key = each
        value = item[key]
        data = {"op": "PUT", "key": key, "value": value}
        num = key.split("-")[1]
        assign_data_consistent_hashing(data, num)


def consistent_hashing_tests():

    # Testing GET_ONE
    print("\n***GETTING DATA BY KEY***")
    get_by_key_consistent_hashing(3)
    get_by_key_consistent_hashing(6)
    get_by_key_consistent_hashing(8)

    # Testing GET_ALL
    print("\n***GETTING ALL KEYS***")
    get_all_consistent_hashing()

    # TESTING SERVER REMOVAL
    print("\n***REMOVING SERVER tcp://127.0.0.1:2001 AND SENDING SIGNAL TO COSUL***")

    copyOfData = copy_data("tcp://127.0.0.1:2001")

    ch.delete_server("tcp://127.0.0.1:2001")
    requests.put("http://127.0.0.1:2101/v1/agent/leave")  # SENDING SIGNAL TO CONSUL

    print("\n***REBALANCING AFTER SERVER DELETION***")

    reBalance(copyOfData)

    print("\n***GETTING ALL KEYS AFTER SERVER DELETION***")

    get_all_consistent_hashing()


### End of Consistent hashing Implementation


### Start of HRW Hashing Implementation


def generate_data_hrw_hashing(servers):
    ## TODO
    print("\nSetting up server pool...")
    hrw.add_servers(servers)
    print("\n")
    for num in range(100):
        data = {"op": "PUT", "key": f"key-{num}", "value": f"value-{num}"}
        print(f"Sending data:{data}")
        server = hrw.get_server(num)  # Getting appropriate server based on HRW hashing
        print("This key will be stored by server " + server + " based on HRW hashing")
        producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
        producers[server].recv_multipart()

    hrw_hashing_tests()  # Tests GET_ONE and GET_ALL for HRW hashing
    clear_data()  # Clears data on the server for future tests

    print("\nDone\n")


def get_by_key_hrw_hashing(num):
    print("\nGet by key ", num)
    data = {"op": "GET_ONE", "key": f"key-{num}"}
    server = hrw.get_server(num)  # Getting appropriate server based on HRW hashing
    print("This key will be retreived by server " + server + " based on HRW hashing")
    producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
    resp = producers[server].recv_multipart()
    print(json.dumps(json.loads(resp[1].decode("utf-8")), indent=2))


def get_all_hrw_hashing():
    data = {"op": "GET_ALL"}
    servers = hrw.get_server_list()  # Getting all servers in the HRW hash pool
    for server in servers:
        print("\nGetting all from server " + server)
        producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
        resp = producers[server].recv_multipart()
        print(json.dumps(json.loads(resp[1].decode("utf-8")), indent=2))


def hrw_hashing_tests():

    time.sleep(1)
    # Testing GET_ONE
    print("\n***GETTING DATA BY KEY***")
    get_by_key_hrw_hashing(2)
    get_by_key_hrw_hashing(5)
    get_by_key_hrw_hashing(9)

    # Testing GET_ALL
    print("\n***GETTING ALL KEYS***")
    get_all_hrw_hashing()


### End of HRW Hashing Implementation


def clear_data():
    data = {"op": "RESET"}
    for server in servers:
        producers[server].send_multipart([b"", json.dumps(data).encode("utf-8")])
        producers[server].recv_multipart()


def get_producers():
    return producers


if __name__ == "__main__":
    servers = []

    ports = (
        consul.get_members()
    )  # Getting member addresses from consul to shard data between them

    for port in ports:
        servers.append(f"tcp://127.0.0.1:{port}")

    print("Servers:", servers)
    producers = {}

    # print("\n*******STARTING...ROUND ROBIN*******\n")
    # producers = create_clients(servers)
    # generate_data_round_robin(servers)
    # producers.clear()

    print("\n*******STARTING...CONSISTENT HASHING*******\n")
    producers = create_clients(servers)
    ch = consistent_hashing.ConsistentHashing()
    generate_data_consistent_hashing(servers)
    producers.clear()

    print("\n*******STARTING...HRW HASHING*******\n")
    producers = create_clients(servers)
    hrw = HrwHashing()
    generate_data_hrw_hashing(servers)
    producers.clear()