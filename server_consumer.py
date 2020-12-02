import zmq
import sys
from multiprocessing import Process
import json
import consul
import subprocess
import time


def server(port):
    context = zmq.Context()
    consumer = context.socket(zmq.REP)
    consumer.connect(f"tcp://127.0.0.1:{port}")
    dataStore = {}

    while True:
        msg = consumer.recv()
        raw = json.loads(msg.decode("utf-8"))
        op = raw["op"]
        print(f"Server_port={port}: operation={op}")
        # FIXME: Implement to store the key-value data.
        if op == "PUT":
            key, value = raw["key"], raw["value"]
            print(f"key={key}, value={value}")
            dataStore[key] = value
            consumer.send(b"OK")
        elif op == "GET_ONE":
            key = raw["key"]
            print(f"key={key}")
            data = {key: dataStore[key]}
            consumer.send(json.dumps(data).encode("utf-8"))
        elif op == "GET_ALL":
            allDataDict = {}
            allDataList = []
            for item in dataStore:
                data = {item: dataStore[item]}
                allDataList.append(data)
            allDataDict["collection"] = allDataList
            consumer.send(json.dumps(allDataDict).encode("utf-8"))
        elif op == "RESET":
            dataStore.clear()
            consumer.send(b"OK")
        else:
            print("Invalid operation")
            consumer.send(b"Error")


def launch_consul_member(port):
    address = "127.0.{}.1".format(each_server + 1)
    port = int(port) + 100
    dns_port = int(port) + 100
    data_dir = "consul" + str(each_server + 1)
    cmd = 'consul agent -node=agent-{} -data-dir=/tmp/{} -bind={} -dns-port={} -http-port={} -retry-join "192.168.1.66" -disable-host-node-id'.format(
        port, data_dir, address, dns_port, port
    )
    ls_output = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )


if __name__ == "__main__":
    num_server = 1
    num_server = consul.get_cluster_size()
    print(f"num_server={num_server}")
    server_port = ""

    for each_server in range(num_server):
        server_port = "200{}".format(each_server + 1)
        print(f"Starting a server at:{server_port}...")
        Process(target=server, args=(server_port,)).start()

        # Starting consul members
        launch_consul_member(server_port)

    while True:
        # Check for membership changes
        members = consul.get_members()
        for port in members:
            if int(port) > int(server_port):  # New port was added
                print("\nMEMBERSHIP CHANGE DETECTED!\n")
                newPort = int(server_port) + 1
                Process(target=server, args=(str(newPort),)).start()
                server_port = str(newPort)

        time.sleep(5)
