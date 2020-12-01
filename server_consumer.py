import zmq
import sys
from multiprocessing import Process
import json

# import time


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


### Testing code for large amounts of data
# t_end = time.time() + 10
# while time.time() < t_end:
#     raw = consumer.recv_json()
#     key, value = raw["key"], raw["value"]
#     print(f"Server_port={port}:key={key},value={value}")
#     # FIXME: Implement to store the key-value data.
#     dataStore[key] = value

# print("\n" + port + "\n" + json.dumps(dataStore))


if __name__ == "__main__":
    num_server = 1
    if len(sys.argv) > 1:
        num_server = int(sys.argv[1])
        print(f"num_server={num_server}")

    for each_server in range(num_server):
        server_port = "200{}".format(each_server)
        print(f"Starting a server at:{server_port}...")
        Process(target=server, args=(server_port,)).start()
