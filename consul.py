import requests
from base64 import b64decode
import json
import ast


# def get_consumers():
#     resp = requests.get("http://127.0.0.1:8500/v1/kv/?keys")
#     keys = resp.json()
#     return keys


# def add_consumer(addr):
#     query = "http://127.0.0.1:8500/v1/kv/" + addr
#     return requests.put(query)


# def rem_consumer(addr):
#     query = "http://127.0.0.1:8500/v1/kv/" + addr
#     return requests.delete(query)


def get_cluster_size():
    raw = requests.get("http://127.0.0.1:8500/v1/kv/cluster_size")
    resp = raw.json()[0]
    value64 = resp["Value"]
    return int(b64decode(value64).decode("utf-8"))


def get_members():
    raw = requests.get("http://127.0.0.1:8500/v1/agent/members")
    resp = raw.json()
    res = []
    for item in resp:
        name = item["Name"]
        if name != "master":
            port = int(name.split("-")[1]) - 100
            res.append(str(port))
    return res


if __name__ == "__main__":
    get_members()
