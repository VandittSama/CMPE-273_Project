import hashlib
from socket import inet_aton
from struct import unpack


def get_weight(server, key):
    a = 1103515245
    b = 12345
    hsh = hashlib.sha256()
    hsh.update(bytes(str(key).encode("utf-8")))
    hash = int(hsh.hexdigest(), 16)
    # Below rendezvous formula is referenced from https://github.com/nikhilgarg28/rendezvous
    # But I have implemented my own technique to calculate 'hash'
    return (a * ((a * server + b) ^ hash) + b) % (2 ^ 31)


class HrwHashing:
    def __init__(self, servers=None):
        servers = servers or {}
        self._servers = set(servers)

    def add_servers(self, servers):
        # Simply storing all the servers in a class member
        for server in servers:
            self._servers.add(server)
            print("Server " + server + " added to HRW pool")

    def get_server(self, key):
        assert len(self._servers) > 0
        weights = []
        # Calculating the weight of key for each server and returning the one with max weight
        for server in self._servers:
            ipAndPort = server[6:].split(":")
            ip_packed = inet_aton(ipAndPort[0])
            serverNum = unpack("!L", ip_packed)[0]
            serverNum += int(ipAndPort[1])
            weight = get_weight(serverNum, key)
            weights.append((weight, server))

        _, server = max(weights)
        return server

    def get_server_list(self):
        return self._servers