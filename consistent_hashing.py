import hashlib
from bisect import bisect, bisect_left, bisect_right


# def add_replica(): + virtual nodes

# def remove_server(): + reshuffle


def hash_func(key: str, num_slots: int) -> int:
    hsh = hashlib.sha256()
    hsh.update(bytes(key.encode("utf-8")))
    return int(hsh.hexdigest(), 16) % num_slots


class ConsistentHashing:
    def __init__(self):
        self._keys = []
        self._servers = []
        self.num_slots = 20

    def add_servers(self, servers):
        if len(self._keys) == self.num_slots:
            raise Exception("Hash ring is full")
        # Adding all servers into hashing ring, this method can also be used to add new servers
        for server in servers:
            key = hash_func(server, self.num_slots)
            index = bisect(self._keys, key)
            print("Server = " + str(server) + " at key =" + str(key))
            self._servers.insert(index, server)
            self._keys.insert(index, key)

    def get_server(self, item: str) -> str:
        key = hash_func(item, self.num_slots)
        # Getting the server to the right of they key (Clockwise)
        index = bisect_right(self._keys, key) % len(self._keys)
        print(
            "Hashed Key of item = "
            + str(key)
            + ", So server "
            + self._servers[index]
            + " will store this key"
        )
        return self._servers[index]
