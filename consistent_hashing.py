import hashlib
from bisect import bisect, bisect_left, bisect_right
import client_producer

# Used https://levelup.gitconnected.com/consistent-hashing-27636286a8a9 for reference


def hash_func(key: str, num_slots: int) -> int:
    hsh = hashlib.sha256()
    hsh.update(bytes(key.encode("utf-8")))
    return int(hsh.hexdigest(), 16) % num_slots


class ConsistentHashing:
    def __init__(self):
        self._keys = []
        self._servers = []
        self.num_slots = 50

    def add_servers(self, servers):
        if len(self._keys) == self.num_slots:
            raise Exception("Hash ring is full")
        # Adding all servers into hashing ring, this method can also be used to add new servers
        for server in servers:
            key = hash_func(server, self.num_slots)
            index = bisect(self._keys, key)
            print("Server = " + str(server) + " at key =" + str(key))
            if index > 0 and self._keys[index - 1] == key:
                raise Exception(
                    "Collision occured, a server already exists at this key"
                )
            self._servers.insert(index, server)
            self._keys.insert(index, key)

    def get_server(self, item: str) -> str:
        key = hash_func(item, self.num_slots)
        # Getting the server to the right of they key (Clockwise)
        index = bisect_right(self._keys, key) % len(self._keys)
        print(
            "Hashed index of item is "
            + str(key)
            + " out of "
            + str(self.num_slots)
            + " total slots"
        )
        return self._servers[index]

    def delete_server(self, server):
        if len(self._keys) == 0:
            raise Exception("Hash ring is empty")

        key = hash_func(server, self.num_slots)
        index = bisect_left(self._keys, key)

        if index >= len(self._keys) or self._keys[index] != key:
            raise Exception("Server does not exist")

        self._keys.pop(index)
        self._servers.pop(index)

    def get_server_list(self):
        return self._servers
