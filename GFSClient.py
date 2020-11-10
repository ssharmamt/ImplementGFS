from functools import lru_cache

from GFSMaster import GFSMaster
from GFSChunkServer import GFSChunkServer


class GFSClient:

    def __init__(self):
        pass

    cache = dict()

    def read_file(self, file_name, byte_offset):
        chunk_location = ""
        if file_name in self.cache:
            chunk_location = self.get_chunk_from_cache(file_name)[0]
        else:
            chunk_id = int(byte_offset/64000000)
            master = GFSMaster()
            chunk_location = master.get_read_chunks(file_name, chunk_id)
            replicas = master.get_list_replicas(file_name, chunk_id)
            self.cache[file_name] = replicas
        return GFSChunkServer.read_from_chunk(chunk_location)

    @lru_cache(maxsize=3)
    def get_chunk_from_cache(self, file_name):
        return self.cache[file_name]

    last_chunk =2

    def write_data(self, file_name, data):
        chunk_location = ""
        if file_name in self.cache:
            chunk_location = self.get_chunk_from_cache(file_name)
        else:
            chunk_location = GFSMaster.get_list_replicas(file_name,self.last_chunk)
            self.cache[file_name] = chunk_location

        for chunk in chunk_location:
            GFSChunkServer.write_chunk(chunk, data)


class UserApplication:

    @staticmethod
    def main():
        client = GFSClient()
        data = client.read_file("file_1", 1024)
        print(data)
        client.write_data("file_1", "Hi Can I write to this file")
